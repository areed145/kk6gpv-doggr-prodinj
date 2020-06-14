import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import date, datetime
import json
from bson import json_util
import random
import os


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        else:
            return super(NpEncoder, self).default(obj)


client = MongoClient(os.environ["MONGODB_CLIENT"])
db = client.petroleum


def prodinj(api):
    df_header = pd.DataFrame(list(db.doggr.find({"api": api})))
    try:
        prod = (
            pd.DataFrame(df_header["prod"].values[0])
            .sort_values(by="date")
            .reset_index(drop=True)
        )
        prod_ = True
    except Exception:
        prod_ = False

    try:
        inj = (
            pd.DataFrame(df_header["inj"].values[0])
            .sort_values(by="date")
            .reset_index(drop=True)
        )
        inj_ = True
    except Exception:
        inj_ = False
    try:
        inj.loc[inj["welltype_i"].isin(["WF", "WD"]), "water_i"] = inj[
            "wtrstm"
        ]
        inj.loc[inj["welltype_i"] == "SF", "steam"] = inj["wtrstm"]
        inj.loc[inj["welltype_i"] == "SC", "cyclic"] = inj["wtrstm"]
        inj.loc[inj["welltype_i"].isin(["GD", "GS", "PM"]), "gas_i"] = inj[
            "wtrstm"
        ]
        inj.loc[inj["welltype_i"] == "AI", "air"] = inj["wtrstm"]
    except Exception:
        pass
    prodinj = pd.DataFrame(
        columns=[
            "date",
            "oil",
            "water",
            "gas",
            "daysprod",
            "oilgrav",
            "pcsg",
            "ptbg",
            "btu",
            "method",
            "waterdisp",
            "pwtstatus_p",
            "welltype_p",
            "status_p",
            "poolcode_p",
            "wtrstm",
            "water_i",
            "steam",
            "cyclic",
            "gasair",
            "gas_i",
            "air",
            "daysinj",
            "pinjsurf",
            "wtrsrc",
            "wtrknd",
            "pwtstatus_i",
            "welltype_i",
            "status_i",
            "poolcode_i",
        ]
    )
    if prod_:
        if inj_:
            df = prod.merge(inj, on="date", how="outer")
        else:
            df = prod
    else:
        df = inj
    prodinj = prodinj.append(df)
    for col in [
        "oil",
        "water",
        "gas",
        "daysprod",
        "wtrstm",
        "water_i",
        "steam",
        "cyclic",
        "gasair",
        "gas_i",
        "air",
        "daysinj",
    ]:
        prodinj[col] = prodinj[col].fillna(0)
    prodinj["cyclic_ct"] = 0
    prodinj.loc[prodinj["cyclic"] > 0, "cyclic_ct"] = 1
    prodinj["oil_last"] = prodinj["oil"].shift(1)
    prodinj["cyclic_last"] = prodinj["cyclic"].shift(1)
    prodinj.loc[
        (prodinj["cyclic_last"] > 0) & (prodinj["oil_last"] == 0), "cyclic_ct"
    ] = 0
    prodinj["cyclic_ct"] = prodinj["cyclic_ct"].cumsum()
    for col in ["_id", "prod", "inj"]:
        try:
            df_header.drop(columns=[col], inplace=True)
        except Exception:
            pass
    dict_header = df_header.iloc[0].to_dict()
    dict_header["prodinj"] = prodinj.drop(columns=["cyclic_last"]).T.to_dict()
    data = json_util.loads(json.dumps(dict_header["prodinj"], cls=NpEncoder))
    db.doggr.update_one(
        {"api": api}, {"$set": {"prodinj": data}}, upsert=False
    )

    try:
        db.doggr.update_one(
            {"api": api}, {"$unset": {"cyclic_jobs": 1}}, upsert=False
        )
    except Exception:
        pass

    try:
        db.doggr.update_one(
            {"api": api}, {"$unset": {"prod": 1}}, upsert=False
        )
    except Exception:
        pass

    try:
        db.doggr.update_one({"api": api}, {"$unset": {"inj": 1}}, upsert=False)
    except Exception:
        pass

    try:
        cyclic_jobs = []
        for job in prodinj["cyclic_ct"].unique():
            if job > 0:
                df = prodinj[prodinj["cyclic_ct"] == job]
                cyclic_job = {}
                cyclic_job["number"] = job
                cyclic_job["start"] = df["date"].min()
                cyclic_job["end"] = df[df["cyclic"] > 0]["date"].max()
                cyclic_job["total"] = df["cyclic"].sum()
                cyclic_job["oil_pre"] = df["oil_last"].iloc[0]
                cyclic_job["oil_post"] = df[df["oil"] > 0]["oil"].iloc[0]
                cyclic_job["oil"] = df["oil"].to_list()[:6]
                cyclic_jobs.append(cyclic_job)
        cyclic_jobs = pd.DataFrame(cyclic_jobs).T.to_dict()
        dict_header["cyclic_jobs"] = pd.DataFrame(cyclic_jobs).T.to_dict()
        data = json_util.loads(
            json.dumps(dict_header["cyclic_jobs"], cls=NpEncoder)
        )
        db.doggr.update_one(
            {"api": api}, {"$set": {"cyclic_jobs": data}}, upsert=False
        )
    except Exception:
        pass


if __name__ == "__main__":
    df_prod = pd.DataFrame(
        list(db.doggr.find({"prod": {"$exists": True}}, {"api": 1}))
    )
    df_inj = pd.DataFrame(
        list(db.doggr.find({"inj": {"$exists": True}}, {"api": 1}))
    )
    apis = list(set(list(df_prod["api"]) + list(df_inj["api"])))
    random.shuffle(apis)
    for api in apis:
        try:
            prodinj(api)
            print(api, " succeeded")
        except Exception:
            print(api, " failed")
