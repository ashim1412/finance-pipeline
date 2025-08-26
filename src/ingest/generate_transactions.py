import os
import uuid
import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import yaml

with open("config/config.yml", "r") as f:
    config = yaml.safe_load(f)

RAW_DIR = config["data"]["raw_dir"]
N_CUSTOMERS = config["data"]["n_customers"]
START_DATE = datetime.strptime(config["data"]["start_date"], "%Y-%m-%d")
END_DATE = datetime.strptime(config["data"]["end_date"], "%Y-%m-%d")
SEED = config["seed"]

np.random.seed(SEED)

accounts = [f"0012{str(i).zfill(7)}" for i in range(1, N_CUSTOMERS+1)]
customer_ids = range(1, N_CUSTOMERS+1)
salary_days = np.random.randint(1,29, N_CUSTOMERS)
salary_amounts = np.random.randint(15000, 200000, N_CUSTOMERS)
has_rent = np.random.rand(N_CUSTOMERS)<0.55
rent_amounts = np.where(has_rent, np.random.randint(4000,35000, N_CUSTOMERS), 0)
has_emi = np.random.rand(N_CUSTOMERS)<0.35
emi_amounts = np.where(has_emi, np.random.randint(3000, 20000, N_CUSTOMERS), 0)
avg_util = np.random.randint(800,5000, N_CUSTOMERS)

customers = pd.DataFrame({
    "customer_id": customer_ids,
    "account_number": accounts,
    "salary_day": salary_days,
    "salary_amount": salary_amounts,
    "has_rent": has_rent.astype(int),
    "rent_amount": rent_amounts,
    "has_emi": has_emi.astype(int),
    "emi_amount": emi_amounts,
    "avg_monthly_utilities": avg_util
})

customers.to_csv("data/customers.csv", index= False)

CATEGORIES = ["SALARY","RENT","UTILITIES","GROCERY","DINING","FUEL","TRANSFER","EMI","QR_PAYMENT","ONLINE_SHOP","CASH_WITHDRAWAL","FEES"]
CHANNELS = ["ATM", "POS", "QR", "Online", "Branch"]
MERCHANTS = ["BhatBhateni","Daraz","Ncell","Ntc","QFX","ShellFuel","HimalGrocers","QuickMart","eSewaMerchant"]

def simulate_day(account,day, eng, meta_row):
    rows = []

    #salary
    if day.day == meta_row.salary_day:
        rows.append({
            "tran_id": str(uuid.uuid4()),
            "tran_date": day.strftime("%Y-%m-%d"),
            "account_number": account,
            "amount": float(meta_row.salary_amount),
            "dc_indicator": "C",
            "category": "SALARY",
            "remakr": f"Salary credit {day.strftime('%b %Y')}",
            "channel": "Online",
            "is_salary": 1
        })

    # Rent
    if meta_row.has_rent:
        rent_day = day + timedelta(days = random.randint(2,5))
        if rent_day.month == day.month:
               rows.append({
                "tran_id": str(uuid.uuid4()),
                "tran_date": rent_day.strftime("%Y-%m-%d"),
                "account_number": account,
                "amount": float(meta_row.rent_amount),
                "dc_indicator": "D",
                "category": "RENT",
                "remark": "Monthly rent",
                "channel": "Online",
                "is_salary": 0
            })
    
    # EMI

    if meta_row.has_emi:
         emi_day = day+timedelta(days = random.randint(5,12))
         if emi_day.month == day.month:
               rows.append({
                "tran_id": str(uuid.uuid4()),
                "tran_date": emi_day.strftime("%Y-%m-%d"),
                "account_number": account,
                "amount": float(meta_row.emi_amount),
                "dc_indicator": "D",
                "category": "EMI",
                "remark": "Loan EMI",
                "channel": "Online",
                "is_salary": 0
            })
               
    #Utilities

    if random.random()<0.9 and 10<= day.day <=25:
         util_amt = max(200, rng.normal(meta_row.avg_monthly_utilities, 300))
         rows.append({
            "tran_id": str(uuid.uuid4()),
            "tran_date": day.strftime("%Y-%m-%d"),
            "account_number": account,
            "amount": round(util_amt,2),
            "dc_indicator": "D",
            "category": "UTILITIES",
            "remark": random.choice(["NEA Bill","KUKL Water","ISP Payment"]),
            "channel": "Online",
            "is_salary": 0
        })
    
    # Random spending
    for _ in range(random.randint(0,2)):
        cat = rng.choice(["GROCERY","DINING","FUEL","QR_PAYMENT","ONLINE_SHOP","CASH_WITHDRAWAL","TRANSFER"])
        amt = round(max(100, rng.normal(1500,800)),2)
        rows.append({
            "tran_id": str(uuid.uuid4()),
            "tran_date": day.strftime("%Y-%m-%d"),
            "account_number": account,
            "amount": amt,
            "dc_indicator": "D" if cat != "TRANSFER" else rng.choice(["C","D"]),
            "category": cat,
            "remark": rng.choice(MERCHANTS),
            "channel": rng.choice(CHANNELS),
            "is_salary": 0
        })

    return rows

def compute_balance(df, initial_balance=0):
    df = df.sort_values(["tran_date","dc_indicator"], ascending=[True, False])
    bal = initial_balance
    balances = []
    for _, row in df.iterrows():
        amt = row["amount"]
        if row["dc_indicator"]=="C":
            bal += amt
        else:
            bal -= amt
        balances.append(round(bal,2))
    df["balance"] = balances
    return df

os.makedirs(RAW_DIR, exist_ok=True)
rng = np.random.default_rng(SEED)

for day in pd.date_range(START_DATE, END_DATE):
    daily_rows = []
    for idx, row in customers.iterrows():
        daily_rows.extend(simulate_day(row.account_number, day, rng, row))
    
    if daily_rows:
        df_day = pd.DataFrame(daily_rows)
        # compute balances per account
        dfs = []
        for acct in df_day.account_number.unique():
            acct_df = df_day[df_day.account_number==acct]
            acct_df = compute_balance(acct_df)
            dfs.append(acct_df)
        df_day = pd.concat(dfs)

        # save CSV in partitioned path
        out_path = os.path.join(RAW_DIR, day.strftime("%Y/%m/%d"))
        os.makedirs(out_path, exist_ok=True)
        df_day.to_csv(os.path.join(out_path,"transactions.csv"), index=False)

print("✅ Raw data generated in partitioned folders")