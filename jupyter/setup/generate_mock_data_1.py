import os
import random
from datetime import timedelta, datetime
import pandas as pd
import numpy as np
from faker import Faker
from sklearn.datasets import make_blobs

fake = Faker()

def generate_customer_data(batch_size: int, n_clusters: int = 5) -> pd.DataFrame:
    # Define clusters manually to ensure clear separation
    cluster_centers = [
        (30000, 10000),
        (60000, 25000),
        (90000, 40000),
        (120000, 55000),
        (150000, 70000)
    ]
    clusters_std = [5000, 5000, 5000, 5000, 5000]

    # Generate synthetic data using make_blobs
    X, y = make_blobs(n_samples=batch_size, centers=cluster_centers, cluster_std=clusters_std, random_state=42)

    data = []
    for i, (income, loan_amount) in enumerate(X):
        customer_data = {
            "customer_id": fake.random_number(digits=8, fix_len=True),
            "name": fake.name(),
            "gender": random.choice(["Male", "Female"]),
            "sector": random.choice(["Retail", "Healthcare", "Education", "Technology", "Finance", "Government"]),
            "date_of_birth": datetime.now() - timedelta(days=int(np.clip(np.random.lognormal(mean=np.log(30), sigma=0.25, size=1), 18, 140)[0]) * 365),
            "address": fake.street_address(),
            "city": fake.city(),
            "country": fake.country(),
            "phone_number": fake.phone_number(),
            "email": fake.email(),
            "income": round(income, 2),
            "employment_status": random.choice(["Employed", "Unemployed", "Self-employed"]),
            "years_of_employment": np.random.exponential(5, 1)[0],
            "cb_person_default_on_file": random.choice(["Y", "N"]),
            "cb_preson_cred_hist_length": max(0, min(30, np.random.lognormal(0.5, 1))),
            "education_level": random.choice(["High School", "Associate's Degree", "Bachelor's Degree", "Master's Degree", "Doctorate"]),
            "cluster": y[i]
        }
        data.append(customer_data)
    df = pd.DataFrame(data)
    df["cb_preson_cred_hist_length"] = df["cb_preson_cred_hist_length"].round().astype(pd.Int64Dtype())
    df["years_of_employment"] = df["years_of_employment"].round().astype(pd.Int64Dtype())
    return df

def generate_loans_data(batch_size: int, customer_data: pd.DataFrame) -> pd.DataFrame:
    loan_intents = ["Personal", "Mortgage", "Education", "Business", "Medical", "Venture", "Home improvement", "Debt consolidation"]
    loan_intent_weights = [10, 20, 15, 15, 5, 5, 10, 20]
    repayment_methods = ["Monthly", "Bi-weekly"]

    data = []
    for _ in range(batch_size):
        customer = customer_data.sample(1).iloc[0]
        customer_income = customer["income"]
        interest_rate = round(random.uniform(2, 12), 2)
        start_date = fake.date_between(start_date="-2y", end_date="today")
        end_date = start_date + timedelta(days=random.randint(180, 1095))
        loan_intent = random.choices(loan_intents, weights=loan_intent_weights)[0]
        credit_score = max(300, min(850, int(random.gauss(0.01 * customer_income, 50))))
        collateral_value = max(0, min(50000, int(random.triangular(0, 0.2 * customer_income, 0.5 * customer_income))))
        loan_amount = round(random.triangular(0.2 * customer_income, 0.5 * customer_income, 0.8 * customer_income))
        loan_term = random.randint(12, 60)
        repayment_method = random.choice(repayment_methods)

        loans_data = {
            "loan_id": fake.uuid4(),
            "customer_id": customer["customer_id"],
            "loan_amount": loan_amount,
            "interest_rate": interest_rate,
            "start_date": start_date,
            "end_date": end_date,
            "status": random.choice([1, 0]),
            "loan_intent": loan_intent,
            "credit_score": credit_score,
            "loan_term": loan_term,
            "loan_grade": get_loan_grade(credit_score),
            "repayment_method": repayment_method,
            "collateral_value": collateral_value,
            "loan_purpose": get_loan_purpose(loan_intent),
            "cluster": customer["cluster"]
        }
        data.append(loans_data)
    df = pd.DataFrame(data)
    df["loan_amount"] = df["loan_amount"].astype(pd.Int64Dtype())
    df["credit_score"] = df["credit_score"].astype(pd.Int64Dtype())
    df["loan_term"] = df["loan_term"].astype(pd.Int64Dtype())
    df["collateral_value"] = df["collateral_value"].astype(pd.Int64Dtype())
    return df

def get_loan_purpose(loan_intent: str) -> str:
    if loan_intent == "Personal":
        return random.choice(["Home Renovation", "Vacation", "Wedding", "Debt Consolidation"])
    elif loan_intent == "Mortgage":
        return "Home Purchase"
    elif loan_intent == "Business":
        return random.choice(["Startup Capital", "Expansion", "Equipment Purchase"])
    elif loan_intent == "Education":
        return "Tuition Fees"
    else:
        return "Other"

def get_loan_grade(credit_score: int) -> str:
    if credit_score >= 750:
        return "A"
    elif 700 <= credit_score < 750:
        return random.choice(["A", "B"])
    elif 650 <= credit_score < 700:
        return random.choice(["B", "C"])
    elif 600 <= credit_score < 650:
        return random.choice(["C", "D"])
    else:
        return random.choice(["D", "E", "F", "G"])

def generate_mock_data():
    random.seed(10)
    customer_df = generate_customer_data(batch_size=random.randint(1000, 2000), n_clusters=5)
    loans_df = generate_loans_data(batch_size=random.randint(3000, 5000), customer_data=customer_df)
    home = os.environ["HOME"]
    customer_df.drop("cluster", axis=1, inplace=True)
    loans_df.drop("cluster", axis=1, inplace=True)
    customer_df.to_parquet(f"{home}/work/data/customers.parquet")
    loans_df.to_parquet(f"{home}/work/data/loans.parquet")

if __name__ == "__main__":
    generate_mock_data()
