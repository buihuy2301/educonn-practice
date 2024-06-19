"""
This script generates mock data for the Customers and Loans tables 
and writes them to S3 in Parquet format.
"""

import os
import random
from datetime import timedelta, datetime
import pandas as pd
import numpy as np
from faker import Faker

fake = Faker()


def generate_customer_data(batch_size: int) -> pd.DataFrame:
    """
    Generate sample data for Customers table
    Args:
        batch_size (int): Number of rows to generate

    Returns:
        pd.DataFrame: DataFrame with sample data
    """
    sectors = [
        "Retail",
        "Healthcare",
        "Education",
        "Technology",
        "Finance",
        "Government",
    ]
    sector_weights = [0.25, 0.15, 0.15, 0.20, 0.15, 0.10]
    education_level = [
        "High School",
        "Associate's Degree",
        "Bachelor's Degree",
        "Master's Degree",
        "Doctorate",
    ]
    education_level_weights = [0.2, 0.2, 0.25, 0.15, 0.2]
    employment_status = ["Employed", "Unemployed", "Self-employed"]
    employment_status_weights = [0.7, 0.25, 0.05]
    cb_person_default_on_file = ["Y", "N"]
    cb_person_default_on_file_weights = [0.1, 0.9]
    genders = ["Male", "Female"]
    data = []
    for _ in range(batch_size):
        customer_data = {
            "customer_id": fake.random_number(digits=8, fix_len=True),
            "name": fake.name(),
            "gender": random.choice(genders),
            "sector": random.choices(sectors, weights=sector_weights)[0],
            "date_of_birth": datetime.now()
            - timedelta(
                days=int(
                    np.clip(
                        np.random.lognormal(mean=np.log(30), sigma=0.25, size=1), 18, 140
                    )[0]
                )
                * 365
            ),
            "address": fake.street_address(),
            "city": fake.city(),
            "country": fake.country(),
            "phone_number": fake.phone_number(),
            "email": fake.email(),
            "income": round(random.triangular(4000, 200000, 13000), 2),
            "employment_status": random.choices(
                employment_status,
                weights=employment_status_weights,
            )[0],
            "years_of_employment": np.random.exponential(5, 1)[0],
            "cb_person_default_on_file": random.choices(
                cb_person_default_on_file,
                weights=cb_person_default_on_file_weights,
            )[0],
            "cb_preson_cred_hist_length": max(0, min(30, np.random.lognormal(0.5, 1))),
            "education_level": random.choices(
                education_level, weights=education_level_weights
            )[0],
        }
        for key in customer_data:
            if (
                key != "customer_id" and random.random() < 0.05
            ):  # Adjust the probability as needed
                customer_data[key] = None
        data.append(customer_data)
    df = pd.DataFrame(data)
    df["cb_preson_cred_hist_length"] = (
        df["cb_preson_cred_hist_length"].round().astype(pd.Int64Dtype())
    )
    df["years_of_employment"] = (
        df["years_of_employment"].round().astype(pd.Int64Dtype())
    )
    return df


def generate_loans_data(batch_size: int, customer_data: pd.DataFrame) -> pd.DataFrame:
    """
    Generate sample data for Loans table
    Args:
        batch_size (int): number of rows to generate
        customer_data (pd.DataFrame): Customer data to use for generating loans

    Returns:
        pd.DataFrame: DataFrame with sample data
    """
    loan_intents = [
        "Personal",
        "Mortgage",
        "Education",
        "Business",
        "Medical",
        "Venture",
        "Home improvement",
        "Debt consolidation",
    ]
    loan_intent_weights = [10, 20, 15, 15, 5, 5, 10, 20]
    loan_status = [1, 0]
    repayment_methods = ["Monthly", "Bi-weekly"]
    data = []
    for _ in range(batch_size):
        loan_id = fake.uuid4()
        customer_id = random.choice(customer_data["customer_id"])
        customer_income = customer_data.loc[
            customer_data["customer_id"] == customer_id, "income"
        ].iloc[0]
        interest_rate = round(random.uniform(2, 12), 2)
        start_date = fake.date_between(start_date="-2y", end_date="today")
        end_date = start_date + timedelta(
            days=random.randint(180, 1095)
        )  # Loan term between 6 and 36 months
        status = random.choices(
            loan_status, weights=[random.uniform(0, 0.3), 1 - random.uniform(0, 0.3)]
        )[0]
        loan_intent = random.choices(loan_intents, weights=loan_intent_weights)[0]
        if customer_income > 0:
            credit_score = max(
                300,
                min(
                    800,
                    int(
                        random.triangular(
                            0.005 * customer_income,
                            0.012 * customer_income,
                            0.008 * customer_income,
                        )
                    ),
                ),
            )
            collateral_value = max(
                0,
                min(
                    50000,
                    int(
                        random.triangular(
                            0, 0.2 * customer_income, 0.5 * customer_income
                        )
                    ),
                ),
            )
            loan_amount = round(
                random.triangular(
                    0.2 * customer_income, 0.5 * customer_income, 0.8 * customer_income
                ),
            )
        else:
            credit_score = random.randint(600, 800)
            collateral_value = random.randint(0, 50000)
            loan_amount = random.randint(1000, 50000)
        loan_term = random.randint(12, 60)
        repayment_method = random.choice(repayment_methods)
        loan_purpose = get_loan_purpose(loan_intent)
        loan_grade = get_loan_grade(credit_score)
        loans_data = {
            "loan_id": loan_id,
            "customer_id": customer_id,
            "loan_amount": loan_amount,
            "interest_rate": interest_rate,
            "start_date": start_date,
            "end_date": end_date,
            "status": status,
            "loan_intent": loan_intent,
            "credit_score": credit_score,
            "loan_term": loan_term,
            "loan_grade": loan_grade,
            "repayment_method": repayment_method,
            "collateral_value": collateral_value,
            "loan_purpose": loan_purpose,
        }
        for key in loans_data:
            if (
                key != "loan_id" and key != "customer_id" and random.random() < 0.05
            ):  # Adjust the probability as needed
                loans_data[key] = None
        data.append(loans_data)
    df = pd.DataFrame(data)
    df["loan_amount"] = df["loan_amount"].astype(pd.Int64Dtype())
    df["credit_score"] = df["credit_score"].astype(pd.Int64Dtype())
    df["loan_term"] = df["loan_term"].astype(pd.Int64Dtype())
    df["collateral_value"] = df["collateral_value"].astype(pd.Int64Dtype())
    return df


def get_loan_purpose(loan_intent: str) -> str:
    """
    Generate loan purpose based on loan intent
    Args:
        loan_intent (str): intent of the loan

    Returns:
        str: loan purpose
    """
    if loan_intent == "Personal":
        return random.choice(
            ["Home Renovation", "Vacation", "Wedding", "Debt Consolidation"]
        )
    elif loan_intent == "Mortgage":
        return "Home Purchase"
    elif loan_intent == "Business":
        return random.choice(["Startup Capital", "Expansion", "Equipment Purchase"])
    elif loan_intent == "Education":
        return "Tuition Fees"
    else:
        return "Other"


def get_loan_grade(credit_score: int) -> str:
    """_summary_

    Args:
        credit_score (int): credit score of the customer

    Returns:
        str: loan grade of the customer
    """
    # Assign loan grade based on credit score
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
    """
    Generate mock data for customer and loans tables
    """
    random.seed(10)
    customer_df = generate_customer_data(batch_size=random.randint(1000, 2000))
    loans_df = generate_loans_data(
        batch_size=random.randint(3000, 5000), customer_data=customer_df
    )
    home = os.environ["HOME"]
    customer_df.to_parquet(f"{home}/work/data/customers.parquet")
    loans_df.to_parquet(f"{home}/work/data/loans.parquet")


if __name__ == "__main__":
    generate_mock_data()
