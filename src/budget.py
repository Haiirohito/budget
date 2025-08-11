import pandas as pd
import numpy as np

# ===== Load & Clean Data =====
data = "dataset/upi_transactions_2024.csv"
df = pd.read_csv(data)

# Remove extra spaces in column names
df.columns = df.columns.str.strip()

# Parse timestamp
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

# Convert amount
df["amount (INR)"] = pd.to_numeric(df["amount (INR)"], errors="coerce")

# Drop bad/missing data
df = df.dropna(subset=["timestamp", "amount (INR)"])
df = df.drop_duplicates()

# Create month-year period
df["month_year"] = df["timestamp"].dt.to_period("M")

# If you want per-user normalization:
# First, see if you have a sender ID or phone number column
user_identifier_col = None
for col in df.columns:
    if "sender" in col.lower() and "id" in col.lower():
        user_identifier_col = col
        break

if user_identifier_col:
    user_monthly_spend = (
        df.groupby([user_identifier_col, "merchant_category", "month_year"])["amount (INR)"]
          .sum()
          .reset_index()
    )
    monthly_cat_spend = (
        user_monthly_spend.groupby(["merchant_category", "month_year"])["amount (INR)"]
          .mean()  # average per user
          .reset_index()
    )
else:
    print("⚠ No sender_id found — using total spend instead of per-user averages.")
    monthly_cat_spend = (
        df.groupby(["merchant_category", "month_year"])["amount (INR)"]
          .sum()
          .reset_index()
    )

# ===== Hybrid Budget Function =====
def hybrid_budget(category_df):
    category_df = category_df.sort_values("month_year")

    # Weighted Average for last 3 months
    if len(category_df) >= 3:
        last3 = category_df["amount (INR)"].iloc[-3:].values
        weights = np.array([0.5, 0.3, 0.2])
        wa = np.dot(last3[::-1], weights)  # last month = biggest weight
    else:
        wa = category_df["amount (INR)"].mean()

    # Median of available months
    median_spend = category_df["amount (INR)"].median()

    # Cap WA at 125% of median
    budget = min(wa, median_spend * 1.25)

    # Seasonality adjustment
    current_month = category_df["month_year"].iloc[-1].month
    month_avg = category_df[category_df["month_year"].dt.month == current_month]["amount (INR)"].mean()
    overall_avg = category_df["amount (INR)"].mean()
    if overall_avg > 0:
        budget *= month_avg / overall_avg

    return round(budget, 2)

# ===== Apply to Each Category =====
budgets = (
    monthly_cat_spend.groupby("merchant_category", group_keys=False)
    .apply(hybrid_budget)
    .reset_index(name="next_month_budget")
)

print(budgets)
