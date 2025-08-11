import pandas as pd

# ===== Load & Clean Data =====
data = "dataset/upi_transactions_2024.csv"
df = pd.read_csv(data)

# Clean column names
df.columns = df.columns.str.strip()

# Parse timestamp
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

# Clean amounts
df["amount (INR)"] = pd.to_numeric(df["amount (INR)"], errors="coerce")
df = df.dropna(subset=["timestamp", "amount (INR)"])

# Extract year & month
df["year"] = df["timestamp"].dt.year
df["month"] = df["timestamp"].dt.month

# Filter for 2024 only
df_2024 = df[df["year"] == 2024]

# ===== Monthly summary (all categories combined) =====
monthly_summary = (
    df_2024.groupby("month").agg(
        total_spend=("amount (INR)", "sum"),
        avg_transaction_size=("amount (INR)", "mean"),
        transaction_count=("amount (INR)", "count")
    ).reset_index()
)

# ===== Monthly spend per category =====
monthly_per_category = (
    df_2024.groupby(["month", "merchant_category"])["amount (INR)"]
    .sum()
    .reset_index()
    .sort_values(["month", "merchant_category"])
)

# ===== Show results =====
print("===== Monthly Summary (All Categories) =====")
print(monthly_summary)

print("\n===== Monthly Spend per Category =====")
print(monthly_per_category)
