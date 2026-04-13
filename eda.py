# Import required libraries
import pandas as pd

# Load dataset
df = pd.read_csv("100_sample_MDM.csv")

# ===============================
# DATASET PREVIEW
# ===============================
print("\n========== DATASET PREVIEW ==========")
print(df.head())


# ===============================
# COLUMN NAMES
# ===============================
print("\n========== COLUMNS ==========")
print(df.columns)


# ===============================
# DATA STRUCTURE INFO
# ===============================
print("\n========== DATA INFO ==========")
df.info()


# ===============================
# MISSING VALUES
# ===============================
print("\n========== MISSING VALUES ==========")
print(df.isnull().sum())


# ===============================
# SAMPLE SOURCE_NAME VALUES
# ===============================
print("\n========== SAMPLE SOURCE_NAME VALUES ==========")
print(df["SOURCE_NAME"].head(20))


# ===============================
# COUNT NAMES WITH NUMBERS
# ===============================
names_with_numbers = df["SOURCE_NAME"].str.contains(r'\d', regex=True).sum()
print("\n========== NAMES WITH NUMBERS ==========")
print(names_with_numbers)


# ===============================
# SAMPLE FULL_ADDRESS VALUES
# ===============================
print("\n========== SAMPLE FULL_ADDRESS VALUES ==========")
print(df["FULL_ADDRESS"].head(20))


# ===============================
# ENCODING ISSUES IN NAMES
# ===============================
weird_names = df[df["SOURCE_NAME"].str.contains("Ã|â", na=False)]

print("\n========== ENCODING ISSUES IN NAMES ==========")
print(weird_names["SOURCE_NAME"])