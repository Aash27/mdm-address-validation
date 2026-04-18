import ftfy


def fix_encoding(text):
    if not isinstance(text, str):
        return text

    # Step 1: ftfy first
    text = ftfy.fix_text(text)

    # Step 2: remove leftover junk like Â
    text = text.replace("Â", "")

    # Step 3: second pass ftfy
    text = ftfy.fix_text(text)

    return text.strip()

def fix_encoding_dataframe(df, columns=['SOURCE_NAME', 'FULL_ADDRESS']):
    """
    Applies fix_encoding to specified columns in the dataframe.
    Adds encoding_fixed column to track which records changed.
    """
    df = df.copy()
    df['encoding_fixed'] = False

    for col in columns:
        if col in df.columns:
            original = df[col].copy()
            df[col] = df[col].apply(fix_encoding)
            changed = df[col] != original
            df.loc[changed, 'encoding_fixed'] = True

    return df


if __name__ == "__main__":
    test_cases = [
        ("BajÃ£Âo",           "Bajão"),
        ("JosÃ©",              "José"),
        ("MÃ¡laga",            "Málaga"),
        ("Ã¼becker",           "übecker"),
        ("Stone Security Llc", "Stone Security Llc"),
    ]

    print("===== ENCODING FIXER TEST =====\n")
    all_passed = True

    for corrupted, expected in test_cases:
        result = fix_encoding(corrupted)
        status = "PASS" if result == expected else "FAIL"
        if status == "FAIL":
            all_passed = False
        print(f"  [{status}] Input:    {corrupted}")
        print(f"         Expected: {expected}")
        print(f"         Got:      {result}\n")

    print("===== RESULT =====")
    print("All tests passed!" if all_passed else "Some tests FAILED - check above.")