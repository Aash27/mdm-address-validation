import re


def parse_address(full_address):
    """
    Splits the |#|# delimited address into clean components.
    Returns a dict with individual fields and a clean string for API.
    """

    if not isinstance(full_address, str) or not full_address.strip():
        return {
            "street": None,
            "city": None,
            "state": None,
            "country": None,
            "zip": None,
            "clean_address": None,
            "is_complete": False,
            "flag_reason": "Empty or null address"
        }

    # Split using delimiter
    parts = [p.strip() for p in full_address.split("|#|#")]
    parts = [p if p else None for p in parts]

    # Ensure 5 parts
    while len(parts) < 5:
        parts.append(None)

    street, city, state, country, zip_code = parts[0], parts[1], parts[2], parts[3], parts[4]

   
    # COMPLETENESS CHECK (FIXED)

    missing = []

    # Street must exist AND contain a number
    if not street or not re.search(r'\d', street):
        missing.append("street")

    if not city:
        missing.append("city")

    if not country:
        missing.append("country")

    is_complete = len(missing) == 0

    # CLEAN ADDRESS BUILD
    clean_parts = [p for p in [street, city, state, country, zip_code] if p]
    clean_address = ", ".join(clean_parts) if clean_parts else None

    # FLAG REASON

    flag_reason = f"Missing or invalid: {', '.join(missing)}" if missing else None

    return {
        "street": street,
        "city": city,
        "state": state,
        "country": country,
        "zip": zip_code,
        "clean_address": clean_address,
        "is_complete": is_complete,
        "flag_reason": flag_reason
    }


def parse_address_dataframe(df):
    """
    Applies parse_address to every row in the dataframe.
    Adds new columns for each address component.
    """
    df = df.copy()

    parsed = df['FULL_ADDRESS'].apply(parse_address)

    df['street']        = parsed.apply(lambda x: x['street'])
    df['city']          = parsed.apply(lambda x: x['city'])
    df['state']         = parsed.apply(lambda x: x['state'])
    df['country']       = parsed.apply(lambda x: x['country'])
    df['zip']           = parsed.apply(lambda x: x['zip'])
    df['clean_address'] = parsed.apply(lambda x: x['clean_address'])
    df['is_complete']   = parsed.apply(lambda x: x['is_complete'])
    df['flag_reason']   = parsed.apply(lambda x: x['flag_reason'])

    return df


if __name__ == "__main__":
    test_cases = [
        (
            "5460 Whispering Oaks Ln|#|#Fort Worth|#|#TX|#|#US|#|#76108",
            True,
            "5460 Whispering Oaks Ln, Fort Worth, TX, US, 76108"
        ),
        (
            "Waushara|#|#Berlin|#|#WI|#|#US|#|#54923",
            False,
            "Waushara, Berlin, WI, US, 54923"
        ),
        (
            "Reiherberg 112|#|#Celle|#|#|#|#DE|#|#29229",
            True,
            "Reiherberg 112, Celle, DE, 29229"
        ),
        (
            "",
            False,
            None
        ),
    ]

    print("===== ADDRESS PARSER TEST =====\n")
    all_passed = True

    for address, expected_complete, expected_clean in test_cases:
        result = parse_address(address)

        complete_ok = result['is_complete'] == expected_complete
        clean_ok = result['clean_address'] == expected_clean

        passed = complete_ok and clean_ok
        if not passed:
            all_passed = False

        status = "PASS" if passed else "FAIL"

        print(f"  [{status}] Input:    {address[:60] if address else 'EMPTY'}")
        print(f"         Complete: {result['is_complete']} (expected {expected_complete})")
        print(f"         Clean:    {result['clean_address']}")
        print(f"         Flag:     {result['flag_reason']}\n")

    print("===== RESULT =====")
    print("All tests passed!" if all_passed else "Some tests FAILED - check above.")