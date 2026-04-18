from langdetect import detect, LangDetectException
from deep_translator import GoogleTranslator


MULTILINGUAL_STREET_PREFIXES = {
    'Calle': 'Street',
    'Carrer': 'Street',
    'Carretera': 'Road',
    'Avenida': 'Avenue',
    'Av.': 'Avenue',
    'Via': 'Via',
    'Rda.': 'Road',
    'C/': 'Street',
    'Polg.ind.': 'Industrial Park',
    'Ctra.': 'Road',
    'Reiherberg': 'Reiherberg',
    'Str.': 'Street',
    'Postfach': 'PO Box',
    'Postbus': 'PO Box',
    'Apartado': 'PO Box',
}

ENGLISH_ABBREVIATIONS = {
    'St':   'Street',
    'St.':  'Street',
    'Blvd': 'Boulevard',
    'Rd':   'Road',
    'Rd.':  'Road',
    'Ave':  'Avenue',
    'Ave.': 'Avenue',
    'Dr':   'Drive',
    'Dr.':  'Drive',
    'Ln':   'Lane',
    'Ln.':  'Lane',
    'Pkwy': 'Parkway',
    'Hwy':  'Highway',
    'Ct':   'Court',
    'Ct.':  'Court',
    'Pl':   'Place',
    'Pl.':  'Place',
}


def detect_language(text):
    """
    Detects the language of a text string.
    Returns language code e.g. 'en', 'es', 'de', 'ca'
    Returns 'unknown' if detection fails.
    """
    if not isinstance(text, str) or not text.strip():
        return 'unknown'
    try:
        return detect(text)
    except LangDetectException:
        return 'unknown'


def translate_to_english(text, source_lang='auto'):
    """
    Translates text to English using Google Translate.
    Returns original text if translation fails.
    """
    if not isinstance(text, str) or not text.strip():
        return text
    if source_lang == 'en':
        return text
    try:
        translated = GoogleTranslator(
            source=source_lang,
            target='en'
        ).translate(text)
        return translated if translated else text
    except Exception:
        return text


def expand_abbreviations(text):
    """
    Expands English street abbreviations and
    normalizes multilingual street prefixes.
    """
    if not isinstance(text, str):
        return text

    words = text.split()
    expanded = []
    for word in words:
        clean_word = word.strip('.,')
        if clean_word in ENGLISH_ABBREVIATIONS:
            expanded.append(ENGLISH_ABBREVIATIONS[clean_word])
        elif clean_word in MULTILINGUAL_STREET_PREFIXES:
            expanded.append(MULTILINGUAL_STREET_PREFIXES[clean_word])
        else:
            expanded.append(word)
    return ' '.join(expanded)


def process_language(text):
    """
    Full language processing pipeline for a single text value.
    Detects language, translates if non-English, expands abbreviations.
    Returns dict with original, language, translated, and final text.
    """
    if not isinstance(text, str) or not text.strip():
        return {
            "original": text,
            "detected_language": "unknown",
            "was_translated": False,
            "translated_text": text,
            "final_text": text
        }

    lang = detect_language(text)
    was_translated = False
    translated = text

    if lang not in ['en', 'unknown']:
        translated = translate_to_english(text, source_lang=lang)
        was_translated = True

    final = expand_abbreviations(translated)

    return {
        "original": text,
        "detected_language": lang,
        "was_translated": was_translated,
        "translated_text": translated,
        "final_text": final
    }


def process_language_dataframe(df):
    """
    Applies language processing to FULL_ADDRESS and SOURCE_NAME columns.
    Adds tracking columns for language and translation status.
    """
    df = df.copy()

    address_results = df['clean_address'].apply(
        lambda x: process_language(x) if isinstance(x, str) else {
            "detected_language": "unknown",
            "was_translated": False,
            "final_text": x
        }
    )

    df['detected_language'] = address_results.apply(lambda x: x['detected_language'])
    df['was_translated']    = address_results.apply(lambda x: x['was_translated'])
    df['clean_address']     = address_results.apply(lambda x: x['final_text'])

    return df


if __name__ == "__main__":
    test_cases = [
        "Calle José Luis Cuevas 304, Coatzacoalcos",
        "Reiherberg 112, Celle, DE",
        "Carrer Santa Carolina 95 Bajos, Barcelona",
        "1970 Texas Eastern Rd, Ragley, LA, US",
        "Postfach 1234, München",
    ]

    print("===== LANGUAGE DETECTOR TEST =====\n")
    for text in test_cases:
        result = process_language(text)
        print(f"  Input:    {result['original']}")
        print(f"  Language: {result['detected_language']}")
        print(f"  Translated: {result['was_translated']}")
        print(f"  Final:    {result['final_text']}\n")