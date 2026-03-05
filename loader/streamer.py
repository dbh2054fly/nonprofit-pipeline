from lxml import etree

def clean_text(x):
    """Normalize text from XML: None/whitespace -> None, otherwise stripped string."""
    if x is None:
        return None
    s = x.strip()
    return s if s else None

def parse_int(x):
    """
    Safe int parser:
    - None/blank/whitespace -> None
    - allows commas: '1,234' -> 1234
    - returns None if not parseable
    """
    s = clean_text(x)
    if s is None:
        return None
    s = s.replace(",", "")
    try:
        return int(s)
    except ValueError:
        return None

def normalize_ein(x):
    """
    Normalize EIN to 9-digit string.
    Returns None if value is missing or invalid.
    """
    if x is None:
        return None

    s = x.strip()

    if not s:
        return None

    # keep digits only
    digits = "".join(ch for ch in s if ch.isdigit())

    if not digits:
        return None

    # enforce 9 digits
    return digits.zfill(9)

NS = "{http://www.irs.gov/efile}"
def process_xml(file_obj):
    #tag variables
    grant_indicator_tag = f"{NS}GrantsToOrganizationsInd"
    filer_tag = f"{NS}Filer"
    schedule_i_tag = f"{NS}IRS990ScheduleI"
    url_tag = f"{NS}WebsiteAddressTxt"
    mission_tag = f"{NS}MissionDesc"
    assets_tag = f"{NS}TotalAssetsEOYAmt"
    revenue_tag = f"{NS}CYTotalRevenueAmt"
    expenses_tag = f"{NS}CYTotalExpensesAmt"
    grants_paid_tag = f"{NS}CYGrantsAndSimilarPaidAmt"
    return_type_tag = f"{NS}ReturnTypeCd"

    tags = (
        grant_indicator_tag,
        filer_tag,
        schedule_i_tag,
        url_tag,
        mission_tag,
        assets_tag,
        revenue_tag,
        expenses_tag,
        grants_paid_tag,
        return_type_tag
    )

    #initializing storage 
    ein = business_name = address = city = state = zipcode = None
    does_grants = None
    url = None
    mission = None
    assets = revenue = expenses = grants_paid = None
    return_type = None

    grant_list = []

    #parse
    for event, elem in etree.iterparse(file_obj, events=("end",), tag=tags):
        if elem.tag == filer_tag:
            ein = normalize_ein(elem.findtext(f".//{NS}EIN"))
            business_name = elem.findtext(f".//{NS}BusinessNameLine1Txt")
            address = elem.findtext(f".//{NS}AddressLine1Txt")
            city = elem.findtext(f".//{NS}CityNm")
            state = elem.findtext(f".//{NS}StateAbbreviationCd")
            zipcode = elem.findtext(f".//{NS}ZIPCd")
            elem.clear()
        elif elem.tag == grant_indicator_tag:
            does_grants = (clean_text(elem.text) == "1")
            elem.clear()
        elif elem.tag == url_tag:
            url = clean_text(elem.text)
            elem.clear()
        elif elem.tag == mission_tag:
            mission = clean_text(elem.text)
            elem.clear()
        elif elem.tag == assets_tag:
            assets = parse_int(elem.text)
            elem.clear()
        elif elem.tag == revenue_tag:
            revenue = parse_int(elem.text)
            elem.clear()
        elif elem.tag == expenses_tag:
            expenses = parse_int(elem.text)
            elem.clear()
        elif elem.tag == grants_paid_tag:
            grants_paid = parse_int(elem.text)
            elem.clear()
        elif elem.tag == return_type_tag:
            return_type = clean_text(elem.text)
            elem.clear()
        elif elem.tag == schedule_i_tag:
            for grant in elem.findall(f".//{NS}RecipientTable"):
                recipient = clean_text(grant.findtext(f".//{NS}BusinessNameLine1Txt"))
                amount = parse_int(grant.findtext(f".//{NS}CashGrantAmt"))
                purpose = clean_text(grant.findtext(f".//{NS}PurposeOfGrantTxt"))
                grant_row = (ein, recipient, amount, purpose)
                grant_list.append(grant_row)
            elem.clear()
        
    foundation_row = (
        ein,
        business_name,
        address,
        city,
        state,
        zipcode,
        url,
        mission,
        does_grants,
        assets,
        revenue,
        expenses,
        grants_paid,
        return_type
    )
    
    return foundation_row, grant_list
