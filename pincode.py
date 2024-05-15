import re
import json

f = open('postalCodes.json')
postal_codes_json = json.load(f)
f.close()

def get_address_by_postal_code(org_id, postal_code):
    try:
        data = postal_codes_json.get(postal_code)
        while not data and postal_code:
            postal_code = "^" + postal_code[:-1]
            match = key_match(postal_codes_json, postal_code)
            if match:
                for key in match:
                    data = match[key]
                    break
        return data
    except Exception as error:
        raise Exception(f"get_address_by_postal_code({org_id}, {postal_code}) - {error}")


def key_match(o, r):
    return {k: o[k] for k in o.keys() if re.match(r, k)}