from pyscbwrapper import SCB
from wikidataintegrator import wdi_core, wdi_login
from SPARQLWrapper import SPARQLWrapper, JSON
import configparser
from datetime import date
import time
import pandas as pd

config = configparser.ConfigParser()
config.read(r"C:\Users\adria\Downloads\work\config.ini", encoding="UTF-8")
user = config.get("Settings", "user")
pwd = config.get("Settings", "pwd")

login_instance = wdi_login.WDLogin(user, pwd)


# getting GDP data from db
gdp_areas = ["LV00A", "LV00C", "LV00B", "LV009", "LV005", "LV0002000", "LV0003000", "LV0031010", "LV0004000", "LV0005000", "LV0040010", "LV0006000", "LV0054010", "LV0007000"]
scb = SCB("lv", "VEK", "IK", "IKR", "IKR050")
scb.set_query(INDICATOR = ["B1GQ"], AREA = gdp_areas,
              NACE = ["TOTAL"], TIME = ["2021"])
scb.get_query()
gdp_scb_data = scb.get_data()


# getting GDP per capita data from db
gdp_capita_areas = ["LV0010000", "LV0050000", "LV0110000", "LV0130000", "LV0090000", "LV0170000", "LV0210000", "LV0250000", "LV0270000"]
scb = SCB("lv", "VEK", "IK", "IKR", "IKR010")
scb.set_query(AREA = gdp_capita_areas, ContentsCode = ["IKR010"], TIME = ["2021"])
scb.get_query()
gdp_capita_scb_data = scb.get_data()


# getting area's data from wikidata
query = """
    SELECT DISTINCT  ?object ?objectLabel ?atvk ?nuts
WHERE
{
        #Valstspilsētas
        { ?object wdt:P31 wd:Q109329953}.
        OPTIONAL {?object wdt:P1115 ?atvk} .
        OPTIONAL {?object wdt:P605 ?nuts} .
  FILTER NOT EXISTS {?object wdt:P576 ?diss}.
        
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],lv,en" }   
  }
"""
sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user)
sparql.setQuery(query)
sparql.setReturnFormat(JSON)
wiki_results = sparql.query().convert()


# correcting 'gdp_areas' order to be equal to 'gdp_scb_data["data"]' order
gdp_areas = []
for i in range(len(gdp_scb_data["data"])):
    gdp_areas.append(gdp_scb_data["data"][i]["key"][0])


# correcting 'gdp_capita_areas' order to be equal to 'gdp_capita_scb_data["data"]' order
gdp_capita_areas = []
for i in range(len(gdp_capita_scb_data["data"])):
    gdp_capita_areas.append(gdp_capita_scb_data["data"][i]["key"][0])


# find and asign names to area codes
gdp_capita_areas_names = []
for code in gdp_capita_areas:
    for area in pd.read_csv('https://data.gov.lv/dati/lv/dataset/8351e936-4a6d-4627-96f7-6478f39c67d9/resource/af4bab9d-7547-4b2a-aa19-28e73aa62d94/download/teritorijas.csv', usecols=["ValueCode","ValueTextL"]).values:
        if area[0] == code:
            gdp_capita_areas_names.append({area[0]: area[1]})


def write_data(today, point_in_time_date, area_q_code, gdp=None, gdp_capita=None):
    # adding qualifiers to the data
    point_in_time = wdi_core.WDTime(time = f"+{point_in_time_date}T00:00:00Z", prop_nr = "P585", is_qualifier=True, rank = "preferred")
    determination_method = wdi_core.WDItemID(value="Q791801", prop_nr = "P459", is_qualifier=True, rank = "preferred")

    # adding references to the data
    title = wdi_core.WDMonolingualText(value = "Iekšzemes kopprodukts un bruto pievienotā vērtība pa darbības veidiem reģionos un "
                                        "valstspilsētās faktiskajās cenās (pēc administratīvi teritoriālās reformas 2021. gadā)",
                                        prop_nr = "P1476", is_reference = True, rank = "preferred", language='lv')
    publisher = wdi_core.WDItemID(value = "Q39420022", prop_nr = "P123", is_reference = True, rank = "preferred")
    reference_url = wdi_core.WDUrl(value = "https://data.stat.gov.lv/pxweb/lv/OSP_PUB/START__VEK__IK__IKR/IKR010/",
                                        prop_nr = "P854", is_reference = True, rank = "preferred")
    retrieved = wdi_core.WDTime(time = f"+{today}T00:00:00Z", prop_nr = "P813", is_reference = True, rank = "preferred")

    if gdp is not None:
        # commiting the data
        data_gdp = wdi_core.WDQuantity(value=gdp, prop_nr = 'P2131', qualifiers=[point_in_time, determination_method], references = [[title, publisher, reference_url, retrieved]], unit="Q4916", rank = "preferred")
        wd_item = wdi_core.WDItemEngine(wd_item_id = area_q_code, new_item = False, data = [data_gdp])

        # uploading the data
        try:
            wd_item.write(login_instance)
            print(f"Successfully wrote the data to '{area_q_code}':\nGDP - {gdp} EUR\n")

        except wdi_core.WDApiError:
            print(f"Failed to write the 'GDP' data to '{area_q_code}'\n")
    
    elif gdp_capita is not None:
        # commiting the data
        data_gdp_capita = wdi_core.WDQuantity(value=gdp_capita, prop_nr = 'P2132', qualifiers=[point_in_time, determination_method], references = [[title, publisher, reference_url, retrieved]], unit="Q4916", rank = "preferred")
        wd_item = wdi_core.WDItemEngine(wd_item_id = area_q_code, new_item = False, data = [data_gdp_capita])

        # uploading the data
        try:
            wd_item.write(login_instance)
            print(f"Successfully wrote the data to '{area_q_code}':\nGDP per capita - {gdp_capita} EUR\n")

        except wdi_core.WDApiError:
            print(f"Failed to write the 'GDP per capita' data to '{area_q_code}'\n")

    # waiting to avoid being rate limited
    time.sleep(8)


point_in_time_date = "2021-01-01"

# finding the correct GDP to the corresponding area
for i in range(len(gdp_areas)):
    for wiki_result in wiki_results["results"]["bindings"]:
        area_q_code = wiki_result["object"]["value"][31:]

        if gdp_areas[i] == "LV" + wiki_result["atvk"]["value"]:
            gdp =  int(gdp_scb_data["data"][i]["values"][0]) * 1000
            today = date.today()

            write_data(today=today, point_in_time_date=point_in_time_date, area_q_code=area_q_code, gdp=gdp)


# finding the correct GDP per capita to the corresponding area
for i in range(len(gdp_capita_areas_names)):
    for wiki_result in wiki_results["results"]["bindings"]:
        area_q_code = wiki_result["object"]["value"][31:]

        if wiki_result["objectLabel"]["value"] in list(gdp_capita_areas_names[i].values())[0]:
            gdp_capita = int(gdp_capita_scb_data["data"][i]["values"][0])
            today = date.today()

            write_data(today=today, point_in_time_date=point_in_time_date, area_q_code=area_q_code, gdp_capita=gdp_capita)


input("The program has completed successfully.\nPress ENTER to quit")
