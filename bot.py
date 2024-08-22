from pyscbwrapper import SCB
from wikidataintegrator import wdi_core, wdi_login
from SPARQLWrapper import SPARQLWrapper, JSON
import configparser
from datetime import date
import time


config = configparser.ConfigParser()
config.read(r"C:\Users\adria\Downloads\work\config.ini", encoding="UTF-8")
user = config.get("Settings", "user")
pwd = config.get("Settings", "pwd")

login_instance = wdi_login.WDLogin(user, pwd)


# getting GDP data from db
gdp_areas_atvk = ["LV0001000", "LV0002000", "LV0003000", "LV0031010", "LV0004000", "LV0005000", "LV0040010", "LV0006000", "LV0054010", "LV0007000"]
scb = SCB("lv", "VEK", "IK", "IKR", "IKR050")
scb.set_query(INDICATOR = ["B1GQ"], AREA = gdp_areas_atvk, NACE = ["TOTAL"], TIME = ["2021"])
scb.get_query()
gdp_scb_data = scb.get_data()


# getting GDP per capita data from db
gdp_capita_areas_atvk = ["LV0001000", "LV0002000", "LV0003000", "LV0031010", "LV0004000", "LV0005000", "LV0040010", "LV0006000", "LV0054010", "LV0007000"]
scb = SCB("lv", "VEK", "IK", "IKR", "IKR060")
scb.set_query(INDICATOR = ["B1GQ"], AREA = gdp_capita_areas_atvk, ContentsCode = ["IKR061"], TIME = ["2021"])
scb.get_query()
gdp_capita_scb_data = scb.get_data()


# getting area's data from wikidata
query = """
SELECT DISTINCT ?object ?objectLabel ?atvk ?nuts
WHERE
{
        #Valstspilsētas
        { ?object wdt:P31 wd:Q109329953}.
        OPTIONAL {?object wdt:P1115 ?atvk}.
        OPTIONAL {?object wdt:P605 ?nuts}.
  FILTER NOT EXISTS {?object wdt:P576 ?diss}.

  SERVICE wikibase:label {bd:serviceParam wikibase:language "[AUTO_LANGUAGE],lv,en"}
  }
"""
sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user)
sparql.setQuery(query)
sparql.setReturnFormat(JSON)
wiki_results = sparql.query().convert()


# correcting 'gdp_areas_atvk' order to be equal to 'gdp_scb_data["data"]' order
gdp_areas_atvk = []
for i in range(len(gdp_scb_data["data"])):
    gdp_areas_atvk.append(gdp_scb_data["data"][i]["key"][0])


# correcting 'gdp_capita_areas_atvk' order to be equal to 'gdp_capita_scb_data["data"]' order
gdp_capita_areas_atvk = []
for i in range(len(gdp_capita_scb_data["data"])):
    gdp_capita_areas_atvk.append(gdp_capita_scb_data["data"][i]["key"][0])


for wiki_result in wiki_results["results"]["bindings"]:
    data = []
    today = date.today()
    area_q_code = wiki_result["object"]["value"][31:]
    area_name = wiki_result["objectLabel"]["value"]
    area_atvk = "LV" + wiki_result["atvk"]["value"]
    point_in_time_date = "2021-01-01"
    gdp = None
    gdp_capita = None

    # adding qualifiers to the data
    point_in_time = wdi_core.WDTime(time = f"+{point_in_time_date}T00:00:00Z", prop_nr = "P585", is_qualifier=True, rank = "preferred")
    determination_method = wdi_core.WDItemID(value="Q791801", prop_nr = "P459", is_qualifier=True, rank = "preferred")

    # adding references to the data
    title = wdi_core.WDMonolingualText(value = "Iekšzemes kopprodukts un bruto pievienotā vērtība pa darbības veidiem reģionos un "
                                        "valstspilsētās faktiskajās cenās (pēc administratīvi teritoriālās reformas 2021. gadā)",
                                        prop_nr = "P1476", is_reference = True, rank = "preferred", language='lv')
    publisher = wdi_core.WDItemID(value = "Q39420022", prop_nr = "P123", is_reference = True, rank = "preferred")
    retrieved = wdi_core.WDTime(time = f"+{today}T00:00:00Z", prop_nr = "P813", is_reference = True, rank = "preferred")

    # finding the correct GDP to the corresponding area
    for i in range(len(gdp_areas_atvk)):
        if gdp_areas_atvk[i] == area_atvk:
            gdp =  int(gdp_scb_data["data"][i]["values"][0]) * 1000

            # adding reference 'reference_url' to the data
            reference_url = wdi_core.WDUrl(value = "https://data.stat.gov.lv/pxweb/lv/OSP_PUB/START__VEK__IK__IKR/IKR050/",
                                        prop_nr = "P854", is_reference = True, rank = "preferred")

            # commiting the data
            data_gdp = wdi_core.WDQuantity(value=gdp, prop_nr = 'P2131', qualifiers=[point_in_time, determination_method],
                                        references = [[title, publisher, reference_url, retrieved]], unit="Q4916", rank = "preferred")
            data.append(data_gdp)

    # finding the correct GDP per capita to the corresponding area
    for i in range(len(gdp_capita_areas_atvk)):
        if gdp_capita_areas_atvk[i] == area_atvk:
            gdp_capita = int(gdp_capita_scb_data["data"][i]["values"][0])

            # adding reference 'reference_url' to the data
            reference_url = wdi_core.WDUrl(value = "https://data.stat.gov.lv/pxweb/lv/OSP_PUB/START__VEK__IK__IKR/IKR060/",
                                        prop_nr = "P854", is_reference = True, rank = "preferred")

            # commiting the data
            data_gdp_capita = wdi_core.WDQuantity(value=gdp_capita, prop_nr = 'P2132', qualifiers=[point_in_time, determination_method],
                                        references = [[title, publisher, reference_url, retrieved]], unit="Q4916", rank = "preferred")
            data.append(data_gdp_capita)

    # uploading the data
    if data != []:
        wd_item = wdi_core.WDItemEngine(wd_item_id = area_q_code, new_item = False, data = data)

        try:
            wd_item.write(login_instance)

            print(f"Successfully wrote the data to {area_name} ({area_q_code}):")

            if gdp is not None:
                print(f"GDP - {gdp} EUR")
            if gdp_capita is not None:
                print(f"GDP per capita - {gdp_capita} EUR")
            print("")

        except wdi_core.WDApiError:
            print(f"Failed to write the data to {area_name} ({area_q_code})\n")

        # waiting to avoid being rate limited
        time.sleep(8)


input("The program has completed successfully.\nPress ENTER to quit")
