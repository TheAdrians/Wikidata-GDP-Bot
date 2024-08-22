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
gdp_areas_atvk = ["LV0020000", "LV0021000", "LV0022000", "LV0023000", "LV0024000", "LV0025000", "LV0026000", "LV0027000", "LV0028000", "LV0029000", "LV0030000", "LV0031000", "LV0032000", "LV0033000", "LV0034000", "LV0035000", "LV0036000", "LV0037000", "LV0038000", "LV0039000", "LV0040000", "LV0041000", "LV0042000", "LV0043000", "LV0044000", "LV0045000", "LV0046000", "LV0047000", "LV0048000", "LV0049000", "LV0051000", "LV0052000", "LV0053000", "LV0054000", "LV0055000", "LV0056000"]
scb = SCB("lv", "VEK", "IK", "IKR", "IKR060")
scb.set_query(INDICATOR = ["B1GQ"], AREA = gdp_areas_atvk, ContentsCode = ["IKR060"], TIME = ["2021"])
scb.get_query()
gdp_scb_data = scb.get_data()


# getting GDP per capita data from db
gdp_capita_areas_atvk = ["LV0020000", "LV0021000", "LV0022000", "LV0023000", "LV0024000", "LV0025000", "LV0026000", "LV0027000", "LV0028000", "LV0029000", "LV0030000", "LV0031000", "LV0032000", "LV0033000", "LV0034000", "LV0035000", "LV0036000", "LV0037000", "LV0038000", "LV0039000", "LV0040000", "LV0041000", "LV0042000", "LV0043000", "LV0044000", "LV0045000", "LV0046000", "LV0047000", "LV0048000", "LV0049000", "LV0051000", "LV0052000", "LV0053000", "LV0054000", "LV0055000", "LV0056000"]
scb = SCB("lv", "VEK", "IK", "IKR", "IKR060")
scb.set_query(INDICATOR = ["B1GQ"], AREA = gdp_capita_areas_atvk, ContentsCode = ["IKR061"], TIME = ["2021"])
scb.get_query()
gdp_capita_scb_data = scb.get_data()


# getting area's data from wikidata
query = """
SELECT DISTINCT ?object ?objectLabel ?atvk ?nuts
WHERE
{
        #Novadi
        { ?item wdt:P31 wd:Q3345345}.
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
    publisher = wdi_core.WDItemID(value = "Q39420022", prop_nr = "P123", is_reference = True, rank = "preferred")
    retrieved = wdi_core.WDTime(time = f"+{today}T00:00:00Z", prop_nr = "P813", is_reference = True, rank = "preferred")

    # finding the correct GDP to the corresponding area
    for i in range(len(gdp_areas_atvk)):
        if gdp_areas_atvk[i] == area_atvk:
            gdp =  int(gdp_scb_data["data"][i]["values"][0]) * 1000

            # adding references 'title' and 'reference_url' to the data
            title = wdi_core.WDMonolingualText(value = "Iekšzemes kopprodukts un bruto pievienotā vērtība statistiskajos reģionos, valstspilsētās un "
                                               "novados faktiskajās cenās (pēc administratīvi teritoriālās reformas 2021. gadā)",
                                        prop_nr = "P1476", is_reference = True, rank = "preferred", language='lv')
            reference_url = wdi_core.WDUrl(value = "https://data.stat.gov.lv/pxweb/lv/OSP_PUB/START__VEK__IK__IKR/IKR060/",
                                        prop_nr = "P854", is_reference = True, rank = "preferred")

            # commiting the data
            data_gdp = wdi_core.WDQuantity(value=gdp, prop_nr = 'P2131', qualifiers=[point_in_time, determination_method],
                                        references = [[title, publisher, reference_url, retrieved]], unit="Q4916", rank = "preferred")
            data.append(data_gdp)

    # finding the correct GDP per capita to the corresponding area
    for i in range(len(gdp_capita_areas_atvk)):
        if gdp_capita_areas_atvk[i] == area_atvk:
            gdp_capita = int(gdp_capita_scb_data["data"][i]["values"][0])

            # adding references 'title' and 'reference_url' to the data
            title = wdi_core.WDMonolingualText(value = "Iekšzemes kopprodukts un bruto pievienotā vērtība uz vienu iedzīvotāju statistiskajos reģionos, "
                                               "valstspilsētās un novados faktiskajās cenās (pēc administratīvi teritoriālās reformas 2021. gadā)",
                                        prop_nr = "P1476", is_reference = True, rank = "preferred", language='lv')
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
