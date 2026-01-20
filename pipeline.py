import pandas as pd
import numpy as np


PIPELINE_FUNCTIONS_LIST = [
    # -------------------- Coordinates --------------------
    "nest_longitude",
    "nest_latitude",
    "p1_longitude",
    "p1_latitude",
    "p2_longitude",
    "p2_latitude",
    "p3_longitude",
    "p3_latitude",
    "p4_longitude",
    "p4_latitude",
    "p5_longitude",
    "p5_latitude",

    # -------------------- Distances from nest --------------------
    "p1_dist_from_nest_code",
    "p2_dist_from_nest_code",
    "p3_dist_from_nest_code",
    "p4_dist_from_nest_code",
    "p5_dist_from_nest_code",
    "min_adult_dist_from_nest_code",

    # -------------------- Adult presence --------------------
    "AF_Present",
    "AM_Present",
    "UA1_Present",
    "UA2_Present",
    "at_least_one_adult_present",

    # -------------------- Juvenile presence --------------------
    "JV1_Present",
    "JV2_Present",
    "JV3_Present",
    "At_least_one_JV_Present",

    # -------------------- Juvenile–Adult distances --------------------
    "jv1_dist_from_nearest_adult_code",
    "jv2_dist_from_nearest_adult_code",
    "jv3_dist_from_nearest_adult_code",

    # -------------------- AF --------------------
    "AF_In_Nest_Tree",
    "AF_Attending",
    "AF_three_and_four",
    "AF_Flying",
    "AF_Eating",
    "AF_Feeding_Young",
    "AF_NestBuilding",
    "AF_Incubating",
    "AF_Brooding",
    "AF_Aggressive_Interaction",
    "AF_Copulating",
    "af_surveytime",

    # -------------------- AM --------------------
    "AM_In_Nest_Tree",
    "AM_Attending",
    "AM_three_and_four",
    "AM_Flying",
    "AM_Eating",
    "AM_Feeding_Young",
    "AM_NestBuilding",
    "AM_Incubating",
    "AM_Brooding",
    "AM_Aggressive_Interaction",
    "AM_Copulating",
    "am_surveytime",

    # -------------------- UA1 --------------------
    "UA1_In_Nest_Tree",
    "UA1_Attending",
    "UA1_three_and_four",
    "UA1_Flying",
    "UA1_Eating",
    "UA1_Feeding_Young",
    "UA1_NestBuilding",
    "UA1_Incubating",
    "UA1_Brooding",
    "UA1_Aggressive_Interaction",
    "UA1_Copulating",
    "ua1_surveytime",

    # -------------------- UA2 --------------------
    "UA2_In_Nest_Tree",
    "UA2_Attending",
    "UA2_three_and_four",
    "UA2_Flying",
    "UA2_Eating",
    "UA2_Feeding_Young",
    "UA2_NestBuilding",
    "UA2_Incubating",
    "UA2_Brooding",
    "UA2_Aggressive_Interaction",
    "UA2_Copulating",
    "ua2_surveytime",

    # -------------------- JV1 --------------------
    "JV1_In_Nest_Tree",
    "JV1_Attending",
    "jv1_surveytime",
    "JV1_Aggressive_Interaction",
    "JV1_Flying",
    "JV1_Eating",
    "JV1_Feeding_Young",
    "JV1_NestBuilding",
    "JV1_Copulating",
    "JV1_three_and_four",
    "JV1_Brooding",
    "JV1_Incubating",
    "jv1_dist_from_af_ua1_code",
    "jv1_dist_from_am_ua2_code",
    "No_Adults_Present_but_JV1_Present",

    # -------------------- JV2 --------------------
    "JV2_In_Nest_Tree",
    "JV2_Attending",
    "jv2_surveytime",
    "JV2_Aggressive_Interaction",
    "JV2_Flying",
    "JV2_Eating",
    "JV2_Feeding_Young",
    "JV2_NestBuilding",
    "JV2_Copulating",
    "JV2_three_and_four",
    "JV2_Brooding",
    "JV2_Incubating",
    "jv2_dist_from_af_ua1_code",
    "jv2_dist_from_am_ua2_code",
    "No_Adults_Present_but_JV2_Present",

    # -------------------- JV3 --------------------
    "JV3_In_Nest_Tree",
    "JV3_Attending",
    "jv3_surveytime",
    "JV3_Aggressive_Interaction",
    "JV3_Flying",
    "JV3_Eating",
    "JV3_Feeding_Young",
    "JV3_NestBuilding",
    "JV3_Copulating",
    "JV3_three_and_four",
    "JV3_Brooding",
    "JV3_Incubating",
    "jv3_dist_from_af_ua1_code",
    "jv3_dist_from_am_ua2_code",
    "No_Adults_Present_but_JV3_Present",

    # -------------------- FEHA --------------------
    "FEHA1_Present",
    "FEHA1_surveytime",
    "FEHA1_Aggressive_Interaction",
    "FEHA1_Flying",
    "FEHA1_Eating",
    "FEHA1_Feeding_Young",
    "FEHA1_NestBuilding",
    "FEHA1_Copulating",
    "FEHA1_Brooding",
    "FEHA1_Incubating",
    "FEHA1_In_Nest_Tree",
    "FEHA1_Attending",

    "FEHA2_Present",
    "FEHA2_surveytime",
    "FEHA2_Aggressive_Interaction",
    "FEHA2_Flying",
    "FEHA2_Eating",
    "FEHA2_Feeding_Young",
    "FEHA2_NestBuilding",
    "FEHA2_Copulating",
    "FEHA2_Brooding",
    "FEHA2_Incubating",
    "FEHA2_In_Nest_Tree",
    "FEHA2_Attending",

    "At_Least_One_FEHA_Present",

    # -------------------- Presence logic --------------------
    "no_adult_present",
    "only_one_adult_present",
    "two_adults_present",
    "at_least_one_adult_in_nest_tree",
    "two_adults_attending",
]

# --- Core constants ---
PERCH_COORDS = {
    "Nest": (-105.2244406, 40.19171332),
    "B": (-105.2253575, 40.19020633),
    "C": (-105.2208245, 40.19066225),
    "D": (-105.2233232, 40.19364778),
    "E": (-105.2242718, 40.19176352),
    "F": (-105.2224264, 40.19258836),
    "G": (-105.2208257, 40.19326157),
    "H": (-105.2268786, 40.19252552),
    "I": (-105.2269895, 40.19165888),
    "J": (-105.2253288, 40.19018101),
    "K": (-105.2247074, 40.19124004),
    "M": (-105.2268418, 40.19143469),
    "N": (-105.2263372, 40.19099532),
    "O": (-105.2206465, 40.18941934),
    "P": (-105.2240902, 40.18973338),
    "Q": (-105.2255059, 40.19179001),
    "R": (-105.2235627, 40.19495400),
    "S": (-105.2253065, 40.19007474),
    "T": (-105.2247982, 40.19146175),
    "U": (-105.2257932, 40.19270733),
    "V": (-105.2245692, 40.19303044),
    "W": (-105.2194728, 40.19549992),
    "X": (-105.2236688, 40.19557184),
    "Y": (-105.2202846, 40.19079453),
    "Z": (-105.2249855, 40.18971306),
    "AA": (-105.2240364, 40.19006977),
    "AB": (-105.2272454, 40.18249219),
    "AC": (-105.2275662, 40.18253372),
    "AD": (-105.2301444, 40.18310400),
    "AE": (-105.2216210, 40.19240688),
    "AF": (-105.2158183, 40.17288937),
    "AG": (-105.2140066, 40.18344636),
    "AH": (-105.2254356, 40.19212724),
    "AI": (-105.2227025, 40.19468481),
    "AJ": (-105.2232382, 40.19496117),
    "AK": (-105.2226642, 40.19378428),
    "AL": (-105.2194373, 40.19015807),
    "AM": (-105.2243452, 40.19335900),
    "AN": (-105.2257977, 40.19048349),
    "AO": (-105.2207615, 40.18997222),
    "AP": (-105.2245110, 40.19245637),
    "AQ": (-105.2299825, 40.21284818),
    "AS": (-105.2167905, 40.19537860),
    "AT": (-105.2154158, 40.19463003),
    "BA": (-105.2229454, 40.19927710),
    "BB": (-105.2177829, 40.20118277),
    "BC": (-105.2128362, 40.19766487),
    "BD": (-105.2160633, 40.19329568),
    "BE": (-105.2194839, 40.19191107),
    "BF": (-105.2478652, 40.18994495),
    "BG": (-105.2201216, 40.19551310),
    "BH": (-105.2206868, 40.19555823),
    "BI": (-105.2065147, 40.19386763),
    "BJ": (-105.2196940, 40.19093101),
    "BK": (-105.2241249, 40.19132706),
    "BL": (-105.2251638, 40.19178019),
    "BM": (-105.2235992, 40.19317182),
    "BN": (-105.2231233, 40.19304312),
    "BO": (-105.2253359, 40.19247523),
    "BP": (-105.2195319, 40.19060050),
    "BQ": (-105.2145986, 40.18306227),
    "BR": (-105.2145089, 40.18060378),
    "BS": (-105.2145289, 40.18873800),
    "BT": (-105.2253352, 40.19201664),
    "BU": (-105.2256609, 40.19364330),
    "BV": (-105.2256696, 40.19531169),
    "BW": (-105.2252823, 40.19484301),
    "BX": (-105.2237690, 40.19193063),
    "CA": (-105.2207082, 40.19064959),
    "CB": (-105.2208470, 40.19087956),
    "CC": (-105.2244864, 40.20062440),
    "CD": (-105.2187429, 40.19372252),
    "CE": (-105.2454394, 40.18887036),
    "CF": (-105.2217055, 40.19665622),
    "CG": (-105.2196370, 40.20246492),
    "CH": (-105.2249314, 40.19614144),
    "CI": (-105.2197192, 40.19357947),
    "CJ": (-105.2169999, 40.19469618),
    "CK": (-105.2179770, 40.19494140),
    "CL": (-105.2173569, 40.19504685),
    "CM": (-105.2138364, 40.21254025),
    "CN": (-105.2141413, 40.21672429),
    "CO": (-105.2134798, 40.21883070),
    "CP": (-105.2089119, 40.21797621),
    "CQ": (-105.2166178, 40.19064564),
    "CR": (-105.2113294, 40.21545741),
    "CS": (-105.2162945, 40.22409018),
    "CT": (-105.2121565, 40.21802762),
    "CU": (-105.2157505, 40.21962240),
    "CV": (-105.2205999, 40.22152254),
    "CW": (-105.2258118, 40.19929300),
    "CX": (-105.2207752, 40.19584563),
    "CY": (-105.2198810, 40.21979664),
    "CZ": (-105.2162554, 40.21766207),
    "DA": (-105.2179376, 40.22049004),
    "DB": (-105.2160189, 40.22356756),
    "DC": (-105.2087890, 40.18841745),
    "DD": (-105.2257063, 40.19422971),
    "DE": (-105.2152381, 40.23029484),
    "DF": (-105.2191810, 40.22284514),
    "DG": (-105.2182291, 40.22022113),
    "DH": (-105.2262483, 40.20154396),
    "DI": (-105.2187823, 40.21976717),
    "DJ": (-105.2208031, 40.22408560),
    "DK": (-105.2149868, 40.23117436),
    "DL": (-105.2157379, 40.23035159),
    "DM": (-105.2167695, 40.22853232),
    "DN": (-105.2299361, 40.21789510),
    "DO": (-105.2255730, 40.19568637),
    "DP": (-105.2254998, 40.19456077),
    "DQ": (-105.2166394, 40.19131919),
    "DR": (-105.2155961, 40.19035513),
    "DS": (-105.2184522, 40.19018652),
    "DT": (-105.2151644, 40.19048756),
    "DU": (-105.2215743, 40.19373824),
    "DV": (-105.2175110, 40.19063970),
    "DW": (-105.2097948, 40.22333037),
    "DX": (-105.2170149, 40.22337670),
    "DY": (-105.2217088, 40.22151214),
    "DZ": (-105.2093135, 40.22168134),
    "EA": (-105.2179582, 40.19345194),
    "EB": (-105.2154502, 40.22511379),
    "EC": (-105.2235314, 40.22924749),
    "ED": (-105.2158480, 40.22687863),
    "EE": (-105.2181254, 40.22306944),
    "EF": (-105.2138832, 40.22383720),
    "EG": (-105.2127369, 40.22177448),
    "EH": (-105.2101851, 40.22128730),
    "EI": (-105.2096295, 40.22063416),
    "EJ": (-105.2115692, 40.22252768),
    "EK": (-105.2107741, 40.22501516),
    "EL": (-105.2129103, 40.22408066),
    "EM": (-105.2108451, 40.22455068),
    "EN": (-105.2155390, 40.18896297),
    "EO": (-105.2098222, 40.22273047),
    "EP": (-105.2140512, 40.22495191),
    "EQ": (-105.2155033, 40.22189062),
    "ER": (-105.2136700, 40.22597773),
    "ES": (-105.2149956, 40.22373145),
    "ET": (-105.2100566, 40.22339615),
    "EU": (-105.2135132, 40.22376663),
    "EV": (-105.2125117, 40.23937015),
    "EW": (-105.2118406, 40.22548537),
    "EX": (-105.2250838, 40.24322956),
    "EY": (-105.2238713, 40.23643435),
    "EZ": (-105.2184546, 40.22970219),
    "FA": (-105.2242420, 40.19888883),
    "FB": (-105.2197798, 40.19531867),
    "FC": (-105.2239959, 40.19463861),
    "FD": (-105.2234908, 40.19512776),
    "FE": (-105.2234037, 40.19507808),
    "FF": (-105.2106891, 40.19355394),
    "FG": (-105.2187739, 40.19328996),
    "FH": (-105.2194211, 40.19471451),
    "FI": (-105.2244931, 40.19396385),
    "FJ": (-105.2204564, 40.18950486),
    "FK": (-105.2207242, 40.19676675),
    "FL": (-105.2091819, 40.22312129),
    "FM": (-105.2253490, 40.18959581),
    "FN": (-105.2197103, 40.21710690),
    "FO": (-105.2173628, 40.19446059),
    "FP": (-105.2204444, 40.21962783),
    "FQ": (-105.2200441, 40.19636473),
    "FR": (-105.2199801, 40.22570354),
    "FS": (-105.2205279, 40.22597735),
    "FT": (-105.2089058, 40.19342360),
    "FU": (-105.2255379, 40.19808880),
    "FV": (-105.2260542, 40.18909072),
    "FW": (-105.2152962, 40.19393689),
    "FX": (-105.2141785, 40.21877027),
    "FY": (-105.2246177, 40.19777707),
    "FZ": (-105.2172714, 40.22050371),
    "GA": (-105.2268348, 40.19157268),
    "GB": (-105.2261316, 40.19017088),
    "GC": (-105.2103884, 40.21495812),
    "GD": (-105.2208816, 40.19085146),
    "GE": (-105.2238029, 40.19602806),
    "GF": (-105.1981935, 40.18888942),
    "GG": (-105.2239380, 40.19609849),
    "GH": (-105.2196960, 40.21987212),
    "GI": (-105.2119423, 40.22443824),
}

EARTH_RADIUS_M = 6_371_000
EXCLUDED_LOCATION_CODE = 12

# --- Helper functions ---
def great_circle_distance(lat1_deg, lon1_deg, lat2_deg, lon2_deg):
    lat1 = np.radians(lat1_deg)
    lon1 = np.radians(lon1_deg)
    lat2 = np.radians(lat2_deg)
    lon2 = np.radians(lon2_deg)
    return EARTH_RADIUS_M * np.arccos(
        np.clip(
            np.sin(lat1) * np.sin(lat2) + np.cos(lat1) * np.cos(lat2) * np.cos(lon1 - lon2),
            -1.0, 1.0
        )
    )

# =============================================================================
# ADULT FEMALE (AF) FUNCTIONS
# =============================================================================
def AF1_AF14(df):
    df['female.loc'] = pd.to_numeric(df['female.loc'], errors='coerce') 
    
    for i in range(1, 15):
        df[f'AF{i}'] = np.where(df['female.loc']== i, 3, 0)
    return df


def af_surveytime(df):
    mask = df["female.loc"].notna() & (df["female.loc"] != 12)
    df["AF_surveytime"] = np.where(mask, 3, 0)
    return df

def AF_In_Nest_Tree(df):
    df = AF1_AF14(df)
    df['AF_In_Nest_Tree'] = np.where((df['AF1'] == 3) | (df['AF2'] == 3), 3, 0)
    return df

def AF_Attending(df):
    df = AF1_AF14(df)
    #attending_cols = ['AF1', 'AF2', 'AF3', 'AF4']
    attending_cols = ['AF7', 'AF8', 'AF14']
    df['AF_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df

def AF_Present(df):
    df = AF1_AF14(df)
    present_cols = ['AF1', 'AF2', 'AF3', 'AF4', 'AF5', 'AF6', 'AF7', 'AF8']
    df['AF_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df

def AF_three_and_four(df):
    df = AF1_AF14(df)
    df['AF_3+4'] = np.where((df['AF3'] == 3) | (df['AF4'] == 3), 3, 0)
    return df

def AF_Flying(df):
    df = AF1_AF14(df)
    flying_cols = ['AF7', 'AF8', 'AF10', 'AF11', 'AF14']
    df['AF_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df

def AF_Eating(df):
    df['AF_Eating'] = np.where(df['female.be'] == 'C', 3, 0)
    return df

def AF_Feeding_Young(df):
    df['AF_Feeding_Young'] = np.where(df['female.be'] == 'M', 3, 0)
    return df

def AF_NestBuilding(df):
    df['AF_NestBuilding'] = np.where(df['female.be'] == 'D', 3, 0)
    return df

def AF_Incubating(df):
    df['AF_Incubating'] = np.where(df['female.be'] == 'L', 3, 0)
    return df

def AF_Brooding(df):
    df['AF_Brooding'] = np.where(df['female.be'] == 'Q', 3, 0)
    return df

def AF_Aggressive_Interaction(df):
    df['AF_Aggressive_Interaction'] = np.where(df['female.be'] == 'F', 3, 0)
    return df

def AF_Copulating(df):
    df['AF_Copulating'] = np.where(df['female.be'] == 'H', 3, 0)
    return df

# =============================================================================
# ADULT MALE (AM) FUNCTIONS
# =============================================================================
def AM1_AM14(df):
    df['male.loc'] = pd.to_numeric(df['male.loc'], errors='coerce')
    for i in range(1, 15):
        df[f'AM{i}'] = np.where(df['male.loc'] == i, 3, 0)
    return df

def am_surveytime(df):
    mask = df["male.loc"].notna() & (df["male.loc"] != 12)
    df["AM_surveytime"] = np.where(mask, 3, 0)
    return df

def AM_In_Nest_Tree(df):
    df = AM1_AM14(df)
    df['AM_In_Nest_Tree'] = np.where((df['AM1'] == 3) | (df['AM2'] == 3), 3, 0)
    return df

def AM_Attending(df):
    df = AM1_AM14(df)
    #attending_cols = ['AM1', 'AM2', 'AM3', 'AM4']
    attending_cols = ['AM7', 'AM8', 'AM14']
    df['AM_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df

def AM_Present(df):
    df = AM1_AM14(df)
    present_cols = ['AM1', 'AM2', 'AM3', 'AM4', 'AM5', 'AM6', 'AM7', 'AM8']
    df['AM_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df

def AM_three_and_four(df):
    df = AM1_AM14(df)
    df['AM_3+4'] = np.where((df['AM3'] == 3) | (df['AM4'] == 3), 3, 0)
    return df

def AM_Flying(df):
    df = AM1_AM14(df)
    flying_cols = ['AM7', 'AM8', 'AM10', 'AM11', 'AM14']
    df['AM_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df

def AM_Eating(df):
    df['AM_Eating'] = np.where(df['male.be'] == 'C', 3, 0)
    return df

def AM_Feeding_Young(df):
    df['AM_Feeding_Young'] = np.where(df['male.be'] == 'M', 3, 0)
    return df

def AM_NestBuilding(df):
    df['AM_NestBuilding'] = np.where(df['male.be'] == 'D', 3, 0)
    return df

def AM_Incubating(df):
    df['AM_Incubating'] = np.where(df['male.be'] == 'L', 3, 0)
    return df

def AM_Brooding(df):
    df['AM_Brooding'] = np.where(df['male.be'] == 'Q', 3, 0)
    return df

def AM_Aggressive_Interaction(df):
    df['AM_Aggressive_Interaction'] = np.where(df['male.be'] == 'F', 3, 0)
    return df

def AM_Copulating(df):
    df['AM_Copulating'] = np.where(df['male.be'] == 'H', 3, 0)
    return df

# =============================================================================
# UNDIFFERENTIATED ADULT 1 (UA1) FUNCTIONS
# =============================================================================
def UA11_UA114(df):
    df['undiff1.loc'] = pd.to_numeric(df['undiff1.loc'], errors='coerce')
    for i in range(1, 15):
        df[f'UA1_{i}'] = np.where(df['undiff1.loc'] == i, 3, 0)
    return df

def ua1_surveytime(df):
    mask = df["undiff1.loc"].notna() & (df["undiff1.loc"] != 12)
    df["UA1_surveytime"] = np.where(mask, 3, 0)
    return df

def UA1_In_Nest_Tree(df):
    df = UA11_UA114(df)
    df['UA1_In_Nest_Tree'] = np.where((df['UA1_1'] == 3) | (df['UA1_2'] == 3), 3, 0)
    return df

def UA1_Attending(df):
    df = UA11_UA114(df)
    #attending_cols = ['UA1_1', 'UA1_2', 'UA1_3', 'UA1_4']
    attending_cols = ['UA1_7', 'UA1_8', 'UA1_14']
    df['UA1_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df

def UA1_Present(df):
    df = UA11_UA114(df)
    present_cols = ['UA1_1', 'UA1_2', 'UA1_3', 'UA1_4', 'UA1_5', 'UA1_6', 'UA1_7', 'UA1_8']
    df['UA1_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df

def UA1_three_and_four(df):
    df = UA11_UA114(df)
    df['UA1_3+4'] = np.where((df['UA1_3'] == 3) | (df['UA1_4'] == 3), 3, 0)
    return df

def UA1_Flying(df):
    df = UA11_UA114(df)
    flying_cols = ['UA1_7', 'UA1_8', 'UA1_10', 'UA1_11', 'UA1_14']
    df['UA1_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df

def UA1_Eating(df):
    df['UA1_Eating'] = np.where(df['undiff1.be'] == 'C', 3, 0)
    return df

def UA1_Feeding_Young(df):
    df['UA1_Feeding_Young'] = np.where(df['undiff1.be'] == 'M', 3, 0)
    return df

def UA1_NestBuilding(df):
    df['UA1_NestBuilding'] = np.where(df['undiff1.be'] == 'D', 3, 0)
    return df

def UA1_Incubating(df):
    df['UA1_Incubating'] = np.where(df['undiff1.be'] == 'L', 3, 0)
    return df

def UA1_Brooding(df):
    df['UA1_Brooding'] = np.where(df['undiff1.be'] == 'Q', 3, 0)
    return df

def UA1_Aggressive_Interaction(df):
    df['UA1_Aggressive_Interaction'] = np.where(df['undiff1.be'] == 'F', 3, 0)
    return df

def UA1_Copulating(df):
    df['UA1_Copulating'] = np.where(df['undiff1.be'] == 'H', 3, 0)
    return df

# =============================================================================
# UNDIFFERENTIATED ADULT 2 (UA2) FUNCTIONS
# =============================================================================
def UA21_UA214(df):
    df['undiff2.loc'] = pd.to_numeric(df['undiff2.loc'], errors='coerce')
    for i in range(1, 15):
        df[f'UA2_{i}'] = np.where(df['undiff2.loc'] == i, 3, 0)
    return df

def ua2_surveytime(df):
    mask = df["undiff2.loc"].notna() & (df["undiff2.loc"] != 12)
    df["UA2_surveytime"] = np.where(mask, 3, 0)
    return df

def UA2_In_Nest_Tree(df):
    df = UA21_UA214(df)
    df['UA2_In_Nest_Tree'] = np.where((df['UA2_1'] == 3) | (df['UA2_2'] == 3), 3, 0)
    return df

def UA2_Attending(df):
    df = UA21_UA214(df)
    #attending_cols = ['UA2_1', 'UA2_2', 'UA2_3', 'UA2_4']
    attending_cols = ['UA2_7', 'UA2_8', 'UA2_14']
    df['UA2_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df

def UA2_Present(df):
    df = UA21_UA214(df)
    present_cols = ['UA2_1', 'UA2_2', 'UA2_3', 'UA2_4', 'UA2_5', 'UA2_6', 'UA2_7', 'UA2_8']
    df['UA2_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df

def UA2_three_and_four(df):
    df = UA21_UA214(df)
    df['UA2_3+4'] = np.where((df['UA2_3'] == 3) | (df['UA2_4'] == 3), 3, 0)
    return df

def UA2_Flying(df):
    df = UA21_UA214(df)
    flying_cols = ['UA2_7', 'UA2_8', 'UA2_10', 'UA2_11', 'UA2_14']
    df['UA2_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df

def UA2_Eating(df):
    df['UA2_Eating'] = np.where(df['undiff2.be'] == 'C', 3, 0)
    return df

def UA2_Feeding_Young(df):
    df['UA2_Feeding_Young'] = np.where(df['undiff2.be'] == 'M', 3, 0)
    return df

def UA2_NestBuilding(df):
    df['UA2_NestBuilding'] = np.where(df['undiff2.be'] == 'D', 3, 0)
    return df

def UA2_Incubating(df):
    df['UA2_Incubating'] = np.where(df['undiff2.be'] == 'L', 3, 0)
    return df

def UA2_Brooding(df):
    df['UA2_Brooding'] = np.where(df['undiff2.be'] == 'Q', 3, 0)
    return df

def UA2_Aggressive_Interaction(df):
    df['UA2_Aggressive_Interaction'] = np.where(df['undiff2.be'] == 'F', 3, 0)
    return df

def UA2_Copulating(df):
    df['UA2_Copulating'] = np.where(df['undiff2.be'] == 'H', 3, 0)
    return df

# =============================================================================
# JUVENILE 1 (JV1) FUNCTIONS
# =============================================================================
def JV11_JV114(df):
    df['juv1.loc'] = pd.to_numeric(df['juv1.loc'], errors='coerce')
    for i in range(1, 15):
        df[f'JV1_{i}'] = np.where(df['juv1.loc'] == i, 3, 0)
    return df

def jv1_surveytime(df):
    mask = df["juv1.loc"].notna() & (df["juv1.loc"] != 12)
    df["JV1_surveytime"] = np.where(mask, 3, 0)
    return df

def JV1_In_Nest_Tree(df):
    df = JV11_JV114(df)
    df['JV1_In_Nest_Tree'] = np.where((df['JV1_1'] == 3) | (df['JV1_2'] == 3), 3, 0)
    return df

def JV1_Attending(df):
    df = JV11_JV114(df)
    #attending_cols = ['JV1_1', 'JV1_2', 'JV1_3', 'JV1_4']
    attending_cols = ['JV1_7', 'JV1_8', 'JV1_14']
    df['JV1_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df

def JV1_Present(df):
    df = JV11_JV114(df)
    present_cols = ['JV1_1', 'JV1_2', 'JV1_3', 'JV1_4', 'JV1_5', 'JV1_6', 'JV1_7', 'JV1_8']
    df['JV1_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df

def JV1_three_and_four(df):
    df = JV11_JV114(df)
    df['JV1_3+4'] = np.where((df['JV1_3'] == 3) | (df['JV1_4'] == 3), 3, 0)
    return df

def JV1_Flying(df):
    df = JV11_JV114(df)
    flying_cols = ['JV1_7', 'JV1_8', 'JV1_10', 'JV1_11', 'JV1_14']
    df['JV1_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df

def JV1_Eating(df):
    df['JV1_Eating'] = np.where(df['juv1.be'] == 'C', 3, 0)
    return df

def JV1_Feeding_Young(df):
    df['JV1_Feeding_Young'] = np.where(df['juv1.be'] == 'M', 3, 0)
    return df

def JV1_NestBuilding(df):
    df['JV1_NestBuilding'] = np.where(df['juv1.be'] == 'D', 3, 0)
    return df

def JV1_Incubating(df):
    df['JV1_Incubating'] = np.where(df['juv1.be'] == 'L', 3, 0)
    return df

def JV1_Brooding(df):
    df['JV1_Brooding'] = np.where(df['juv1.be'] == 'Q', 3, 0)
    return df

def JV1_Aggressive_Interaction(df):
    df['JV1_Aggressive_Interaction'] = np.where(df['juv1.be'] == 'F', 3, 0)
    return df

def JV1_Copulating(df):
    df['JV1_Copulating'] = np.where(df['juv1.be'] == 'H', 3, 0)
    return df

# =============================================================================
# JUVENILE 2 (JV2) FUNCTIONS
# =============================================================================
def JV21_JV214(df):
    df['juv2.loc'] = pd.to_numeric(df['juv2.loc'], errors='coerce')
    for i in range(1, 15):
        df[f'JV2_{i}'] = np.where(df['juv2.loc'] == i, 3, 0)
    return df

def jv2_surveytime(df):
    mask = df["juv2.loc"].notna() & (df["juv2.loc"] != 12)
    df["JV2_surveytime"] = np.where(mask, 3, 0)
    return df

def JV2_In_Nest_Tree(df):
    df = JV21_JV214(df)
    df['JV2_In_Nest_Tree'] = np.where((df['JV2_1'] == 3) | (df['JV2_2'] == 3), 3, 0)
    return df

def JV2_Attending(df):
    df = JV21_JV214(df)
    #attending_cols = ['JV2_1', 'JV2_2', 'JV2_3', 'JV2_4']
    attending_cols = ['JV2_7', 'JV2_8', 'JV2_14']
    df['JV2_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df

def JV2_Present(df):
    df = JV21_JV214(df)
    present_cols = ['JV2_1', 'JV2_2', 'JV2_3', 'JV2_4', 'JV2_5', 'JV2_6', 'JV2_7', 'JV2_8']
    df['JV2_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df

def JV2_three_and_four(df):
    df = JV21_JV214(df)
    df['JV2_3+4'] = np.where((df['JV2_3'] == 3) | (df['JV2_4'] == 3), 3, 0)
    return df

def JV2_Flying(df):
    df = JV21_JV214(df)
    flying_cols = ['JV2_7', 'JV2_8', 'JV2_10', 'JV2_11', 'JV2_14']
    df['JV2_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df

def JV2_Eating(df):
    df['JV2_Eating'] = np.where(df['juv2.be'] == 'C', 3, 0)
    return df

def JV2_Feeding_Young(df):
    df['JV2_Feeding_Young'] = np.where(df['juv2.be'] == 'M', 3, 0)
    return df

def JV2_NestBuilding(df):
    df['JV2_NestBuilding'] = np.where(df['juv2.be'] == 'D', 3, 0)
    return df

def JV2_Incubating(df):
    df['JV2_Incubating'] = np.where(df['juv2.be'] == 'L', 3, 0)
    return df

def JV2_Brooding(df):
    df['JV2_Brooding'] = np.where(df['juv2.be'] == 'Q', 3, 0)
    return df

def JV2_Aggressive_Interaction(df):
    df['JV2_Aggressive_Interaction'] = np.where(df['juv2.be'] == 'F', 3, 0)
    return df

def JV2_Copulating(df):
    df['JV2_Copulating'] = np.where(df['juv2.be'] == 'H', 3, 0)
    return df

# =============================================================================
# JUVENILE 3 (JV3) FUNCTIONS
# =============================================================================
def JV31_JV314(df):
    df['juv3.loc'] = pd.to_numeric(df['juv3.loc'], errors='coerce')
    for i in range(1, 15):
        df[f'JV3_{i}'] = np.where(df['juv3.loc'] == i, 3, 0)
    return df

def jv3_surveytime(df):
    mask = df["juv3.loc"].notna() & (df["juv3.loc"] != 12)
    df["JV3_surveytime"] = np.where(mask, 3, 0)
    return df

def JV3_In_Nest_Tree(df):
    df = JV31_JV314(df)
    df['JV3_In_Nest_Tree'] = np.where((df['JV3_1'] == 3) | (df['JV3_2'] == 3), 3, 0)
    return df

def JV3_Attending(df):
    df = JV31_JV314(df)
    #attending_cols = ['JV3_1', 'JV3_2', 'JV3_3', 'JV3_4']
    attending_cols = ['JV3_7', 'JV3_8', 'JV3_14']
    df['JV3_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df

def JV3_Present(df):
    df = JV31_JV314(df)
    present_cols = ['JV3_1', 'JV3_2', 'JV3_3', 'JV3_4', 'JV3_5', 'JV3_6', 'JV3_7', 'JV3_8']
    df['JV3_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df

def JV3_three_and_four(df):
    df = JV31_JV314(df)
    df['JV3_3+4'] = np.where((df['JV3_3'] == 3) | (df['JV3_4'] == 3), 3, 0)
    return df

def JV3_Flying(df):
    df = JV31_JV314(df)
    flying_cols = ['JV3_7', 'JV3_8', 'JV3_10', 'JV3_11', 'JV3_14']
    df['JV3_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df

def JV3_Eating(df):
    df['JV3_Eating'] = np.where(df['juv3.be'] == 'C', 3, 0)
    return df

def JV3_Feeding_Young(df):
    df['JV3_Feeding_Young'] = np.where(df['juv3.be'] == 'M', 3, 0)
    return df

def JV3_NestBuilding(df):
    df['JV3_NestBuilding'] = np.where(df['juv3.be'] == 'D', 3, 0)
    return df

def JV3_Incubating(df):
    df['JV3_Incubating'] = np.where(df['juv3.be'] == 'L', 3, 0)
    return df

def JV3_Brooding(df):
    df['JV3_Brooding'] = np.where(df['juv3.be'] == 'Q', 3, 0)
    return df

def JV3_Aggressive_Interaction(df):
    df['JV3_Aggressive_Interaction'] = np.where(df['juv3.be'] == 'F', 3, 0)
    return df

def JV3_Copulating(df):
    df['JV3_Copulating'] = np.where(df['juv3.be'] == 'H', 3, 0)
    return df

# =============================================================================
# FERRUGINOUS HAWK 1 (FEHA1) FUNCTIONS
# =============================================================================
def FEHA11_FEHA114(df):
    df['FEHA_1.loc'] = pd.to_numeric(df['FEHA_1.loc'], errors='coerce')
    for i in range(1, 15):
        df[f'FEHA1_{i}'] = np.where(df['FEHA_1.loc'] == i, 3, 0)
    return df

def FEHA1_surveytime(df):
    mask = df["FEHA_1.loc"].notna() & (df["FEHA_1.loc"] != 12)
    df["FEHA1_surveytime"] = np.where(mask, 3, 0)
    return df

def FEHA1_Present(df):
    df = FEHA11_FEHA114(df)
    present_cols = ['FEHA1_1', 'FEHA1_2', 'FEHA1_3', 'FEHA1_4', 'FEHA1_5', 'FEHA1_6', 'FEHA1_7', 'FEHA1_8']
    df['FEHA1_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df
def FEHA1_Flying(df):
    df = FEHA11_FEHA114(df)
    flying_cols = ['FEHA1_7', 'FEHA1_8', 'FEHA1_10', 'FEHA1_11', 'FEHA1_14']
    df['FEHA1_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df
def FEHA1_Eating(df):
    df['FEHA1_Eating'] = np.where(df['FEHA_1.be'] == 'C', 3, 0)
    return df
def FEHA1_Feeding_Young(df):
    df['FEHA1_Feeding_Young'] = np.where(df['FEHA_1.be'] == 'M', 3, 0)
    return df
def FEHA1_NestBuilding(df):
    df['FEHA1_NestBuilding'] = np.where(df['FEHA_1.be'] == 'D', 3, 0)
    return df
def FEHA1_Incubating(df):
    df['FEHA1_Incubating'] = np.where(df['FEHA_1.be'] == 'L', 3, 0)
    return df
def FEHA1_Brooding(df):
    df['FEHA1_Brooding'] = np.where(df['FEHA_1.be'] == 'Q', 3, 0)
    return df
def FEHA1_Aggressive_Interaction(df):
    df['FEHA1_Aggressive_Interaction'] = np.where(df['FEHA_1.be'] == 'F', 3, 0)
    return df
def FEHA1_Copulating(df):
    df['FEHA1_Copulating'] = np.where(df['FEHA_1.be'] == 'H', 3, 0)
    return df   
def FEHA1_In_Nest_Tree(df):
    df = FEHA11_FEHA114(df)
    df['FEHA1_In_Nest_Tree'] = np.where((df['FEHA1_1'] == 3) | (df['FEHA1_2'] == 3), 3, 0)
    return df
def FEHA1_Attending(df):
    df = FEHA11_FEHA114(df)
    #attending_cols = ['FEHA1_1', 'FEHA1_2', 'FEHA1_3', 'FEHA1_4']
    attending_cols = ['FEHA1_7', 'FEHA1_8', 'FEHA1_14']
    df['FEHA1_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df


# =============================================================================
# FERRUGINOUS HAWK 2 (FEHA2) FUNCTIONS
# =============================================================================
def FEHA21_FEHA214(df):
    df['FEHA_2.loc'] = pd.to_numeric(df['FEHA_2.loc'], errors='coerce')
    for i in range(1, 15):
        df[f'FEHA2_{i}'] = np.where(df['FEHA_2.loc'] == i, 3, 0)
    return df

def FEHA2_surveytime(df):
    mask = df["FEHA_2.loc"].notna() & (df["FEHA_2.loc"] != 12)
    df["FEHA2_surveytime"] = np.where(mask, 3, 0)
    return df

def FEHA2_Present(df):
    df = FEHA21_FEHA214(df)
    present_cols = ['FEHA2_1', 'FEHA2_2', 'FEHA2_3', 'FEHA2_4', 'FEHA2_5', 'FEHA2_6', 'FEHA2_7', 'FEHA2_8']
    df['FEHA2_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df
def FEHA2_Flying(df):
    df = FEHA21_FEHA214(df)
    flying_cols = ['FEHA2_7', 'FEHA2_8', 'FEHA2_10', 'FEHA2_11', 'FEHA2_14']
    df['FEHA2_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df
def FEHA2_Eating(df):
    df['FEHA2_Eating'] = np.where(df['FEHA_2.be'] == 'C', 3, 0)
    return df
def FEHA2_Feeding_Young(df):
    df['FEHA2_Feeding_Young'] = np.where(df['FEHA_2.be'] == 'M', 3, 0)
    return df
def FEHA2_NestBuilding(df):
    df['FEHA2_NestBuilding'] = np.where(df['FEHA_2.be'] == 'D', 3, 0)
    return df
def FEHA2_Incubating(df):
    df['FEHA2_Incubating'] = np.where(df['FEHA_2.be'] == 'L', 3, 0)
    return df
def FEHA2_Brooding(df):
    df['FEHA2_Brooding'] = np.where(df['FEHA_2.be'] == 'Q', 3, 0)
    return df
def FEHA2_Aggressive_Interaction(df):
    df['FEHA2_Aggressive_Interaction'] = np.where(df['FEHA_2.be'] == 'F', 3, 0)
    return df
def FEHA2_Copulating(df):
    df['FEHA2_Copulating'] = np.where(df['FEHA_2.be'] == 'H', 3, 0)
    return df
def FEHA2_In_Nest_Tree(df):
    df = FEHA21_FEHA214(df)
    df['FEHA2_In_Nest_Tree'] = np.where((df['FEHA2_1'] == 3) | (df['FEHA2_2'] == 3), 3, 0)
    return df
def FEHA2_Attending(df):
    df = FEHA21_FEHA214(df)
    #attending_cols = ['FEHA2_1', 'FEHA2_2', 'FEHA2_3', 'FEHA2_4']
    attending_cols = ['FEHA2_7', 'FEHA2_8', 'FEHA2_14']
    df['FEHA2_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df

# =============================================================================
# OTHER BIRD OF PREY (OBE) FUNCTIONS
# =============================================================================
def OBE1_OBE14(df):
    df['OBE.loc'] = pd.to_numeric(df['OBE.loc'], errors='coerce')
    for i in range(1, 15):
        df[f'OBE_{i}'] = np.where(df['OBE.loc'] == i, 3, 0)
    return df

def OBE_surveytime(df):
    mask = df["OBE.loc"].notna() & (df["OBE.loc"] != 12)
    df["OBE_surveytime"] = np.where(mask, 3, 0)
    return df

def OBE_Present(df):
    df = OBE1_OBE14(df)
    present_cols = ['OBE_1', 'OBE_2', 'OBE_3', 'OBE_4', 'OBE_5', 'OBE_6', 'OBE_7', 'OBE_8']
    df['OBE_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df
def OBE_Flying(df):
    df = OBE1_OBE14(df)
    flying_cols = ['OBE_7', 'OBE_8', 'OBE_10', 'OBE_11', 'OBE_14']
    df['OBE_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df
def OBE_Eating(df):
    df['OBE_Eating'] = np.where(df['OBE.be'] == 'C', 3, 0)
    return df
def OBE_Feeding_Young(df):
    df['OBE_Feeding_Young'] = np.where(df['OBE.be'] == 'M', 3, 0)
    return df
def OBE_NestBuilding(df):
    df['OBE_NestBuilding'] = np.where(df['OBE.be'] == 'D', 3, 0)
    return df
def OBE_Incubating(df):
    df['OBE_Incubating'] = np.where(df['OBE.be'] == 'L', 3, 0)
    return df
def OBE_Brooding(df):
    df['OBE_Brooding'] = np.where(df['OBE.be'] == 'Q', 3, 0)
    return df
def OBE_Aggressive_Interaction(df):
    df['OBE_Aggressive_Interaction'] = np.where(df['OBE.be'] == 'F', 3, 0)
    return df
def OBE_Copulating(df):
    df['OBE_Copulating'] = np.where(df['OBE.be'] == 'H', 3, 0)
    return df
def OBE_In_Nest_Tree(df):
    df = OBE1_OBE14(df)
    df['OBE_In_Nest_Tree'] = np.where((df['OBE_1'] == 3) | (df['OBE_2'] == 3), 3, 0)
    return df
def OBE_Attending(df):
    df = OBE1_OBE14(df)
    #attending_cols = ['OBE_1', 'OBE_2', 'OBE_3', 'OBE_4']
    attending_cols = ['OBE_7', 'OBE_8', 'OBE_14']
    df['OBE_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df


# =============================================================================
# GOLDEN EAGLE 1 (GOEA_1) FUNCTIONS
# =============================================================================
def GOEA_11_GOEA_114(df):
    df['GOEA_1.loc'] = pd.to_numeric(df['GOEA_1.loc'], errors='coerce')
    for i in range(1, 15):
        df[f'GOEA1_{i}'] = np.where(df['GOEA_1.loc'] == i, 3, 0)
    return df

def GOEA_1_surveytime(df):
    mask = df["GOEA_1.loc"].notna() & (df["GOEA_1.loc"] != 12)
    df["GOEA_1_surveytime"] = np.where(mask, 3, 0)
    return df

def GOEA_1_Present(df):
    df = GOEA_11_GOEA_114(df)
    present_cols = ['GOEA1_1', 'GOEA1_2', 'GOEA1_3', 'GOEA1_4', 'GOEA1_5', 'GOEA1_6', 'GOEA1_7', 'GOEA1_8']
    df['GOEA_1_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df
def GOEA_1_Flying(df):
    df = GOEA_11_GOEA_114(df)
    flying_cols = ['GOEA1_7', 'GOEA1_8', 'GOEA1_10', 'GOEA1_11', 'GOEA1_14']
    df['GOEA_1_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df
def GOEA_1_Eating(df):
    df['GOEA_1_Eating'] = np.where(df['GOEA_1.be'] == 'C', 3, 0)
    return df
def GOEA_1_Feeding_Young(df):
    df['GOEA_1_Feeding_Young'] = np.where(df['GOEA_1.be'] == 'M', 3, 0)
    return df
def GOEA_1_NestBuilding(df):
    df['GOEA_1_NestBuilding'] = np.where(df['GOEA_1.be'] == 'D', 3, 0)
    return df
def GOEA_1_Incubating(df):
    df['GOEA_1_Incubating'] = np.where(df['GOEA_1.be'] == 'L', 3, 0)
    return df
def GOEA_1_Brooding(df):
    df['GOEA_1_Brooding'] = np.where(df['GOEA_1.be'] == 'Q', 3, 0)
    return df
def GOEA_1_Aggressive_Interaction(df):
    df['GOEA_1_Aggressive_Interaction'] = np.where(df['GOEA_1.be'] == 'F', 3, 0)
    return df
def GOEA_1_Copulating(df):
    df['GOEA_1_Copulating'] = np.where(df['GOEA_1.be'] == 'H', 3, 0)
    return df   
def GOEA_1_In_Nest_Tree(df):
    df = GOEA_11_GOEA_114(df)
    df['GOEA_1_In_Nest_Tree'] = np.where((df['GOEA1_1'] == 3) | (df['GOEA1_2'] == 3), 3, 0)
    return df
def GOEA_1_Attending(df):
    df = GOEA_11_GOEA_114(df)
    #attending_cols = ['GOEA1_1', 'GOEA1_2', 'GOEA1_3', 'GOEA1_4']
    attending_cols = ['GOEA1_7', 'GOEA1_8', 'GOEA1_14']
    df['GOEA_1_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df

# =============================================================================
# GOLDEN EAGLE 2 (GOEA_2) FUNCTIONS
# =============================================================================
def GOEA_21_GOEA_214(df):
    for i in range(1, 15):
        df[f'GOEA2_{i}'] = np.where(df['GOEA_2.loc'] == i, 3, 0)
    return df

def GOEA_2_surveytime(df):
    mask = df["GOEA_2.loc"].notna() & (df["GOEA_2.loc"] != 12)
    df["GOEA_2_surveytime"] = np.where(mask, 3, 0)
    return df

def GOEA_2_Present(df):
    df = GOEA_21_GOEA_214(df)
    present_cols = ['GOEA2_1', 'GOEA2_2', 'GOEA2_3', 'GOEA2_4', 'GOEA2_5', 'GOEA2_6', 'GOEA2_7', 'GOEA2_8']
    df['GOEA_2_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df
def GOEA_2_Flying(df):
    df = GOEA_21_GOEA_214(df)
    flying_cols = ['GOEA2_7', 'GOEA2_8', 'GOEA2_10', 'GOEA2_11', 'GOEA2_14']
    df['GOEA_2_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df
def GOEA_2_Eating(df):
    df['GOEA_2_Eating'] = np.where(df['GOEA_2.be'] == 'C', 3, 0)
    return df
def GOEA_2_Feeding_Young(df):
    df['GOEA_2_Feeding_Young'] = np.where(df['GOEA_2.be'] == 'M', 3, 0)
    return df
def GOEA_2_NestBuilding(df):
    df['GOEA_2_NestBuilding'] = np.where(df['GOEA_2.be'] == 'D', 3, 0)
    return df
def GOEA_2_Incubating(df):
    df['GOEA_2_Incubating'] = np.where(df['GOEA_2.be'] == 'L', 3, 0)
    return df
def GOEA_2_Brooding(df):
    df['GOEA_2_Brooding'] = np.where(df['GOEA_2.be'] == 'Q', 3, 0)
    return df
def GOEA_2_Aggressive_Interaction(df):
    df['GOEA_2_Aggressive_Interaction'] = np.where(df['GOEA_2.be'] == 'F', 3, 0)
    return df
def GOEA_2_Copulating(df):
    df['GOEA_2_Copulating'] = np.where(df['GOEA_2.be'] == 'H', 3, 0)
    return df
def GOEA_2_In_Nest_Tree(df):
    df = GOEA_21_GOEA_214(df)
    df['GOEA_2_In_Nest_Tree'] = np.where((df['GOEA2_1'] == 3) | (df['GOEA2_2'] == 3), 3, 0)
    return df
def GOEA_2_Attending(df):
    df = GOEA_21_GOEA_214(df)
    #attending_cols = ['GOEA2_1', 'GOEA2_2', 'GOEA2_3', 'GOEA2_4']
    attending_cols = ['GOEA2_7', 'GOEA2_8', 'GOEA2_14']
    df['GOEA_2_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df
# =============================================================================
# GOLDEN EAGLE 3 (GOEA_3) FUNCTIONS
# =============================================================================
def GOEA_31_GOEA_314(df):
    for i in range(1, 15):
        df[f'GOEA3_{i}'] = np.where(df['GOEA_3.loc'] == i, 3, 0)
    return df
def GOEA_3_surveytime(df):
    mask = df["GOEA_3.loc"].notna() & (df["GOEA_3.loc"] != 12)
    df["GOEA_3_surveytime"] = np.where(mask, 3, 0)
    return df
def GOEA_3_Present(df):
    df = GOEA_31_GOEA_314(df)
    present_cols = ['GOEA3_1', 'GOEA3_2', 'GOEA3_3', 'GOEA3_4', 'GOEA3_5', 'GOEA3_6', 'GOEA3_7', 'GOEA3_8']
    df['GOEA_3_Present'] = np.where(df[present_cols].sum(axis=1) > 0, 3, 0)
    return df
def GOEA_3_Flying(df):
    df = GOEA_31_GOEA_314(df)
    flying_cols = ['GOEA3_7', 'GOEA3_8', 'GOEA3_10', 'GOEA3_11', 'GOEA3_14']
    df['GOEA_3_Flying'] = np.where(df[flying_cols].sum(axis=1) > 0, 3, 0)
    return df
def GOEA_3_Eating(df):
    df['GOEA_3_Eating'] = np.where(df['GOEA_3.be'] == 'C', 3, 0)
    return df
def GOEA_3_Feeding_Young(df):
    df['GOEA_3_Feeding_Young'] = np.where(df['GOEA_3.be'] == 'M', 3, 0)
    return df
def GOEA_3_NestBuilding(df):
    df['GOEA_3_NestBuilding'] = np.where(df['GOEA_3.be'] == 'D', 3, 0)
    return df
def GOEA_3_Incubating(df):
    df['GOEA_3_Incubating'] = np.where(df['GOEA_3.be'] == 'L', 3, 0)
    return df
def GOEA_3_Brooding(df):
    df['GOEA_3_Brooding'] = np.where(df['GOEA_3.be'] == 'Q', 3, 0)
    return df
def GOEA_3_Aggressive_Interaction(df):
    df['GOEA_3_Aggressive_Interaction'] = np.where(df['GOEA_3.be'] == 'F', 3, 0)
    return df
def GOEA_3_Copulating(df):
    df['GOEA_3_Copulating'] = np.where(df['GOEA_3.be'] == 'H', 3, 0)
    return df   
def GOEA_3_In_Nest_Tree(df):
    df = GOEA_31_GOEA_314(df)
    df['GOEA_3_In_Nest_Tree'] = np.where((df['GOEA3_1'] == 3) | (df['GOEA3_2'] == 3), 3, 0)
    return df
def GOEA_3_Attending(df):
    df = GOEA_31_GOEA_314(df)
    #attending_cols = ['GOEA3_1', 'GOEA3_2', 'GOEA3_3', 'GOEA3_4']
    attending_cols = ['GOEA3_7', 'GOEA3_8', 'GOEA3_14']
    df['GOEA_3_Attending'] = np.where(df[attending_cols].sum(axis=1) > 0, 3, 0)
    return df

# =============================================================================
# COMPOSITE PRESENCE FUNCTIONS
# =============================================================================
def at_least_one_adult_present(df):
    df = AF_Present(df)
    df = AM_Present(df)
    df = UA1_Present(df)
    df = UA2_Present(df)
    df["At_least_one_adult_present"] = np.where(
        (df['AF_Present'] == 3) | (df['AM_Present'] == 3) | 
        (df['UA1_Present'] == 3) | (df['UA2_Present'] == 3), 3, 0
    )
    return df

def At_least_one_adult_attending(df):
    df = AF_Attending(df)
    df = AM_Attending(df)
    df = UA1_Attending(df)
    df = UA2_Attending(df)
    df["At_least_one_adult_attending"] = np.where(
        (df['AF_Attending'] == 3) | (df['AM_Attending'] == 3) | 
        (df['UA1_Attending'] == 3) | (df['UA2_Attending'] == 3), 3, 0
    )
    return df

def At_least_one_JV_Present(df):
    df = JV1_Present(df)
    df = JV2_Present(df)
    df = JV3_Present(df)
    df["At_least_one_JV_present"] = np.where(
        (df['JV1_Present'] == 3) | (df['JV2_Present'] == 3) | (df['JV3_Present'] == 3), 3, 0
    )
    return df

def At_Least_One_FEHA_Present(df):
    df = FEHA1_Present(df)
    df = FEHA2_Present(df)
    df["FEHA_Present"] = np.where(
        (df['FEHA1_Present'] == 3) | (df['FEHA2_Present'] == 3), 3, 0
    )
    return df
def No_Adults_Present_but_JV1_Present(df):
    df = at_least_one_adult_present(df)
    df = JV1_Present(df)
    df["No_Adults_Present_but_JV1_Present"] = np.where(
        (df['At_least_one_adult_present'] == 0) & (df['JV1_Present'] == 3), 3, 0
    )
    return df
def No_Adults_Present_but_JV2_Present(df):
    df = at_least_one_adult_present(df)
    df = JV2_Present(df)
    df["No_Adults_Present_but_JV2_Present"] = np.where(
        (df['At_least_one_adult_present'] == 0) & (df['JV2_Present'] == 3), 3, 0
    )
    return df
def No_Adults_Present_but_JV3_Present(df):
    df = at_least_one_adult_present(df)
    df = JV3_Present(df)
    df["No_Adults_Present_but_JV3_Present"] = np.where(
        (df['At_least_one_adult_present'] == 0) & (df['JV3_Present'] == 3), 3, 0
    )
    return df

# =============================================================================
# COORDINATE FUNCTIONS
# =============================================================================
def nest_longitude(df):
    df["nest_longitude"] = PERCH_COORDS['Nest'][0]
    return df

def nest_latitude(df):
    df["nest_latitude"] = PERCH_COORDS['Nest'][1]
    return df

def p1_longitude(df):
    df["p1_longitude"] = df["p1"].map(
        lambda p: PERCH_COORDS.get(p, (np.nan, np.nan))[0] if pd.notna(p) else np.nan
    )
    return df

def p1_latitude(df):
    df["p1_latitude"] = df["p1"].map(
        lambda p: PERCH_COORDS.get(p, (np.nan, np.nan))[1] if pd.notna(p) else np.nan
    )
    return df

def p2_longitude(df):
    df["p2_longitude"] = df["p2"].map(
        lambda p: PERCH_COORDS.get(p, (np.nan, np.nan))[0] if pd.notna(p) else np.nan
    )
    return df

def p2_latitude(df):
    df["p2_latitude"] = df["p2"].map(
        lambda p: PERCH_COORDS.get(p, (np.nan, np.nan))[1] if pd.notna(p) else np.nan
    )
    return df

def p3_longitude(df):
    df["p3_longitude"] = df["p3"].map(
        lambda p: PERCH_COORDS.get(p, (np.nan, np.nan))[0] if pd.notna(p) else np.nan
    )
    return df

def p3_latitude(df):
    df["p3_latitude"] = df["p3"].map(
        lambda p: PERCH_COORDS.get(p, (np.nan, np.nan))[1] if pd.notna(p) else np.nan
    )
    return df

def p4_longitude(df):
    df["p4_longitude"] = df["p4"].map(
        lambda p: PERCH_COORDS.get(p, (np.nan, np.nan))[0] if pd.notna(p) else np.nan
    )
    return df

def p4_latitude(df):
    df["p4_latitude"] = df["p4"].map(
        lambda p: PERCH_COORDS.get(p, (np.nan, np.nan))[1] if pd.notna(p) else np.nan
    )
    return df

def p5_longitude(df):
    df["p5_longitude"] = df["p5"].map(
        lambda p: PERCH_COORDS.get(p, (np.nan, np.nan))[0] if pd.notna(p) else np.nan
    )
    return df

def p5_latitude(df):
    df["p5_latitude"] = df["p5"].map(
        lambda p: PERCH_COORDS.get(p, (np.nan, np.nan))[1] if pd.notna(p) else np.nan
    )
    return df

# =============================================================================
# DISTANCE CALCULATION FUNCTIONS
# =============================================================================
def p1_dist_from_nest(df):
    if 'p1_dist' in df.columns:
        df['p1_dist_from_nest'] = df['p1_dist']
        return df
    if 'nest_longitude' not in df.columns:
        df = nest_longitude(df)
    if 'nest_latitude' not in df.columns:
        df = nest_latitude(df)
    if 'p1_longitude' not in df.columns:
        df = p1_longitude(df)
    if 'p1_latitude' not in df.columns:
        df = p1_latitude(df)
    
    mask = df[["p1_latitude", "p1_longitude", "nest_latitude", "nest_longitude"]].notna().all(axis=1)
    df["p1_dist_from_nest"] = np.where(
        mask,
        great_circle_distance(
            df["p1_latitude"], df["p1_longitude"],
            df["nest_latitude"], df["nest_longitude"]
        ),
        np.nan
    )
    return df

def p2_dist_from_nest(df):
    if 'p2_dist' in df.columns:
        df['p2_dist_from_nest'] = df['p2_dist']
        return df
    if 'nest_longitude' not in df.columns:
        df = nest_longitude(df)
    if 'nest_latitude' not in df.columns:
        df = nest_latitude(df)
    if 'p2_longitude' not in df.columns:
        df = p2_longitude(df)
    if 'p2_latitude' not in df.columns:
        df = p2_latitude(df)
    
    mask = df[["p2_latitude", "p2_longitude", "nest_latitude", "nest_longitude"]].notna().all(axis=1)
    df["p2_dist_from_nest"] = np.where(
        mask,
        great_circle_distance(
            df["p2_latitude"], df["p2_longitude"],
            df["nest_latitude"], df["nest_longitude"]
        ),
        np.nan
    )
    return df

def p3_dist_from_nest(df):
    if 'p3_dist' in df.columns:
        df['p3_dist_from_nest'] = df['p3_dist']
        return df
    if 'nest_longitude' not in df.columns:
        df = nest_longitude(df)
    if 'nest_latitude' not in df.columns:
        df = nest_latitude(df)
    if 'p3_longitude' not in df.columns:
        df = p3_longitude(df)
    if 'p3_latitude' not in df.columns:
        df = p3_latitude(df)
    
    mask = df[["p3_latitude", "p3_longitude", "nest_latitude", "nest_longitude"]].notna().all(axis=1)
    df["p3_dist_from_nest"] = np.where(
        mask,
        great_circle_distance(
            df["p3_latitude"], df["p3_longitude"],
            df["nest_latitude"], df["nest_longitude"]
        ),
        np.nan
    )
    return df

def p4_dist_from_nest(df):
    if 'p4_dist' in df.columns:
        df['p4_dist_from_nest'] = df['p4_dist']
        return df
    if 'nest_longitude' not in df.columns:
        df = nest_longitude(df)
    if 'nest_latitude' not in df.columns:
        df = nest_latitude(df)
    if 'p4_longitude' not in df.columns:
        df = p4_longitude(df)
    if 'p4_latitude' not in df.columns:
        df = p4_latitude(df)
    
    mask = df[["p4_latitude", "p4_longitude", "nest_latitude", "nest_longitude"]].notna().all(axis=1)
    df["p4_dist_from_nest"] = np.where(
        mask,
        great_circle_distance(
            df["p4_latitude"], df["p4_longitude"],
            df["nest_latitude"], df["nest_longitude"]
        ),
        np.nan
    )
    return df

def p5_dist_from_nest(df):
    if 'p5_dist' in df.columns:
        df['p5_dist_from_nest'] = df['p5_dist']
        return df
    if 'nest_longitude' not in df.columns:
        df = nest_longitude(df)
    if 'nest_latitude' not in df.columns:
        df = nest_latitude(df)
    if 'p5_longitude' not in df.columns:
        df = p5_longitude(df)
    if 'p5_latitude' not in df.columns:
        df = p5_latitude(df)
    
    mask = df[["p5_latitude", "p5_longitude", "nest_latitude", "nest_longitude"]].notna().all(axis=1)
    df["p5_dist_from_nest"] = np.where(
        mask,
        great_circle_distance(
            df["p5_latitude"], df["p5_longitude"],
            df["nest_latitude"], df["nest_longitude"]
        ),
        np.nan
    )
    return df

# =============================================================================
# DISTANCE CODE FUNCTIONS
# =============================================================================
def p1_dist_from_nest_code(df):
    df = p1_dist_from_nest(df)
    
    df["p1_dist_from_nest_code"] = np.select(
        [
            df["p1_dist_from_nest"].notna() & (df["p1_dist_from_nest"] <= 1),
            df["p1_dist_from_nest"].between(2, 200, inclusive="right"),
            df["p1_dist_from_nest"].between(200, 400, inclusive="right"),
            df["p1_dist_from_nest"].between(400, 800, inclusive="right"),
            df["p1_dist_from_nest"].between(800, 1600, inclusive="right"),
            df["p1_dist_from_nest"] > 1600,
        ],
        [0, 3, 4, 5, 6, 7],
        default=9
    )
    return df

def p2_dist_from_nest_code(df):
    df = p2_dist_from_nest(df)
    df["p2_dist_from_nest_code"] = np.select(
        [
            df["p2_dist_from_nest"].notna() & (df["p2_dist_from_nest"] <= 1),
            df["p2_dist_from_nest"].between(2, 200, inclusive="right"),
            df["p2_dist_from_nest"].between(200, 400, inclusive="right"),
            df["p2_dist_from_nest"].between(400, 800, inclusive="right"),
            df["p2_dist_from_nest"].between(800, 1600, inclusive="right"),
            df["p2_dist_from_nest"] > 1600,
        ],
        [0, 3, 4, 5, 6, 7],
        default=9
    )
    return df

def p3_dist_from_nest_code(df):
    df = p3_dist_from_nest(df)
    df["p3_dist_from_nest_code"] = np.select(
        [
            df["p3_dist_from_nest"].notna() & (df["p3_dist_from_nest"] <= 1),
            df["p3_dist_from_nest"].between(2, 200, inclusive="right"),
            df["p3_dist_from_nest"].between(200, 400, inclusive="right"),
            df["p3_dist_from_nest"].between(400, 800, inclusive="right"),
            df["p3_dist_from_nest"].between(800, 1600, inclusive="right"),
            df["p3_dist_from_nest"] > 1600,
        ],
        [0, 3, 4, 5, 6, 7],
        default=9
    )
    return df

def p4_dist_from_nest_code(df):
    df = p4_dist_from_nest(df)
    df["p4_dist_from_nest_code"] = np.select(
        [
            df["p4_dist_from_nest"].notna() & (df["p4_dist_from_nest"] <= 1),
            df["p4_dist_from_nest"].between(2, 200, inclusive="right"),
            df["p4_dist_from_nest"].between(200, 400, inclusive="right"),
            df["p4_dist_from_nest"].between(400, 800, inclusive="right"),
            df["p4_dist_from_nest"].between(800, 1600, inclusive="right"),
            df["p4_dist_from_nest"] > 1600,
        ],
        [0, 3, 4, 5, 6, 7],
        default=9
    )
    return df

def p5_dist_from_nest_code(df):
    df = p5_dist_from_nest(df)
    df["p5_dist_from_nest_code"] = np.select(
        [
            df["p5_dist_from_nest"].notna() & (df["p5_dist_from_nest"] <= 1),
            df["p5_dist_from_nest"].between(2, 200, inclusive="right"),
            df["p5_dist_from_nest"].between(200, 400, inclusive="right"),
            df["p5_dist_from_nest"].between(400, 800, inclusive="right"),
            df["p5_dist_from_nest"].between(800, 1600, inclusive="right"),
            df["p5_dist_from_nest"] > 1600,
        ],
        [0, 3, 4, 5, 6, 7],
        default=9
    )
    return df

def min_adult_dist_from_nest_code(df):
    df = p1_dist_from_nest_code(df)
    df = p2_dist_from_nest_code(df)
    
    valid_codes = [0, 3, 4, 5, 6, 7]
    df["min_adult_dist_from_nest_code"] = np.where(
        df[["p1_dist_from_nest_code", "p2_dist_from_nest_code"]].isin(valid_codes).any(axis=1),
        df[["p1_dist_from_nest_code", "p2_dist_from_nest_code"]].min(axis=1),
        9
    )
    return df

# =============================================================================
# JUVENILE-ADULT DISTANCE FUNCTIONS
# =============================================================================
def jv1_dist_from_af_ua1_code(df):

    if 'p1_longitude' not in df.columns:
        df = p1_longitude(df)
    if 'p1_latitude' not in df.columns:
        df = p1_latitude(df)
    if 'p3_longitude' not in df.columns:
        df = p3_longitude(df)
    if 'p3_latitude' not in df.columns:
        df = p3_latitude(df)
    
    mask = df[["p1_latitude", "p1_longitude", "p3_latitude", "p3_longitude"]].notna().all(axis=1)
    df["jv1_dist_from_af_ua1_code"] = np.where(
        mask,
        great_circle_distance(
            df["p1_latitude"], df["p1_longitude"],
            df["p3_latitude"], df["p3_longitude"]
        ),
        np.nan
    )
    return df

def jv1_dist_from_am_ua2_code(df):
    if 'p2_longitude' not in df.columns:
        df = p2_longitude(df)
    if 'p2_latitude' not in df.columns:
        df = p2_latitude(df)
    if 'p3_longitude' not in df.columns:
        df = p3_longitude(df)
    if 'p3_latitude' not in df.columns:
        df = p3_latitude(df)
    
    mask = df[["p2_latitude", "p2_longitude", "p3_latitude", "p3_longitude"]].notna().all(axis=1)
    df["jv1_dist_from_am_ua2_code"] = np.where(
        mask,
        great_circle_distance(
            df["p2_latitude"], df["p2_longitude"],
            df["p3_latitude"], df["p3_longitude"]
        ),
        np.nan
    )
    return df

def jv2_dist_from_af_ua1_code(df):
    if 'p1_longitude' not in df.columns:
        df = p1_longitude(df)
    if 'p1_latitude' not in df.columns:
        df = p1_latitude(df)
    if 'p4_longitude' not in df.columns:
        df = p4_longitude(df)
    if 'p4_latitude' not in df.columns:
        df = p4_latitude(df)
    
    mask = df[["p1_latitude", "p1_longitude", "p4_latitude", "p4_longitude"]].notna().all(axis=1)
    df["jv2_dist_from_af_ua1_code"] = np.where(
        mask,
        great_circle_distance(
            df["p1_latitude"], df["p1_longitude"],
            df["p4_latitude"], df["p4_longitude"]
        ),
        np.nan
    )
    return df

def jv2_dist_from_am_ua2_code(df):
    if 'p2_longitude' not in df.columns:
        df = p2_longitude(df)
    if 'p2_latitude' not in df.columns:
        df = p2_latitude(df)
    if 'p4_longitude' not in df.columns:
        df = p4_longitude(df)
    if 'p4_latitude' not in df.columns:
        df = p4_latitude(df)
    
    mask = df[["p2_latitude", "p2_longitude", "p4_latitude", "p4_longitude"]].notna().all(axis=1)
    df["jv2_dist_from_am_ua2_code"] = np.where(
        mask,
        great_circle_distance(
            df["p2_latitude"], df["p2_longitude"],
            df["p4_latitude"], df["p4_longitude"]
        ),
        np.nan
    )
    return df

def jv3_dist_from_af_ua1_code(df):
    if 'p1_longitude' not in df.columns:
        df = p1_longitude(df)
    if 'p1_latitude' not in df.columns:
        df = p1_latitude(df)
    if 'p5_longitude' not in df.columns:
        df = p5_longitude(df)
    if 'p5_latitude' not in df.columns:
        df = p5_latitude(df)
    
    mask = df[["p1_latitude", "p1_longitude", "p5_latitude", "p5_longitude"]].notna().all(axis=1)
    df["jv3_dist_from_af_ua1_code"] = np.where(
        mask,
        great_circle_distance(
            df["p1_latitude"], df["p1_longitude"],
            df["p5_latitude"], df["p5_longitude"]
        ),
        np.nan
    )
    return df

def jv3_dist_from_am_ua2_code(df):
    if 'p2_longitude' not in df.columns:
        df = p2_longitude(df)
    if 'p2_latitude' not in df.columns:
        df = p2_latitude(df)
    if 'p5_longitude' not in df.columns:
        df = p5_longitude(df)
    if 'p5_latitude' not in df.columns:
        df = p5_latitude(df)
    
    mask = df[["p2_latitude", "p2_longitude", "p5_latitude", "p5_longitude"]].notna().all(axis=1)
    df["jv3_dist_from_am_ua2_code"] = np.where(
        mask,
        great_circle_distance(
            df["p2_latitude"], df["p2_longitude"],
            df["p5_latitude"], df["p5_longitude"]
        ),
        np.nan
    )
    return df

def jv1_dist_from_nearest_adult_code(df):
    df = jv1_dist_from_af_ua1_code(df)
    df = jv1_dist_from_am_ua2_code(df)
    
    df["jv1_dist_from_nearest_adult_code"] = np.select(
        [
            df[["jv1_dist_from_af_ua1_code", "jv1_dist_from_am_ua2_code"]].notna().all(axis=1),
            df["jv1_dist_from_af_ua1_code"].notna(),
            df["jv1_dist_from_am_ua2_code"].notna(),
        ],
        [
            df[["jv1_dist_from_af_ua1_code", "jv1_dist_from_am_ua2_code"]].min(axis=1),
            df["jv1_dist_from_af_ua1_code"],
            df["jv1_dist_from_am_ua2_code"],
        ],
        default=9
    )
    return df

def jv2_dist_from_nearest_adult_code(df):
    df = jv2_dist_from_af_ua1_code(df)
    df = jv2_dist_from_am_ua2_code(df)
    
    df["jv2_dist_from_nearest_adult_code"] = np.select(
        [
            df[["jv2_dist_from_af_ua1_code", "jv2_dist_from_am_ua2_code"]].notna().all(axis=1),
            df["jv2_dist_from_af_ua1_code"].notna(),
            df["jv2_dist_from_am_ua2_code"].notna(),
        ],
        [
            df[["jv2_dist_from_af_ua1_code", "jv2_dist_from_am_ua2_code"]].min(axis=1),
            df["jv2_dist_from_af_ua1_code"],
            df["jv2_dist_from_am_ua2_code"],
        ],
        default=9
    )
    return df

def jv3_dist_from_nearest_adult_code(df):
    df = jv3_dist_from_af_ua1_code(df)
    df = jv3_dist_from_am_ua2_code(df)
    
    df["jv3_dist_from_nearest_adult_code"] = np.select(
        [
            df[["jv3_dist_from_af_ua1_code", "jv3_dist_from_am_ua2_code"]].notna().all(axis=1),
            df["jv3_dist_from_af_ua1_code"].notna(),
            df["jv3_dist_from_am_ua2_code"].notna(),
        ],
        [
            df[["jv3_dist_from_af_ua1_code", "jv3_dist_from_am_ua2_code"]].min(axis=1),
            df["jv3_dist_from_af_ua1_code"],
            df["jv3_dist_from_am_ua2_code"],
        ],
        default=9
    )
    return df

# =============================================================================
# DISTANCE BIN INDICATOR FUNCTIONS
# =============================================================================
def min_adult_0_200m(df):
    df = min_adult_dist_from_nest_code(df)
    df["min_adult_0_200m"] = np.where(df["min_adult_dist_from_nest_code"].isin([0, 3]), 3, 0)
    return df

def min_adult_200_400m(df):
    df = min_adult_dist_from_nest_code(df)
    df["min_adult_200_400m"] = np.where(df["min_adult_dist_from_nest_code"] == 4, 3, 0)
    return df

def min_adult_400_800m(df):
    df = min_adult_dist_from_nest_code(df)
    df["min_adult_400_800m"] = np.where(df["min_adult_dist_from_nest_code"] == 5, 3, 0)
    return df

def min_adult_800_1600m(df):
    df = min_adult_dist_from_nest_code(df)
    df["min_adult_800_1600m"] = np.where(df["min_adult_dist_from_nest_code"] == 6, 3, 0)
    return df
def min_adult_over_1600m(df):
    df = min_adult_dist_from_nest_code(df)
    df["min_adult_over_1600m"] = np.where(df["min_adult_dist_from_nest_code"] == 7, 3, 0)
    return df


# =============================================================================
# PRESENCE LOGIC FUNCTIONS
# =============================================================================
def no_adult_present(df):
    df["no_adult_present"] = np.where(
        ((df['female.loc'] == 9) & (df['male.loc'] == 9)) | 
        ((df['undiff1.loc'] == 9) & (df['undiff2.loc'] == 9)), 
        3, 0
    )
    return df

def no_adult_present_jv1_present(df):
    df = no_adult_present(df)
    df = JV1_Present(df)
    df["no_adult_present_jv1_present"] = np.where(
        (df['no_adult_present'] == 3) & (df['JV1_Present'] == 3), 3, 0
    )
    return df

def no_adult_present_jv2_present(df):
    df = no_adult_present(df)
    df = JV2_Present(df)
    df["no_adult_present_jv2_present"] = np.where(
        (df['no_adult_present'] == 3) & (df['JV2_Present'] == 3), 3, 0
    )
    return df

def no_adult_present_jv3_present(df):
    df = no_adult_present(df)
    df = JV3_Present(df)
    df["no_adult_present_jv3_present"] = np.where(
        (df['no_adult_present'] == 3) & (df['JV3_Present'] == 3), 3, 0
    )
    return df

def only_one_adult_present(df):
    df["only_one_adult_present"] = np.where(
        ((df['female.loc'].isin([0,3])) & (df['male.loc'] == 9)) | 
        ((df['male.loc'].isin([0,3])) & (df['female.loc'] == 9)) | 
        ((df['undiff1.loc'].isin([0,3])) & (df['undiff2.loc'] == 9)) | 
        ((df['undiff2.loc'].isin([0,3])) & (df['undiff1.loc'] == 9)), 
        3, 0
    )
    return df

def two_adults_attending(df):
    df["two_adults_attending"] = np.where(
        ((df['female.loc'].isin([0,3])) & (df['male.loc'].isin([0,3]))) | 
        ((df['undiff1.loc'].isin([0,3])) & (df['undiff2.loc'].isin([0,3]))), 
        3, 0
    )
    return df
def BE1_Present(df):
    df = df.copy()
    df["BE1_Present"] = np.where(df["OBE.loc"].isin([0,3]), 3, 0)
    return df
def BE2_Present(df):
    df = df.copy()
    df["BE2_Present"] = np.where(df["OBE.loc"].isin([0,3]), 3, 0)
    return df
def At_least_one_territorial_BE_present(df):
    df = BE1_Present(df)
    df = BE2_Present(df)
    df["At_least_one_territorial_BE_present"] = np.where(
        (df['BE1_Present'] == 3) | (df['BE2_Present'] == 3), 3, 0
    )
    return df

def two_adults_present(df):
    df = AF_Present(df)
    df = AM_Present(df)
    df = UA1_Present(df)
    df = UA2_Present(df)
    
    total_present = (
        (df['AF_Present'] == 3).astype(int) + 
        (df['AM_Present'] == 3).astype(int) + 
        (df['UA1_Present'] == 3).astype(int) + 
        (df['UA2_Present'] == 3).astype(int)
    )
    df["two_adults_present"] = np.where(total_present >= 2, 3, 0)
    return df

# ============================================================================
# Distance from nest bins for juveniles
# ============================================================================

def af_ua1_0m(df): 
    df = df.copy()
    df["af_ua1_0m"] = np.where(df["p1_dist_from_nest_code"] == 0, 3, 0)
    return df
def am_ua2_0m(df): 
    df = df.copy()
    df["am_ua2_0m"] = np.where(df["p2_dist_from_nest_code"] == 0, 3, 0)
    return df
def jv1_0m(df): 
    df = df.copy()
    df["jv1_0m"] = np.where(df["p3_dist_from_nest_code"] == 0, 3, 0)
    return df
def jv2_0m(df): 
    df = df.copy()
    df["jv2_0m"] = np.where(df["p4_dist_from_nest_code"] == 0, 3, 0)
    return df
def jv3_0m(df): 
    df = df.copy()
    df["jv3_0m"] = np.where(df["p5_dist_from_nest_code"] == 0, 3, 0)
    return df
def min_adult_0m(df): 
    df = df.copy()
    df["min_adult_0m"] = np.where((df["af_ua1_0m"] == 3) | (df["am_ua2_0m"] == 3), 3, 0)
    return df

def af_ua1_0_200m(df): 
    df = df.copy()
    df["af_ua1_0_200m"] = np.where(df["p1_dist_from_nest_code"].isin([0,3]), 3, 0)
    return df
def am_ua2_0_200m(df): 
    df = df.copy()
    df["am_ua2_0_200m"] = np.where(df["p2_dist_from_nest_code"].isin([0,3]), 3, 0)
    return df
def jv1_0_200m(df): 
    df = df.copy()
    df["jv1_0_200m"] = np.where(df["p3_dist_from_nest_code"].isin([0,3]), 3, 0)
    return df
def jv2_0_200m(df): 
    df = df.copy()
    df["jv2_0_200m"] = np.where(df["p4_dist_from_nest_code"].isin([0,3]), 3, 0)
    return df
def jv3_0_200m(df): 
    df = df.copy()
    df["jv3_0_200m"] = np.where(df["p5_dist_from_nest_code"].isin([0,3]), 3, 0)
    return df
def min_adult_0_200m(df): 
    df = df.copy()
    df["min_adult_0_200m"] = np.where((df["af_ua1_0_200m"] == 3) | (df["am_ua2_0_200m"] == 3), 3, 0)
    return df


def af_ua1_200_400m(df): 
    df = df.copy()
    df["af_ua1_200_400m"] = np.where(df["p1_dist_from_nest_code"] == 5, 3, 0)
    return df
def am_ua2_200_400m(df): 
    df = df.copy()
    df["am_ua2_200_400m"] = np.where(df["p2_dist_from_nest_code"] == 5, 3, 0)
    return df
def jv1_200_400m(df): 
    df = df.copy()
    df["jv1_200_400m"] = np.where(df["p3_dist_from_nest_code"] == 5, 3, 0)
    return df
def jv2_200_400m(df): 
    df = df.copy()
    df["jv2_200_400m"] = np.where(df["p4_dist_from_nest_code"] == 5, 3, 0)
    return df
def jv3_200_400m(df): 
    df = df.copy()
    df["jv3_200_400m"] = np.where(df["p5_dist_from_nest_code"] == 5, 3, 0)
    return df
def min_adult_200_400m(df): 
    df = df.copy()

    df["min_adult_200_400m"] = np.where(
        (df["min_adult_dist_from_nest_code"] == 3),
        3,
        0
    )

    return df


def af_ua1_400_800m(df): 
    df = df.copy()
    df["af_ua1_400_800m"] = np.where(df["p1_dist_from_nest_code"] == 6, 3, 0)
    return df
def am_ua2_400_800m(df): 
    df = df.copy()
    df["am_ua2_400_800m"] = np.where(df["p2_dist_from_nest_code"] == 6, 3, 0)
    return df
def jv1_400_800m(df): 
    df = df.copy()
    df["jv1_400_800m"] = np.where(df["p3_dist_from_nest_code"] == 6, 3, 0)
    return df
def jv2_400_800m(df): 
    df = df.copy()
    df["jv2_400_800m"] = np.where(df["p4_dist_from_nest_code"] == 6, 3, 0)
    return df
def jv3_400_800m(df): 
    df = df.copy()
    df["jv3_400_800m"] = np.where(df["p5_dist_from_nest_code"] == 6, 3, 0)
    return df
def min_adult_400_800m(df): 
    df = df.copy()
    df["min_adult_400_800m"] = np.where(
        (df["min_adult_dist_from_nest_code"] == 5),
        3,
        0
    )
    return df

def af_ua1_800_1600m(df): 
    df = df.copy()
    df["af_ua1_800_1600m"] = np.where(df["p1_dist_from_nest_code"] == 6, 3, 0)
    return df
def am_ua2_800_1600m(df): 
    df = df.copy()
    df["am_ua2_800_1600m"] = np.where(df["p2_dist_from_nest_code"] == 6, 3, 0)
    return df
def jv1_800_1600m(df): 
    df = df.copy()
    df["jv1_800_1600m"] = np.where(df["p3_dist_from_nest_code"] == 6, 3, 0)
    return df
def jv2_800_1600m(df): 
    df = df.copy()
    df["jv2_800_1600m"] = np.where(df["p4_dist_from_nest_code"] == 6, 3, 0)
    return df
def jv3_800_1600m(df): 
    df = df.copy()
    df["jv3_800_1600m"] = np.where(df["p5_dist_from_nest_code"] == 6, 3, 0)
    return df
def min_adult_800_1600m(df): 
    df = df.copy()
    df["min_adult_800_1600m"] = np.where(
        (df["min_adult_dist_from_nest_code"] == 6),
        3,
        0
    )
    return df
def af_ua1_over_1600m(df): 
    df = df.copy()
    df["af_ua1_over_1600m"] = np.where(df["p1_dist_from_nest_code"] == 7, 3, 0)
    return df
def am_ua2_over_1600m(df): 
    df = df.copy()
    df["am_ua2_over_1600m"] = np.where(df["p2_dist_from_nest_code"] == 7, 3, 0)
    return df

def jv1_nearest_adult_0m(df): 
    df = df.copy()
    df["jv1_nearest_adult_0m"] = np.where(df["jv1_dist_from_nearest_adult_code"] == 0, 3, 0)
    return df
def jv2_nearest_adult_0m(df): 
    df = df.copy()
    df["jv2_nearest_adult_0m"] = np.where(df["jv2_dist_from_nearest_adult_code"] == 0, 3, 0)
    return df
def jv3_nearest_adult_0m(df): 
    df = df.copy()
    df["jv3_nearest_adult_0m"] = np.where(df["jv3_dist_from_nearest_adult_code"] == 0, 3, 0)
    return df

def jv1_nearest_adult_0_200m(df): 
    df = df.copy()
    df["jv1_nearest_adult_0_200m"] = np.where(df["jv1_dist_from_nearest_adult_code"] == 3, 3, 0)
    return df
def jv2_nearest_adult_0_200m(df): 
    df = df.copy()
    df["jv2_nearest_adult_0_200m"] = np.where(df["jv2_dist_from_nearest_adult_code"] == 3, 3, 0)
    return df
def jv3_nearest_adult_0_200m(df): 
    df = df.copy()
    df["jv3_nearest_adult_0_200m"] = np.where(df["jv3_dist_from_nearest_adult_code"] == 3, 3, 0)
    return df

def jv1_nearest_adult_200_400m(df): 
    df = df.copy()
    df["jv1_nearest_adult_200_400m"] = np.where(df["jv1_dist_from_nearest_adult_code"] == 4, 3, 0)
    return df
def jv2_nearest_adult_200_400m(df): 
    df = df.copy()
    df["jv2_nearest_adult_200_400m"] = np.where(df["jv2_dist_from_nearest_adult_code"] == 4, 3, 0)
    return df
def jv3_nearest_adult_200_400m(df): 
    df = df.copy()
    df["jv3_nearest_adult_200_400m"] = np.where(df["jv3_dist_from_nearest_adult_code"] == 4, 3, 0)
    return df

def jv1_nearest_adult_400_800m(df): 
    df = df.copy()
    df["jv1_nearest_adult_400_800m"] = np.where(df["jv1_dist_from_nearest_adult_code"] == 5, 3, 0)
    return df
def jv2_nearest_adult_400_800m(df): 
    df = df.copy()
    df["jv2_nearest_adult_400_800m"] = np.where(df["jv2_dist_from_nearest_adult_code"] == 5, 3, 0)
    return df
def jv3_nearest_adult_400_800m(df): 
    df = df.copy()
    df["jv3_nearest_adult_400_800m"] = np.where(df["jv3_dist_from_nearest_adult_code"] == 5, 3, 0)
    return df

def jv1_nearest_adult_800_1600m(df): 
    df = df.copy()
    df["jv1_nearest_adult_800_1600m"] = np.where(df["jv1_dist_from_nearest_adult_code"] == 6, 3, 0)
    return df
def jv2_nearest_adult_800_1600m(df): 
    df = df.copy()
    df["jv2_nearest_adult_800_1600m"] = np.where(df["jv2_dist_from_nearest_adult_code"] == 6, 3, 0)
    return df
def jv3_nearest_adult_800_1600m(df): 
    df = df.copy()
    df["jv3_nearest_adult_800_1600m"] = np.where(df["jv3_dist_from_nearest_adult_code"] == 6, 3, 0)
    return df

def jv1_nearest_adult_gt_1600m(df): 
    df = df.copy()
    df["jv1_nearest_adult_gt_1600m"] = np.where(df["jv1_dist_from_nearest_adult_code"] == 7, 3, 0)
    return df   
def jv2_nearest_adult_gt_1600m(df): 
    df = df.copy()
    df["jv2_nearest_adult_gt_1600m"] = np.where(df["jv2_dist_from_nearest_adult_code"] == 7, 3, 0)
    return df
def jv3_nearest_adult_gt_1600m(df): 
    df = df.copy()
    df["jv3_nearest_adult_gt_1600m"] = np.where(df["jv3_dist_from_nearest_adult_code"] == 7, 3, 0)
    return df

def no_adult_present(df): 
    df = df.copy()

    df["no_adult_present"] = np.where(
        (
            (df['female.loc'] == 9) & (df['male.loc'] == 9)
        ) | (
            (df['undiff1.loc'] == 9) & (df['undiff2.loc'] == 9)
        ),
        3,
        0
    )

    return df
def at_least_one_adult_in_nest_tree(df): 
    df= df.copy()
    df["at_least_one_adult_in_nest_tree"] = np.where( ((df['female.loc'].isin([0,3])) | (df['male.loc'].isin([0,3])) | (df['undiff1.loc'].isin([0,3])) | (df['undiff2.loc'].isin([0,3]))), 3,0)
    return df

def no_adult_present_jv1_present(df):
    df= df.copy()
    df["no_adult_present_jv1_present"] = np.where( df['no_adult_present'] ==3 & (df['JV1_Present'] ==3), 3,0)
    return df
def no_adult_present_jv2_present(df): 
    df= df.copy()
    df["no_adult_present_jv2_present"] = np.where( df['no_adult_present'] ==3 & (df['JV2_Present'] ==3), 3,0)
    return df
def no_adult_present_jv3_present(df): 
    df= df.copy()
    df["no_adult_present_jv3_present"] = np.where( df['no_adult_present'] ==3 & (df['JV3_Present'] ==3), 3,0)
    return df
def only_one_adult_present(df): 
    df = df.copy()
    df["only_one_adult_present"] = np.where( ((df['female.loc'].isin([0,3]) & df['male.loc'] ==9) | (df['male.loc'].isin([0,3]) & df['female.loc'] ==9) | ((df['undiff1.loc'].isin([0,3]) & df['undiff2.loc'] ==9) | (df['undiff2.loc'].isin([0,3]) & df['undiff1.loc'] ==9))), 3, 0)
    return df
def two_adults_attending(df):
    df = df.copy()
    df["two_adults_attending"] = np.where( ((df['female.loc'].isin([0,3]) & df['male.loc'].isin([0,3])) | (df['undiff1.loc'].isin([0,3]) & df['undiff2.loc'].isin([0,3]))), 3, 0)
    return df
def two_adults_present(df): 
    df = df.copy()
    df["two_adults_present"] = np.where( df['AF_Present'] +  df['AM_Present'] + df['UA1_Present'] + df['UA2_Present'] >=6, 3, 0)
    return df

# =============================================================================
# DATA LOADING AND PIPELINE
# =============================================================================
def load_data(file_path):
    """Load data from a CSV file into a pandas DataFrame."""
    try:
        try:
            data = pd.read_csv(file_path, low_memory=False)
        except UnicodeDecodeError:
            data = pd.read_csv(file_path, encoding="latin1", low_memory=False)
        print(f"Successfully loaded {len(data)} rows from {file_path}")
        return data
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
        return None
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
        return None
    except Exception as e:
        print(f"Error loading file: {e}")
        return None

def run_pipeline(df):
    """Run the complete analysis pipeline."""
    print("Running pipeline...")
    
    # Coordinates
    df = nest_longitude(df)
    df = nest_latitude(df)
    df = p1_longitude(df)
    df = p1_latitude(df)
    df = p2_longitude(df)
    df = p2_latitude(df)
    df = p3_longitude(df)
    df = p3_latitude(df)
    df = p4_longitude(df)
    df = p4_latitude(df)
    df = p5_longitude(df)
    df = p5_latitude(df)
    
    # Distances from nest
    df = p1_dist_from_nest_code(df)
    df = p2_dist_from_nest_code(df)
    df = p3_dist_from_nest_code(df)
    df = p4_dist_from_nest_code(df)
    df = p5_dist_from_nest_code(df)
    df = min_adult_dist_from_nest_code(df)
    
    # Adult presence
    df = AF_Present(df)
    df = AM_Present(df)
    df = UA1_Present(df)
    df = UA2_Present(df)
    df = at_least_one_adult_present(df)
    
    # Juvenile presence
    df = JV1_Present(df)
    df = JV2_Present(df)
    df = JV3_Present(df)
    df = At_least_one_JV_Present(df)
    
    # Juvenile-adult distances
    df = jv1_dist_from_nearest_adult_code(df)
    df = jv2_dist_from_nearest_adult_code(df)
    df = jv3_dist_from_nearest_adult_code(df)

    # AF 
    df = AF_In_Nest_Tree(df)
    df = AF_Attending(df)
    df = AF_three_and_four(df)  
    df = AF_Flying(df)
    df = AF_Eating(df)
    df = AF_Feeding_Young(df)
    df = AF_NestBuilding(df)
    df = AF_Incubating(df)
    df = AF_Brooding(df)
    df = AF_Aggressive_Interaction(df)
    df = AF_Copulating(df)
    df = af_surveytime(df)

    # AM
    df = AM_In_Nest_Tree(df)
    df = AM_Attending(df)
    df = AM_three_and_four(df)  
    df = AM_Flying(df)
    df = AM_Eating(df)
    df = AM_Feeding_Young(df)
    df = AM_NestBuilding(df)
    df = AM_Incubating(df)
    df = AM_Brooding(df)
    df = AM_Aggressive_Interaction(df)
    df = AM_Copulating(df)
    df = am_surveytime(df)

    # UA1
    df = UA1_In_Nest_Tree(df)
    df = UA1_Attending(df)
    df = UA1_three_and_four(df)  
    df = UA1_Flying(df)     
    df = UA1_Eating(df)
    df = UA1_Feeding_Young(df)
    df = UA1_NestBuilding(df)
    df = UA1_Incubating(df)
    df = UA1_Brooding(df)
    df = UA1_Aggressive_Interaction(df)
    df = UA1_Copulating(df)
    df = ua1_surveytime(df) 

    # UA2
    df = UA2_In_Nest_Tree(df)
    df = UA2_Attending(df)
    df = UA2_three_and_four(df)  
    df = UA2_Flying(df)     
    df = UA2_Eating(df)
    df = UA2_Feeding_Young(df)
    df = UA2_NestBuilding(df)
    df = UA2_Incubating(df)
    df = UA2_Brooding(df)
    df = UA2_Aggressive_Interaction(df)
    df = UA2_Copulating(df)
    df = ua2_surveytime(df) 

    # JV1
    df = JV1_In_Nest_Tree(df)
    df = JV1_Attending(df)
    df = jv1_surveytime(df)
    df = JV1_Aggressive_Interaction(df)
    df = JV1_Flying(df)
    df = JV1_Eating(df)
    df = JV1_Feeding_Young(df)
    df = JV1_NestBuilding(df)
    df = JV1_Copulating(df)
    df = JV1_three_and_four(df)
    df = JV1_Present(df)
    df = JV1_Brooding(df)
    df = JV1_Incubating(df)
    df = jv1_dist_from_nearest_adult_code(df)
    df = jv1_dist_from_af_ua1_code(df)
    df = jv1_dist_from_am_ua2_code(df)
    df = No_Adults_Present_but_JV1_Present(df)
   



    # JV2
    df = JV2_In_Nest_Tree(df)
    df = JV2_Attending(df)
    df = jv2_surveytime(df) 
    df = JV2_Aggressive_Interaction(df)
    df = JV2_Flying(df)
    df = JV2_Eating(df)
    df = JV2_Feeding_Young(df)
    df = JV2_NestBuilding(df)
    df = JV2_Copulating(df)
    df = JV2_three_and_four(df)
    df = JV2_Present(df)
    df = JV2_Brooding(df)
    df = JV2_Incubating(df)
    df = JV2_In_Nest_Tree(df)
    df = jv2_dist_from_nearest_adult_code(df)
    df = jv2_dist_from_af_ua1_code(df)
    df = jv2_dist_from_am_ua2_code(df)
    df = No_Adults_Present_but_JV2_Present(df)

    # JV3
    df = JV3_In_Nest_Tree(df)
    df = JV3_Attending(df)
    df = jv3_surveytime(df)
    df = JV3_Aggressive_Interaction(df)
    df = JV3_Flying(df)
    df = JV3_Eating(df)
    df = JV3_Feeding_Young(df)
    df = JV3_NestBuilding(df)
    df = JV3_Copulating(df)
    df = JV3_three_and_four(df)
    df = JV3_Present(df)
    df = JV3_Brooding(df)
    df = JV3_Incubating(df)
    df = JV3_In_Nest_Tree(df)
    df = jv3_dist_from_nearest_adult_code(df)
    df = jv3_dist_from_af_ua1_code(df)
    df = jv3_dist_from_am_ua2_code(df)
    df = No_Adults_Present_but_JV3_Present(df)

    #FEHA1
    df = FEHA1_Present(df)
    df = FEHA1_surveytime(df)
    df = FEHA1_Aggressive_Interaction(df)
    df = FEHA1_Flying(df)
    df = FEHA1_Eating(df)
    df = FEHA1_Feeding_Young(df)
    df = FEHA1_NestBuilding(df)
    df = FEHA1_Copulating(df)
    df = FEHA1_Brooding(df)
    df = FEHA1_Incubating(df)
    df = FEHA1_In_Nest_Tree(df)
    df = FEHA1_Attending(df)



    #FEHA2
    df = FEHA2_Present(df)
    df = FEHA2_surveytime(df)
    df = FEHA2_Aggressive_Interaction(df)
    df = FEHA2_Flying(df)
    df = FEHA2_Eating(df)
    df = FEHA2_Feeding_Young(df)
    df = FEHA2_NestBuilding(df)
    df = FEHA2_Copulating(df)
    df = FEHA2_Brooding(df)
    df = FEHA2_Incubating(df)
    df = FEHA2_In_Nest_Tree(df)
    df = FEHA2_Attending(df)

    df = At_Least_One_FEHA_Present(df)

    #OBE
    df = OBE_Present(df)
    df = OBE_surveytime(df)
    df = OBE_Aggressive_Interaction(df)
    df = OBE_Flying(df)
    df = OBE_Eating(df)
    df = OBE_Feeding_Young(df)
    df = OBE_NestBuilding(df)
    df = OBE_Copulating(df)
    df = OBE_Brooding(df)
    df = OBE_Incubating(df)
    df = OBE_In_Nest_Tree(df)
    df = OBE_Attending(df)

    


    # GOEA1
    df = GOEA_1_Present(df)
    df = GOEA_1_surveytime(df)
    df = GOEA_1_Aggressive_Interaction(df)
    df = GOEA_1_Flying(df)
    df = GOEA_1_Eating(df)
    df = GOEA_1_Feeding_Young(df)
    df = GOEA_1_NestBuilding(df)
    df = GOEA_1_Copulating(df)
    df = GOEA_1_Brooding(df)
    df = GOEA_1_Incubating(df)
    df = GOEA_1_In_Nest_Tree(df)
    df = GOEA_1_Attending(df)

    # GOEA2
    df = GOEA_2_Present(df)
    df = GOEA_2_surveytime(df)
    df = GOEA_2_Aggressive_Interaction(df)
    df = GOEA_2_Flying(df)
    df = GOEA_2_Eating(df)
    df = GOEA_2_Feeding_Young(df)
    df = GOEA_2_NestBuilding(df)
    df = GOEA_2_Copulating(df)
    df = GOEA_2_Brooding(df)
    df = GOEA_2_Incubating(df)
    df = GOEA_2_In_Nest_Tree(df)
    df = GOEA_2_Attending(df)

    #GOEA3
    df = GOEA_3_Present(df)
    df = GOEA_3_surveytime(df)
    df = GOEA_3_Aggressive_Interaction(df)
    df = GOEA_3_Flying(df)
    df = GOEA_3_Eating(df)
    df = GOEA_3_Feeding_Young(df)
    df = GOEA_3_NestBuilding(df)
    df = GOEA_3_Copulating(df)
    df = GOEA_3_Brooding(df)
    df = GOEA_3_Incubating(df)
    df = GOEA_3_In_Nest_Tree(df)
    df = GOEA_3_Attending(df)

    # at least one JV present
    df = At_least_one_JV_Present(df)

    
    
    # Distance bins
    

    df = af_ua1_0m(df)
    df = am_ua2_0m(df)
    df = jv1_0m(df)
    df = jv2_0m(df)
    df = jv3_0m(df)
    df = af_ua1_0_200m(df)
    df = am_ua2_0_200m(df) 
    df = jv1_0_200m(df)
    df = jv2_0_200m(df)
    df = jv3_0_200m(df)
    df = af_ua1_200_400m(df)
    df = am_ua2_200_400m(df)
    df = jv1_200_400m(df)
    df = jv2_200_400m(df)
    df = jv3_200_400m(df)
    df = af_ua1_400_800m(df)
    df = am_ua2_400_800m(df)
    df = jv1_400_800m(df)
    df = jv2_400_800m(df)
    df = jv3_400_800m(df)
    df = af_ua1_800_1600m(df)
    df = am_ua2_800_1600m(df)
    df = jv1_800_1600m(df)
    df = jv2_800_1600m(df)
    df = jv3_800_1600m(df)
    df = af_ua1_over_1600m(df)
    df = am_ua2_over_1600m(df)

    df = min_adult_0_200m(df)
    df = min_adult_200_400m(df)
    df = min_adult_400_800m(df)
    df = min_adult_800_1600m(df)
    df = jv1_nearest_adult_0m(df)
    df = jv2_nearest_adult_0m(df)
    df = jv3_nearest_adult_0m(df)
    df = jv1_nearest_adult_0_200m(df)
    df = jv2_nearest_adult_0_200m(df)
    df = jv3_nearest_adult_0_200m(df)
    df = jv1_nearest_adult_200_400m(df)
    df = jv2_nearest_adult_200_400m(df)
    df = jv3_nearest_adult_200_400m(df)
    df = jv1_nearest_adult_400_800m(df)
    df = jv2_nearest_adult_400_800m(df)
    df = jv3_nearest_adult_400_800m(df)
    df = jv1_nearest_adult_800_1600m(df)
    df = jv2_nearest_adult_800_1600m(df)
    df = jv3_nearest_adult_800_1600m(df)
    df = jv1_nearest_adult_gt_1600m(df)
    df = jv2_nearest_adult_gt_1600m(df)
    df = jv3_nearest_adult_gt_1600m(df)
    df = min_adult_over_1600m(df)
    df = min_adult_0m(df)
    
    # Adult presence logic
    df = at_least_one_adult_present(df)
    
    df = two_adults_attending(df)
    
    # Presence logic
    df = no_adult_present(df)
    df = only_one_adult_present(df)
    df = two_adults_present(df)
    df = at_least_one_adult_in_nest_tree(df)
    
    print("Pipeline complete!")
    return df

from math import ceil
import pandas as pd

# ============================================================
# 1. REGISTER PIPELINE FUNCTIONS (ORDER MATTERS)
# ============================================================

PIPELINE_FUNCTIONS = [
    # -------------------- Coordinates --------------------
    nest_longitude,
    nest_latitude,
    p1_longitude,
    p1_latitude,
    p2_longitude,
    p2_latitude,
    p3_longitude,
    p3_latitude,
    p4_longitude,
    p4_latitude,
    p5_longitude,
    p5_latitude,

    # -------------------- Distances from nest --------------------
    p1_dist_from_nest_code,
    p2_dist_from_nest_code,
    p3_dist_from_nest_code,
    p4_dist_from_nest_code,
    p5_dist_from_nest_code,
    min_adult_dist_from_nest_code,

    # -------------------- Adult presence --------------------
    AF_Present,
    AM_Present,
    UA1_Present,
    UA2_Present,
    at_least_one_adult_present,

    # -------------------- Juvenile presence --------------------
    JV1_Present,
    JV2_Present,
    JV3_Present,
    At_least_one_JV_Present,

    # -------------------- Juvenile–Adult distances --------------------
    jv1_dist_from_nearest_adult_code,
    jv2_dist_from_nearest_adult_code,
    jv3_dist_from_nearest_adult_code,

    # -------------------- AF --------------------
    AF_In_Nest_Tree,
    AF_Attending,
    AF_three_and_four,
    AF_Flying,
    AF_Eating,
    AF_Feeding_Young,
    AF_NestBuilding,
    AF_Incubating,
    AF_Brooding,
    AF_Aggressive_Interaction,
    AF_Copulating,
    af_surveytime,

    # -------------------- AM --------------------
    AM_In_Nest_Tree,
    AM_Attending,
    AM_three_and_four,
    AM_Flying,
    AM_Eating,
    AM_Feeding_Young,
    AM_NestBuilding,
    AM_Incubating,
    AM_Brooding,
    AM_Aggressive_Interaction,
    AM_Copulating,
    am_surveytime,

    # -------------------- UA1 --------------------
    UA1_In_Nest_Tree,
    UA1_Attending,
    UA1_three_and_four,
    UA1_Flying,
    UA1_Eating,
    UA1_Feeding_Young,
    UA1_NestBuilding,
    UA1_Incubating,
    UA1_Brooding,
    UA1_Aggressive_Interaction,
    UA1_Copulating,
    ua1_surveytime,

    # -------------------- UA2 --------------------
    UA2_In_Nest_Tree,
    UA2_Attending,
    UA2_three_and_four,
    UA2_Flying,
    UA2_Eating,
    UA2_Feeding_Young,
    UA2_NestBuilding,
    UA2_Incubating,
    UA2_Brooding,
    UA2_Aggressive_Interaction,
    UA2_Copulating,
    ua2_surveytime,

    # -------------------- JV1 --------------------
    JV1_In_Nest_Tree,
    JV1_Attending,
    jv1_surveytime,
    JV1_Aggressive_Interaction,
    JV1_Flying,
    JV1_Eating,
    JV1_Feeding_Young,
    JV1_NestBuilding,
    JV1_Copulating,
    JV1_three_and_four,
    JV1_Brooding,
    JV1_Incubating,
    jv1_dist_from_af_ua1_code,
    jv1_dist_from_am_ua2_code,
    No_Adults_Present_but_JV1_Present,

    # -------------------- JV2 --------------------
    JV2_In_Nest_Tree,
    JV2_Attending,
    jv2_surveytime,
    JV2_Aggressive_Interaction,
    JV2_Flying,
    JV2_Eating,
    JV2_Feeding_Young,
    JV2_NestBuilding,
    JV2_Copulating,
    JV2_three_and_four,
    JV2_Brooding,
    JV2_Incubating,
    jv2_dist_from_af_ua1_code,
    jv2_dist_from_am_ua2_code,
    No_Adults_Present_but_JV2_Present,

    # -------------------- JV3 --------------------
    JV3_In_Nest_Tree,
    JV3_Attending,
    jv3_surveytime,
    JV3_Aggressive_Interaction,
    JV3_Flying,
    JV3_Eating,
    JV3_Feeding_Young,
    JV3_NestBuilding,
    JV3_Copulating,
    JV3_three_and_four,
    JV3_Brooding,
    JV3_Incubating,
    jv3_dist_from_af_ua1_code,
    jv3_dist_from_am_ua2_code,
    No_Adults_Present_but_JV3_Present,

    # -------------------- FEHA --------------------
    FEHA1_Present,
    FEHA1_surveytime,
    FEHA1_Aggressive_Interaction,
    FEHA1_Flying,
    FEHA1_Eating,
    FEHA1_Feeding_Young,
    FEHA1_NestBuilding,
    FEHA1_Copulating,
    FEHA1_Brooding,
    FEHA1_Incubating,
    FEHA1_In_Nest_Tree,
    FEHA1_Attending,

    FEHA2_Present,
    FEHA2_surveytime,
    FEHA2_Aggressive_Interaction,
    FEHA2_Flying,
    FEHA2_Eating,
    FEHA2_Feeding_Young,
    FEHA2_NestBuilding,
    FEHA2_Copulating,
    FEHA2_Brooding,
    FEHA2_Incubating,
    FEHA2_In_Nest_Tree,
    FEHA2_Attending,

    At_Least_One_FEHA_Present,

    # -------------------- Presence logic --------------------
    no_adult_present,
    only_one_adult_present,
    two_adults_present,
    at_least_one_adult_in_nest_tree,
    two_adults_attending,
]

def add_two_week_bins(df, date_col):
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # Monday-based 2-week bins
    df["two_week_bin"] = (
        df[date_col]
        .dt.to_period("2W")
        .apply(lambda p: f"{p.start_time.date()} → {p.end_time.date()}")
    )

    return df
def run_pipeline_once(df, functions):
    for func in functions:
        df = func(df)
    return df

def load_data_streamlit(file):
    try:
        # Streamlit UploadedFile → bytes
        if hasattr(file, "read"):
            content = file.read()
            try:
                data = pd.read_csv(
                    pd.io.common.BytesIO(content),
                    encoding="utf-8",
                    low_memory=False,
                )
            except UnicodeDecodeError:
                data = pd.read_csv(
                    pd.io.common.BytesIO(content),
                    encoding="latin1",
                    low_memory=False,
                )
        else:
            # Normal file path
            try:
                data = pd.read_csv(file, low_memory=False)
            except UnicodeDecodeError:
                data = pd.read_csv(file, encoding="latin1", low_memory=False)

        print(f"Successfully loaded {len(data)} rows")
        return data

    except Exception as e:
        print(f"Error loading file: {e}")
        return None



# ============================================================
# 5. EXPORT TABS TO EXCEL (OPTIONAL)
# ============================================================

def export_pipeline_tabs(tabs, path="pipeline_tabs.xlsx"):
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for tab, funcs in tabs.items():
            df = pd.DataFrame({"function": [f.__name__ for f in funcs]})
            df.to_excel(writer, sheet_name=tab[:31], index=False)

def aggregate_by_two_weeks(df, date_col='date', period= '2W', metric = None, percent=False, output_path='Period_week_summary.xlsx'):
    """
    Aggregate all metrics by 2-week periods.
    Sums all numeric columns (the 0/3 indicator columns).
    `metric` may be None, a single column name (string), or a list/tuple of column names.
    """
    df = df.copy()
    df['female_counts_per_date'] = df['date'].where(df['female.loc'].isin([0,14])).groupby(df['date']).transform('count')
    df['male_counts_per_date'] = df['date'].where(df['male.loc'].isin([0,14])).groupby(df['date']).transform('count')
    df['undiff1_counts_per_date'] = df['date'].where(df['undiff1.loc'].isin([0,14])).groupby(df['date']).transform('count')
    df['undiff2_counts_per_date'] = df['date'].where(df['undiff2.loc'].isin([0,14])).groupby(df['date']).transform('count')

    df['total_valid_sex_counts_per_period'] = (df['female_counts_per_date'] + df['male_counts_per_date'] )/ (df['female_counts_per_date'] + df['male_counts_per_date'] + df['undiff1_counts_per_date'] + df['undiff2_counts_per_date'])
    df = df.drop(columns=['female_counts_per_date', 'male_counts_per_date', 'undiff1_counts_per_date', 'undiff2_counts_per_date'])
    df = df[df['total_valid_sex_counts_per_period'].fillna(0) >= 0.9]

    if metric:
        try:
            if isinstance(metric, (list, tuple)):
                cols = ['date', 'time'] + list(metric)
            else:
                cols = ['date', 'time', metric]
            df = df[cols]
        except Exception:
            pass
    
    # Convert date to datetime
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    
    # Create 2-week bins (Monday-based)
    df["Period_bin"] = df[date_col].dt.to_period(period)
    
    # Get all columns that should be summed (the metric columns with 0/3 values)
    # Exclude columns we don't want to sum
    exclude_cols = [
        date_col, 'Period_bin', 'observer', 'notes', 'nest.name',
        'p1', 'p2', 'p3', 'p4', 'p5',  # perch codes
        'female.loc', 'male.loc', 'undiff1.loc', 'undiff2.loc',  # location codes
        'juv1.loc', 'juv2.loc', 'juv3.loc',
        'female.be', 'male.be', 'undiff1.be', 'undiff2.be',  # behavior codes
        'juv1.be', 'juv2.be', 'juv3.be',
        'FEHA_1.loc', 'FEHA_1.be', 'FEHA_2.loc', 'FEHA_2.be',
        'OBE.loc', 'OBE.be',
        'GOEA_1.loc', 'GOEA_1.be', 'GOEA_2.loc', 'GOEA_2.be', 'GOEA_3.loc', 'GOEA_3.be',
        'time', 'time.s', 'time.e', 'weather', 'temp_f', 'windSp_avg',
        'longitude', 'latitude',  # coordinate columns
    ]
    


    # Get numeric columns (the metric columns)
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    sum_cols = [col for col in numeric_cols if col not in exclude_cols and not any(x in col for x in ['longitude', 'latitude', 'dist_from_nest'])]
    
    # Group by two_week_bin and sum
    agg_df = df.groupby('Period_bin', as_index=False)[sum_cols].sum()
    
    # Convert period back to readable string
    agg_df['Period_bin'] = agg_df['Period_bin'].apply(
        lambda p: f"{p.start_time.date()} to {p.end_time.date()}"
    )
    
    # Sort by date
    agg_df = agg_df.sort_values('Period_bin')
    
    # Count observations per 2-week period
    obs_count = df.groupby('Period_bin').size().reset_index(name='num_observations')
    obs_count = obs_count[obs_count['num_observations'] > 3]  # only include periods with >3 observations
    obs_count['Period_bin'] = obs_count['Period_bin'].apply(
        lambda p: f"{p.start_time.date()} to {p.end_time.date()}"
    )
    
    agg_df = agg_df.merge(obs_count, on='Period_bin', how='left')
    
    # Reorder columns: two_week_bin, num_observations, then metrics
    cols = ['Period_bin', 'num_observations'] + [c for c in agg_df.columns if c not in ['Period_bin', 'num_observations']]
    agg_df = agg_df[cols]

    if percent:
        # add a coloumn calculated percentage
        for col in sum_cols:
            perc_col = f"{col}_percent"
            agg_df[perc_col] = (agg_df[col] / (agg_df['num_observations'] * 3)) * 100
    
    # Save to Excel
    agg_df.to_excel(output_path, index=False, engine='openpyxl')
    
    print(f"\n✓ Aggregated {len(agg_df)} two-week periods")
    print(f"✓ Summed {len(sum_cols)} metric columns")
    print(f"✓ Saved to: {output_path}")
    
    return agg_df

def load_perch_coords(perch_code_file, selected_perch=None):
    df = pd.read_excel(perch_code_file)

    # Normalize column names just in case
    df.columns = df.columns.str.strip()

    # Expected columns Nest	Perch	X	Y
    required = {"Nest", "Perch", "X", "Y"}
    if not required.issubset(df.columns):
        raise ValueError(f"Excel file must contain columns: {required}")

    # Optional filtering
    if selected_perch:
        selected = [p.strip() for p in selected_perch.split(",")]
        df = df[df["Nest"].isin(selected)]

    # Build dict: {code: (lon, lat)}
    perch_coords = {
        row["Perch"]: (row["X"], row["Y"])
        for _, row in df.iterrows()
    }

    return perch_coords

'''

if __name__ == "__main__":
    file_path = "/Users/faisalshahin/Documents/dana_part2/data/Hygiene Heather Oct2016 to Oct7_2025 UPDATED_Dec1_2025.csv"
    perch_code_file = "/Users/faisalshahin/Documents/dana_part2/src/All Nest Perches Updated 1-7-26.xlsx"
    selected_perch = input("Enter Nest to filter by (comma-separated), or press Enter to skip: ")
    Selected_Period = input('Please select your desired time interval for the output: ')
    
    if selected_perch:
        perch_codes_df = load_perch_coords(perch_code_file, selected_perch)
        print(f"Loaded perch codes for nests: {selected_perch}")

        PERCH_COORDS = perch_codes_df
    else:
        print('Using default Hygeine perch codes.')
    
    
    custom_dates = input("Enter custom date range (YYYY-MM-DD to YYYY-MM-DD), or press Enter to skip: ")
        
    
    df = load_data(file_path)
    df_dist_code = pd.read_excel(perch_code_file)
    df_dist_code = df_dist_code[['Nest','Perch', 'Dist from Nest (m)']]
    if selected_perch:
        df_dist_code = df_dist_code[df_dist_code['Nest'] == selected_perch]

    

    
        # OPTIONAL but recommended: clean strings
    df_dist_code['Perch'] = df_dist_code['Perch'].astype(str).str.strip()

    # Collapse to ONE distance per Perch (critical)
    df_dist_code = (
        df_dist_code
        .groupby('Perch', as_index=False)['Dist from Nest (m)']
        .first()
    )

    # Build lookup dictionary
    perch_to_dist = dict(
        zip(df_dist_code['Perch'], df_dist_code['Dist from Nest (m)'])
    )

    # Fill p1_dist through p5_dist
    for i in range(1, 6):
        p_col = f'p{i}'
        dist_col = f'p{i}_dist'

        df[dist_col] = (
            df[p_col]
            .astype(str)
            .str.strip()
            .map(perch_to_dist)
        )


    if custom_dates:
        try:
            start_date, end_date = [d.strip() for d in custom_dates.split("to")]
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
            print(f"Filtered data to date range: {start_date.date()} to {end_date.date()}")
        except Exception as e:
            print(f"Error parsing custom date range: {e}")
    
     # Convert relevant columns to numeric

    for col in df.columns:
        if 'loc' in col:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        if 'p' in col and len(col) == 2:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    
    if df is not None:
        
         # Run the full pipeline
        df = run_pipeline(df)
        
        print("\nFirst few rows:")
        print(df.head())
        
        print("\nNew columns created:")
        new_cols = [col for col in df.columns if any(x in col for x in ['_Present', '_Attending', '_dist_', 'adult_present', 'min_adult'])]
        print(new_cols)
        

        
        output_path = "processed_data.csv"
        df.to_csv(output_path, index=False)
        print(f"\nSaved processed file to: {output_path}")
        for item in PIPELINE_FUNCTIONS:
            print(item)
        selected_metric = input('Please select the metric you want to process: ')
        if Selected_Period and selected_metric:
            aggregate_by_two_weeks(df, period = Selected_Period, metric = selected_metric)
        elif selected_metric and not Selected_Period:
            aggregate_by_two_weeks(df, metric = selected_metric)
        elif Selected_Period and not selected_metric:
            aggregate_by_two_weeks(df, period = Selected_Period)
        else:
            aggregate_by_two_weeks(df)

       

'''

