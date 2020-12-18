############################################################################
# Regions to countries

# The goal is to (1) add iso codes and country names to each region, and (2)
# to match the region names to a geographic dataset (specifically the Natural Earth
# 10m Administrative Regions dataset) so we can map it. Region names are non-standard,
# so this script corrects them and gets iso / country codes. 

# Notes on usage:
    #  -Regions are usually states or provinces - bigger than cities, smaller than countries
    #  -Returns a tuple of (Country name, ISO country code)
    #  -You must get a Facebook Developer access token to use it
    #  -If a region search returns no valid regions, returns (None, None) tuple
    #  -If a region search returns multiple valid regions, returns the first match.
    #  Matches are typically sorted by relevance but there may be inaccuracies. 
    #  -You can set it to issue a warning if there isn't an exact match with 
    #  the query region. 

# How it works:
    # It checks through several sources, using the next if the region name is not
    # found in a previous one. The sources in order are: 
    # 1. An internal dictionary, which it updates with region-country pairs when
    # new matches are found.
    # 2. the NE10 geographical dataset itself
    # 3. A manually specified dictionary to edit the region name, and then it tries
    # the Ne10 dataset again.
    # 4. A Facebook API, then tries the NE10 dataset again
    # 5. Manually specified dictionary to get the country and iso code directly.
    # 6. Gives up and leaves it blank. 
###############################################################################
import os.path
import csv
import requests
import warnings
import pandas as pd
import geopandas as gpd
import re
import numpy as np

class RegionConverter:

    def __init__(self, access_token, manual_dict_path=None, geo_path=None, iso_path=None, warn_region_mismatch=True, encoding="latin-1"):
        """
        Initializes the region converter object. 
        ----
        access_token: Facebook developer access token. See Facebook API to get one.

        dict_path: location of the "special regions" dictionary. By default,
        searches its own folder for it. The file CANNOT be named regions_dict. 

        geo_path: location of the geographical data (should just be from the Natural Earth 
        database). Searches its own folder by default

        iso_path: location of iso code data. Lots od online sources for this. Columns should
        be named 'iso' and 'country_name'

        warn_region_mismatch: Behavior when input query does not match the first result's region
        name (e.g. query "Calif", first result is for "California"). If 0, uses the first result
        as normal. If 1, issues a warning. If 2, returns (None, None) as if the object wasn't found.
        When saving the name dictionary - 0 saves as is ("Calif" --> "United States"), 1 saves 
        corrected version ("California" --> "United States") and 2 doesn't save anything.  US". 
        1 by default. 

        encoding: when reading from the manually specified dictionary, which encoding to use. FB
        appears to use latin-1 so this is the default. 
        """

        #set up path names for the 4 datasources 
        if manual_dict_path is None:
            manual_dict_path = os.path.join(
                os.path.split(os.path.abspath(__file__))[0], "manual_regions.csv")
        self.__dict_path__ = os.path.join(
            os.path.split(os.path.abspath(__file__))[0], "region_dict.csv")
        if geo_path is None:
            geo_path = os.path.join(
                os.path.split(os.path.abspath(__file__))[0], "ne_10m_admin_1_states_provinces.shp"
            )
        if iso_path is None:
            iso_path = os.path.join(os.path.split(os.path.abspath(__file__))[0], "iso.csv")

        #set variables/parameters
        self.error_regions = []
        self.suffixes = ["Province", "Region", "District", "State", "Governate", "Department", "Oblast", "City", "Zone", "Prefecture"]
        self.__encoding__ = encoding
        self.__warn_region_mismatch__ = warn_region_mismatch

        #open files into dataframes/dicts
        self.__manual_dict__ = self.__read_to_dict__(manual_dict_path)
        self.__region_dict__ = self.__read_to_dict__(self.__dict_path__)
        self.__start_dict_len__ = len(self.__region_dict__)
        #remove all redundant keys from the manually specified dictionary
        for k in self.__region_dict__.keys():
            self.__manual_dict__.pop(k, None)
        self.__geos__ = self.__open_geos__(geo_path, iso_path)
        self.__header__ = self.__create_header__(access_token)

    def __del__(self):
        #only update csv if the dict has gotten longer
        print("The following regions could not be found: ")
        print(list(set(self.error_regions)))
        print("\n")
        if len(self.__region_dict__) > self.__start_dict_len__:
            self.__write_dict__()

    def __tuple_to_list__(self, item):
        """
        helper function to convert none objects to empty strings. 
        pass in a dict item
        """
        out = [item[0]]
        for b in item[1]:
            if b is None:
                b = ""
            out.append(b)
        return out

    def __write_dict__(self):
        """
        Save the regions dictionary as a csv. Called by the destructor. 
        """
        wr = csv.writer(open(self.__dict_path__, 'w', encoding=self.__encoding__))
        for item in self.__region_dict__.items():
            try:
                #write key, corrected_region, country, iso
                wr.writerow(self.__tuple_to_list__(item))
            except:
                print("Failed writing: {}, {}, {}".format(item[0], item[1][0], item[1][1], item[1][2]))

    def __read_to_dict__(self, path):
        """
        Read the csv into the regions dictionary. Columns MUST be in order:
        region, corrected_region, country, iso
        """
        region_dict = {}
        counter = 0
        try:
            with open(path, mode='r', newline='', encoding=self.__encoding__) as infile:
                reader = csv.reader(infile)
                for row in reader:
                    counter += 1
                    region_dict[row[0]] = (row[1], row[2], row[3])
            return region_dict
        except Exception as e:
            warnings.warn("Failed to read dict: row {}".format(str(counter)))
            print(e)
            return region_dict
        infile.close()
    
    def __open_geos__(self, geo_path, iso_path):
        """
        open geopandas file and drop everything besides the iso and region names.
        Merge with iso file to see country name as well.
        Also set a geocols variable to quickly see which cols correspond to name.

        iso must have columns 'iso' and 'country_name' for it to work! 
        """
        df = gpd.read_file(geo_path)
        iso_df = pd.read_csv(iso_path, keep_default_na=False)
        drop_idxs = ~df.columns.str.contains('name') & ~df.columns.str.contains('iso')
        df = df.drop(df.columns[drop_idxs], axis=1)
        df = df.drop(['name_len', 'name_zh'], axis=1) #length and chinese characters irrelevant
        self.__geocols__ = df.columns[df.columns.str.contains('name')]
        df = df.merge(iso_df, left_on='iso_a2', right_on='iso', how='left')
        df = df[df['country_name'] != 'Burma'] #old name no longer used
        return df

    def __create_header__(self, bearer_token):
        """
        Creates header dictionary for a request
        """
        header = {"Authorization": "Bearer {}".format(bearer_token)}
        return header
    
    def __connect_to_endpoint__(self, url):
        """
        Get the JSON response from the URL with auth headers specified
        """
        response = requests.request("GET", url, headers=self.__header__)    
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def __remove_parens__(self, string):
        return re.sub(r" ?\([^)]+\)", "", string)

    def __remove_suffix__(self, string):
        for suffix in self.suffixes:
            string = string.replace(suffix, "")
        string = re.sub(' +', ' ', string).rstrip()
        return string

    def region_to_country_from_API(self, region):
        """
        Use Facebook Location Search API to get the name and code
        corresponding to the region. Make sure to specify access token above. 

        If the region is not found, it returns a tuple of Nones. This won't
        immediately throw an exception. 
        """
        url = "https://graph.facebook.com/v8.0/search?location_types=[%22region%22]&type=adgeolocation&q=" + region
        data = self.__connect_to_endpoint__(url)['data']
        if len(data) > 0:
            country_name = data[0]['country_name']
            iso = data[0]['country_code']
            corrected_name = data[0]['name']            
            #update dict and return
            self.__region_dict__[region] = (corrected_name, country_name, iso)
            return corrected_name, country_name, iso
        else:
            #try dropping the suffix and running again
            region_drop_suffix = self.__remove_suffix__(region)
            if region_drop_suffix != region:
                return self.region_to_country_from_API(region_drop_suffix)
            #base case - there is no suffix left to drop
            else:            
                return None

    def region_to_country_from_geo(self, region):
        """
        Use the geo dataframe to find the region. 

        Return None if not found. 
        """
        corrected_name = self.__remove_suffix__(region)
        mask = np.column_stack(
            [self.__geos__[c].str.contains(corrected_name, na=False) for c in self.__geocols__])
        if mask.sum() == 0:  #no matches found
            return None
        matched = self.__geos__.loc[mask.any(axis=1)]
        if len(matched) > 1: #multiple matches --> check for exact matches
            # try exact match original name first - preferred
            mask = np.column_stack([matched[c] == region for c in self.__geocols__])
            if mask.sum() == 0: # if not found, try exact match of the corrected name
                mask = np.column_stack([matched[c] == region for c in self.__geocols__])
                if mask.sum() > 0: # if still not found, just use 1st match
                    matched = matched.loc[mask.any(axis=1)]
            else:
                matched = matched.loc[mask.any(axis=1)]
        #get name, country, iso from the df row. 
        corrected_name = matched['name'].values[0]
        country = matched['country_name'].values[0]
        iso = matched['iso_a2'].values[0]
        self.__region_dict__[region] = (corrected_name, country, iso)
        return corrected_name, country, iso

    def region_to_country(self, region):
        """
        Convert region to country. First tries reading from the dictionary, 
        but if this fails it will call the facebook API. 
        Returns (region, country, code) tuple. 

        region: region name string
        region_dict: dictionary, drawn from saved csv dictionary file
        """

        #remove parentheticals from name
        region = self.__remove_parens__(region)
        #attempt 1: directly from the region dict
        country_tuple = self.__region_dict__.get(region)

        #print("Attempt 1")
        if country_tuple is not None:
            return country_tuple
        
        #print("Attempt 2")
        #attempt 2: directly from geodataframe
        country_tuple = self.region_to_country_from_geo(region)
        if country_tuple is not None:
            return country_tuple

        #print("Attempt 3")
        #attempt 3: manual dictionary to geodf
        manual_tuple = self.__manual_dict__.get(region, (None, None)) #manual_t always has elements
        if manual_tuple[0] is not None and manual_tuple[0] != "":
            country_tuple = self.region_to_country_from_geo(manual_tuple[0])
            if country_tuple is not None:
                return country_tuple

        #print("Attempt 4")
        #attempt 4: facebook API to geodf
        facebook_tuple = self.region_to_country_from_API(region)
        if facebook_tuple is not None:
            country_tuple = self.region_to_country_from_geo(facebook_tuple[0])
            if country_tuple is not None:
                return country_tuple

        #print("Attempt 5")
        #attempt 5: at this point, we can't match to geodf, so we'll just try
        # to get the country code. 
        self.error_regions.append(region)
        if self.__warn_region_mismatch__:
            warnings.warn("Region {} will not match geo dataframe!".format(region))
        if manual_tuple is not None and manual_tuple[1] is not None and manual_tuple[2] is not None:
            #region_corrected is None since it didn't actually match the geo df
            out = (None, manual_tuple[1], manual_tuple[2])
            self.__region_dict__[region] = out
            return out
        elif facebook_tuple is not None:
            #region_corrected is None since it didn't actually match the geo df
            out = (None, facebook_tuple[1], facebook_tuple[2])
            self.__region_dict__[region] = out
            return out
        else:
            if self.__warn_region_mismatch__:
                warnings.warn("Region {} not found anywhere!".format(region))
            self.__region_dict__[region] = (None, None, None)
            return None, None, None
