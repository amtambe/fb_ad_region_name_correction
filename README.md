# fb_ad_region_name_correction
Clean up region names from the Facebook Ads Library to be compatible with Natural Earth goegraphical dataset. Pass in a region name to the region_to_country method; the output is a tuple (correct region name, country name, iso code). 

How to use:

First, get a Facebook API access token from this URL: https://www.facebook.com/ads/library/api/?source=archive-landing-page

converter = RegionConverter(access_token)
conv.region_to_country("Washington State")

Output: (Washington, United States, US)
