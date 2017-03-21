#
# Geonames
#
import os, urllib2, json


def countrySubdivision(lat, lng):
	url = "http://api.geonames.org/countrySubdivisionJSON?lat="+str(lat)+"&lng="+str(lng)+"&username=cappelaere"
	#print url
	response 	= urllib2.urlopen(url).read()
	data 		= json.loads(response)
	#print response
	return data
	
def findNearbyPlaceName(lat, lng):
	url = "http://api.geonames.org/findNearbyPlaceNameJSON?lat="+str(lat)+"&lng="+str(lng)+"&radius=300&style=FULL&cities=cities1000&maxRows=10&username=cappelaere"
	#print url
	response 	= urllib2.urlopen(url).read()
	data 		= json.loads(response)
	
	# fallback
	# url = "http://api.geonames.org/findNearbyJSON?lat="+lat+"&lng="+lng+"&style=FULL&username=cappelaere"
	
	#print data
	return data
	
def info(lat, lng):
	countryInfo 	= countrySubdivision(lat, lng)
	nearbyPlaceInfo = findNearbyPlaceName(lat, lng)
	
	if countryInfo:
		countryName 	= countryInfo['countryName']
		countryCode 	= countryInfo['countryCode']
		adminName1		= countryInfo['adminName1']
		
	if nearbyPlaceInfo:
		geoNames		= nearbyPlaceInfo['geonames']
		near 			= []
		for g in geoNames:
			near.append( g['toponymName'] )
		
	geonameInfo = {
		'countryName': 	countryName,
		'countryCode': 	countryCode,
		'adminName': 	adminName1,
		'nearBy': 		', '.join(near)
	}
	#print 	geonameInfo
	return geonameInfo
	
if __name__ == '__main__':
	lat = 38.404633
	lng = -115.718285
	info(lat, lng)
	