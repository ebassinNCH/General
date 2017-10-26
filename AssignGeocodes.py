import os
from geopy import geocoders
#from geopy import geocoders.GoogleV3
# from osgeo import ogr, osr
from time import sleep

def geocode(ccn, f, address):
    g = geocoders.GoogleV3('AIzaSyChGd2mLA-3lyIF6WN0S4X4Ku0K03g9aF8',timeout=10)
    print(address)
    place, (lat, lng) = g.geocode(address)
    listParts = address.splitlines()
    lastLine = listParts[-1]
    if (lastLine[0]=='(') & (lastLine[-1]==')'):
        try:
            print('LastLine: ' + lastLine)
            lastLine = lastLine[1,-1]
            lat = lastLine.split(',')[0]
            lng = lastLine.split(',')[-1]
        except:
            pass
    print('%s:    %.5f    %.5f' % (place, lat, lng))
    f.write(ccn + '\t' + str(lat) + '\t' + str(lng) + '\t' + place + '\n')
    return lat, lng

df = pd.read_csv('/AdvAnalytics/Reference/RawData/Hospital_General_Info.csv',
                 dtype={'Provider ID': 'str',
                        'Zip Code' :'str',
                        'Phone Number': 'str', 
                        'Address': 'str',
                        'City': 'str',
                        'State': 'str'})
df2 = df.copy()
df2.sort_values('Provider ID', inplace=True)
df2 = df2[df2['Provider ID']>='2213']
df2.reset_index(inplace=True)
#df2 = df2.head().tail(10)
f = open('c:/temp/geocodes.txt', 'w')
for i in range(len(df2.index)):
    sleep(.02)
    if i % 100 == 1:
        print(i)
    ccn = df2['Provider ID'][i]
    address = df2.Location[i].replace('&amp;', '&')
    print(ccn)
    print(address)
    if(len(address)>5):
        try:
            lat, lng = geocode(ccn, f, address)
        except:
            sleep(.2)
            address = df2.City[i] + ', ' + df2.State[i]
            for f in range(4):
                print(' ')
            print('In except loop: ' + address)
            lat, lng = geocode(ccn, f, address)
            for f in range(4):
                print(' ')
        finally:
            pass
f.close()
    
    
#df2['FullAddress'] = df2.Address + ', ' + df2.City + ', ' + df2.State + '  ' + df2['ZIP Code']
#df2['Latitude'], df2['Longitude'] = df2.Location.apply(lambda x: geocode(x))

#print(df2.Location)