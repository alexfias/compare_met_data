import pandas as pd

def convert_reading_data(filename_wind, filename_solar, ctr_abb, output_snapshots=None):
    
    dti = pd.date_range('1979-01-01', '2018-12-31 21:00:00', freq='3H')
    
    wind = pd.read_csv(filename_wind,index_col=0)
    wind.index = dti
    wind = wind.rename(columns={col: ctr_abb[col.split('_')[3]] + ' wind' for col in wind.columns})
    
    solar = pd.read_csv(filename_solar,index_col=0)
    solar.index = dti
    solar = solar.rename(columns={col: ctr_abb[col.split('_')[0]] + ' solar PV' for col in solar.columns})
    
    #write as csv
    try:
        pd.concat([wind,solar],axis=1).reindex(output_snapshots).to_csv('p_max_pu_reading.csv')
    except: 
        pd.concat([wind,solar],axis=1).to_csv('p_max_pu_reading.csv')
        
    return wind, solar

def convert_emhires_data(filename_wind, filename_solar, ctr_abb, output_snapshots=None):
    
    dti = pd.date_range('1986-01-01', '2015-12-31 23:00:00', freq='H')
    
    wind = pd.read_csv(filename_wind,index_col=0)
    wind = wind.drop(columns=['Date','Year','Month','Day','Hour'])
    wind.index = dti
    wind = wind.rename(columns={col: col + ' wind' for col in wind.columns})
    
    solar = pd.read_csv(filename_solar,index_col=0)
    solar = solar.drop(columns=['Date','Year','Month','Day','Hour'])
    solar.index = dti
    solar = solar.rename(columns={col: col + ' solar PV' for col in solar.columns})
    
    #write as csv
    try:
        pd.concat([wind,solar],axis=1).reindex(output_snapshots).to_csv('p_max_pu_emhires.csv')
    except: 
        pd.concat([wind,solar],axis=1).to_csv('p_max_pu_reading.csv')
        
    return wind, solar
    
    def convert_restore_data(folder_name, ctr_abb, output_snapshots=None):
    #read capacities
    capa = pd.read_csv('./CapacitiesRestore2050.csv',index_col=0)
    
    #wind
    for k,v in ctr_abb.items():
        try:
            try:
                wind_new = pd.read_csv(folder_name+'Windonshore/'+v+'_windpower_ISI.csv',index_col=0)
                wind_new = wind_new.rename(columns = {'Feed-in [GW]':v+' wind'})
                wind_new = wind_new.drop(columns=['Month','Day','Hour'])
                wind_new = wind_new/capa.loc[v]['Wind on ISI']
                wind_new.index = pd.date_range('2003-01-01', '2012-12-31 23:00:00', freq='H')
                wind = pd.concat([wind,wind_new], axis=1)
            except:
                wind = pd.read_csv(folder_name+'Windonshore/'+v+'_windpower_ISI.csv',index_col=0)
                wind = wind.rename(columns = {'Feed-in [GW]':v+' wind'})
                wind = wind.drop(columns=['Month','Day','Hour'])
                wind = wind/capa.loc[v]['Wind on ISI']
                wind.index = pd.date_range('2003-01-01', '2012-12-31 23:00:00', freq='H')
        except:
            print(v+' does not exist')        
    
    #solar PV
    for year in ['2003','2004','2005','2006','2007','2008','2009','2010','2011','2012']:
        try:
            del solar
        except: 
            print(year)
        for k,v in ctr_abb.items():
            try:
                try:
                    solar_new = pd.read_csv(folder_name+'PVPower_ISI/PVPower_'+v+'_'+year+'.csv',index_col=0,names = ['Month','Day','Hour','Feed-in','Temp'])
                    solar_new = solar_new.rename(columns = {'Feed-in':v+' solar PV'})
                    solar_new = solar_new.drop(columns=['Month','Day','Hour','Temp'])
                    solar_new = solar_new/capa.loc[v]['PV ISI']
                    solar_new.index = pd.date_range(year+'-01-01', year+'-12-31 23:00:00', freq='H')
                    solar = pd.concat([solar,solar_new], axis=1)
                except:
                    solar = pd.read_csv(folder_name+'PVPower_ISI/PVPower_'+v+'_'+year+'.csv',index_col=0,names = ['Month','Day','Hour','Feed-in','Temp'])
                    solar = solar.rename(columns = {'Feed-in':v+' solar PV'})
                    solar = solar.drop(columns=['Month','Day','Hour','Temp'])
                    solar = solar/capa.loc[v]['PV ISI']
                    solar.index = pd.date_range(year+'-01-01', year+'-12-31 23:00:00', freq='H')
            except:
                print(v+' does not exist')  
             
        #concatenate years
        try: 
            solar_total = pd.concat([solar_total,solar])
        except:
            solar_total = solar
    
    #write as csv
    try:
        pd.concat([wind,solar_total],axis=1).reindex(output_snapshots).to_csv('p_max_pu_restore.csv')
    except: 
        pd.concat([wind,solar_total],axis=1).to_csv('p_max_pu_restore.csv')
        
    return wind,solar_total


def convert_rninja_solar_merra(folder_name, ctr_abb):
    for k,v in ctr_abb.items():

        try:
            try:
                data_new = pd.read_csv(folder_name_rninja+'ninja_pv_country_'+v+'_merra-2_corrected.csv', skiprows=2,index_col=0)
                data_new = data_new.rename(columns = {'national':v+' solar PV'})
                data = pd.concat([data,data_new], axis=1)
            except:
                data = pd.read_csv(folder_name_rninja+'ninja_pv_country_'+v+'_merra-2_corrected.csv', skiprows=2,index_col=0)
                data = data.rename(columns = {'national':v+' solar PV'})
        except:
            print(v+' does not exist')
    return data


def convert_rninja_solar_sarah(folder_name, ctr_abb):
    for k,v in ctr_abb.items():

        try:
            try:
                data_new = pd.read_csv(folder_name_rninja+'ninja_pv_country_'+v+'_sarah_corrected.csv', skiprows=2,index_col=0)
                data_new = data_new.rename(columns = {'national':v+' solar PV'})
                data = pd.concat([data,data_new], axis=1)
            except:
                data = pd.read_csv(folder_name_rninja+'ninja_pv_country_'+v+'_sarah_corrected.csv', skiprows=2,index_col=0)
                data = data.rename(columns = {'national':v+' solar PV'})
        except:
            print(v+' does not exist')
    return data

def convert_rninja_wind_current(folder_name, ctr_abb):
    for k,v in ctr_abb.items():

        try:
            try:
                data_new = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_current-merra-2_corrected.csv', skiprows=2,index_col=0)
                data_new = data_new.rename(columns = {'national':v+' wind'})
                data = pd.concat([data,data_new], axis=1)
            except:
                data = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_current-merra-2_corrected.csv', skiprows=2,index_col=0)
                data = data.rename(columns = {'national':v+' wind'})
        except:
            print(v+' does not exist')
    return data    

def convert_rninja_wind_nearfuture(folder_name, ctr_abb):
    for k,v in ctr_abb.items():

        try:
            try:
                #if near term future not given, take current instead
                try:
                    data_new = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_neartermfuture-merra-2_corrected.csv', skiprows=2,index_col=0)
                except:
                    data_new = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_current-merra-2_corrected.csv', skiprows=2,index_col=0)
                data_new = data_new.rename(columns = {'national':v+' wind'})
                data = pd.concat([data,data_new], axis=1)
            except:
                #if near term future not given, take current instead
                try:
                    data = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_neartermfuture-merra-2_corrected.csv', skiprows=2,index_col=0)
                except:
                    data = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_current-merra-2_corrected.csv', skiprows=2,index_col=0)
                data = data.rename(columns = {'national':v+' wind'})
        except:
            print(v+' does not exist')
    return data    

def convert_rninja_wind_longfuture(folder_name, ctr_abb):
    for k,v in ctr_abb.items():

        try:
            try:
                #if long term future not given, take near term instead. If this is also not given, take current.
                try:
                    data_new = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_longtermfuture-merra-2_corrected.csv', skiprows=2,index_col=0)
                except:
                    try:
                        data_new = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_neartermfuture-merra-2_corrected.csv', skiprows=2,index_col=0)
                    except:
                        data_new = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_current-merra-2_corrected.csv', skiprows=2,index_col=0)
                data_new = data_new.rename(columns = {'national':v+' wind'})
                data = pd.concat([data,data_new], axis=1)
            except:
                #if long term future not given, take near term instead. If this is also not given, take current.
                try:                          
                    data = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_longtermfuture-merra-2_corrected.csv', skiprows=2,index_col=0)
                except:
                    try:
                        data = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_neartermfuture-merra-2_corrected.csv', skiprows=2,index_col=0)
                    except:
                        data = pd.read_csv(folder_name_rninja+'ninja_wind_country_'+v+'_current-merra-2_corrected.csv', skiprows=2,index_col=0)
                data = data.rename(columns = {'national':v+' wind'})
        except:
            print(v+' does not exist')
    return data    

def convert_rninja_data(fname, ctr_abb, wind_func, solar_func):
    
    return wind_func(fname, ctr_abb).drop(['onshore','offshore'],axis=1), solar_func(fname, ctr_abb)
