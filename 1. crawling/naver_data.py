import os
import pandas as pd
import requests
import xmltodict

#url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'
url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'

path = os.getcwd() + '\\utils\\'

with open(path+'apikey.txt','r') as f: keys = f.read().splitlines()
with open(path+'period.txt','r') as f: period = f.read().splitlines()
with open(path+'region.txt','r') as f: region = f.read().splitlines()
with open(path+'savepoint.txt','r') as f:
    try: p_svpt, r_svpt = f.read().split()
    except: p_svpt, r_svpt = period[0], region[0]




def main():
    def api_call():
        return xmltodict.parse(requests.get(url, params={
            'serviceKey':keys[key_idx], 
            'numOfRows':99999,
            'LAWD_CD':region_cd, 
            'DEAL_YMD':period_cd
            }).content)

    key_idx, cnt, save_cnt, error_cnt = [0]*4

    for period_cd in period:

        if period_cd < p_svpt: continue

        for region_cd in region:

            if period_cd == p_svpt and region_cd < r_svpt: continue
            
            try:
                data = api_call()

                while data['response']['header']['resultCode'] == '99':
                    key_idx += 1
                    if key_idx == len(keys):
                        with open(path+'log.txt', 'a') as f: f.write('-'*20+'\n')
                        input('\napi key limit exceeded. press enter to exit')
                        exit()
                    data = api_call()

                pd.DataFrame(data['response']['body']['items']['item']).to_csv(os.getcwd()+f'\\data\\raw_data\\{period_cd}_{region_cd}.csv', encoding='cp949', index=False)

                with open(path+'log.txt', 'a') as f: f.write(f'{period_cd} {region_cd} successfully downloaded\n')
                save_cnt += 1

            except Exception as e:
                with open(path+'log.txt', 'a') as f: f.write(f'{period_cd} {region_cd} {e}\n')
                error_cnt += 1

            finally:
                cnt += 1
                print(f'\r{cnt} completed. saved: {save_cnt} error: {error_cnt}', end='')
                with open(path+'savepoint.txt', 'w') as f: f.write(f'{period_cd} {region_cd}')

if __name__=='__main__': 
    main()
