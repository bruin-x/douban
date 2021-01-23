import logging
import json
import time 

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(filename)s[%(lineno)d] %(levelname)s: %(message)s'
    )

WAIT = 10
BASE_URL = 'https://www.douban.com/'
LOGIN_URL = 'https://accounts.douban.com/passport/login_popup?login_source=anony'


def read_data():
    with open('accountCookies.json', 'r') as f:
        data = json.load(f)
    return data


def save_data(data):
    with open('accountCookies.json', 'w') as f:
        json.dump(data, f)
        logging.info('save data successful: %s' % data)


def save_cookies():
    data = read_data()
    accounts = data['accounts']
    for acc in accounts:
        cookie = login_popup(acc)
        # cookie = acc['user']
        if data['cookies'].__contains__(acc['user']):
            data['cookies'][acc['user']] = cookie
        else:
            data['cookies'].update({acc['user']: cookie})
        
        save_data(data)


def login_popup(account):
    if account:
        user = account['user']
        pw = account['password']
    else:
        logging.warning('Have no account in accountCookies!')

    # login to douban get cookies 
    logging.info('selenium get cookies...')

    opt = webdriver.FirefoxOptions()
    # opt.add_argument('--headless')  # 设置无头模式
    
    brow = webdriver.Firefox(options=opt)
    wait = WebDriverWait(brow, WAIT)
    
    try:
        brow.get(LOGIN_URL)
    except TimeoutError:
        logging.error('get %s timeout' % LOGIN_URL)
    
    try:
        brow.find_element_by_xpath('//ul[@class="tab-start"]/li[2]').click()
        brow.find_element_by_id('username').send_keys(user)
        brow.find_element_by_id('password').send_keys(pw)
        brow.find_element_by_xpath('//div[@class="account-form-field-submit "]').click()
        wait.until(EC.presence_of_element_located((By.XPATH, '//li[@class="nav-user-account"]')))
        brow.get(BASE_URL).refresh()
        time.sleep(5)
    except Exception as e:
        logging.error('failed when login %s: %s' %(user, e))
    else:    
        cookies = brow.get_cookies()
    finally:
        brow.close()

    if not cookies:
        logging.warning('get cookies failed!')
    cookies = {cookie['name']: cookie['value'] for cookie in cookies}
    cookies = ''.join(['{}={};'.format(k,v) for k,v in cookies.items()])

    return cookies


if __name__ == "__main__":
    save_cookies()