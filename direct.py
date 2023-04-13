import requests
import json
from datetime import date

from time import sleep
from loguru import logger

ConnectionError = requests.exceptions.ConnectionError

reportsURL = "https://api.direct.yandex.com/json/v5/reports"

today = str(date.today())

def u(x):
    if type(x) == type(b''):
        return x.decode('utf8')
    else:
        return x

def build_headers(access_token):
    headers = {
        "Authorization": "Bearer " + access_token,
        # "Client-Login": CLIENT_LOGIN, # NOTE Only needed if you're an agency
        # NOTE Language of the feedback messages
        "Accept-Language": "ru",
        "processingMode": "auto",
        # NOTE Don't return row with no. of rows in the end of the report
        "skipReportSummary": "true"
    }

    return headers

def build_body():
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": "2022-08-27",
                "DateTo": f"{today}" 
            },
            "FieldNames": [
                "Date",
                "CampaignName",
                "CampaignUrlPath",
                "LocationOfPresenceName",
                "Impressions",
                "Clicks",
                "Cost"
            ],
            "ReportName": f"{today}",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    body = json.dumps(body, indent=4)
    return body

def get_req(headers, body):
    while True:
        try:
            req = requests.post(reportsURL, body, headers=headers)
            req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8

            if req.status_code == 400:
                logger.critical("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                logger.critical("RequestId: {}".format(req.headers.get("RequestId", False)))
                logger.critical("JSON-код запроса: {}".format(u(body)))
                logger.critical("JSON-код ответа сервера: \n{}".format(u(req.json())))
                return None
                break

            elif req.status_code == 200:
                logger.success("Отчет создан успешно")
                logger.success("RequestId: {}".format(req.headers.get("RequestId", False)))
                # logger.success("Содержание отчета: \n{}".format(u(req.text)))
                return req
                break

            elif req.status_code == 201:
                logger.info("Отчет успешно поставлен в очередь в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                logger.info("Повторная отправка запроса через {} секунд".format(retryIn))
                logger.info("RequestId: {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)

            elif req.status_code == 202:
                logger.info("Отчет формируется в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                logger.info("Повторная отправка запроса через {} секунд".format(retryIn))
                logger.info("RequestId:  {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)

            elif req.status_code == 500:
                logger.critical("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                logger.critical("RequestId: {}".format(req.headers.get("RequestId", False)))
                logger.critical("JSON-код ответа сервера: \n{}".format(u(req.json())))
                return None
                break

            elif req.status_code == 502:
                logger.critical("Время формирования отчета превысило серверное ограничение.")
                logger.critical("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                logger.critical("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                logger.critical("JSON-код ответа сервера: \n{}".format(u(req.json())))
                return None
                break

            else:
                logger.critical("Произошла непредвиденная ошибка")
                logger.critical("RequestId:  {}".format(req.headers.get("RequestId", False)))
                logger.critical("JSON-код запроса: {}".format(body))
                logger.critical("JSON-код ответа сервера: \n{}".format(u(req.json())))
                return None
                break

            # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            logger.critical("Произошла ошибка соединения с сервером API")
            # Принудительный выход из цикла
            return None
            break

        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанализировать действия приложения
            logger.critical("Произошла непредвиденная ошибка")
            # Принудительный выход из цикла
            return None
            break

def write_req(req, account):
    with open(f"tmp_data/{account}.tsv", "w", encoding="utf-8") as f:
        f.write(req.text)

    with open(f'tmp_data/{account}.tsv', 'r') as fin:
        data = fin.read().splitlines(True)
    with open(f'tmp_data/{account}.tsv', 'w') as fout:
        fout.writelines(data[1:])

def get_stats(account, access_token):
    headers = build_headers(access_token)
    body = build_body()
    req = get_req(headers, body)

    if req:
        write_req(req, account)

    else:
        logger.critical("Something went wrong!")
