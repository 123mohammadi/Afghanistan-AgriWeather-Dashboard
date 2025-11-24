# elt_windows_service.py
# دا به ستاسو ETL هر 6 ساعته په اتومات ډول رن کړي - 24/7
# نصبولو لارښوونه لاندې ده

import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import time
import logging
import os
from datetime import datetime
from threading import Thread

# د لاګ تنظیم (ستاسو پخوانی لاګ فایل کارول کیږي)
logging.basicConfig(
    filename='elt_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# د اصلي ETL فایل ایمپورټ کول
try:
    from enhanced_elt_pipeline import run_elt_pipeline
except ImportError as e:
    logger.error(f"enhanced_elt_pipeline.py ایمپورټ نه شو: {e}")
    raise


class ETLWindowsService(win32serviceutil.ServiceFramework):
    _svc_name_ = "AfghanistanETLService"
    _svc_display_name_ = "افغانستان سمارټ کرنه ETL سرویس (24/7)"
    _svc_description_ = "هر ۶ ساعته اتومات د کرنې او روغتیا ډاټا تازه کوي - WeatherDataWarehouse"

    def __init__(self, args):
        super().__init__(args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.is_running = True
        logger.info("ETL Windows Service شروع شو - %s", datetime.now())

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_running = False
        logger.info("ETL Service ودرول شو")

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def main(self):
        logger.info("ETL Service په بشپړ ډول پیل شو - هر ۶ ساعته به رن کیږي")

        # لومړی ځل سمدستي رن کړه
        self.run_etl_once()

        # بيا هر 6 ساعته (21600 ثانیې)
        while self.is_running:
            time.sleep(21600)  # 6 ساعته = 6 × 60 × 60 = 21600 ثانیې
            if self.is_running:
                self.run_etl_once()

    def run_etl_once(self):
        try:
            start_time = datetime.now()
            logger.info("=== ETL پیل شو - %s ===", start_time.strftime("%Y-%m-%d %H:%M:%S"))

            # دلته اصلي ETL چلیږي
            run_elt_pipeline()

            end_time = datetime.now()
            duration = end_time - start_time
            logger.info("ETL په بریالیتوب سره پای ته ورسېد - وخت: %s", str(duration))

        except Exception as e:
            logger.error("ETL په ناکامۍ سره پای ته ورسېد: %s", str(e), exc_info=True)


if __name__ == '__main__':
    # دا به یوازې په CMD کې د install/uninstall/start/stop لپاره کار وکړي
    win32serviceutil.HandleCommandLine(ETLWindowsService)