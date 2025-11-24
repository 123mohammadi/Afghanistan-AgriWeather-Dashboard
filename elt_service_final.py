# elt_service_final.py - 100% کار کوونکی وینډوز سرویس (2025 - نوې نسخه)
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import time
import logging
import os
from datetime import datetime

# --- لاګ تنظیم ---
logging.basicConfig(
    filename=r'D:\datawer\elt_log.txt',  
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# --- د فولډر بدلول (تر ټولو مهمه کرښه) ---
os.chdir(r"D:\datawer")
logger.info(f"Working directory بدل شو: {os.getcwd()}")

# --- اصلي ETL ایمپورټ ---
try:
    from enhanced_elt_pipeline import run_elt_pipeline
except Exception as e:
    logger.error(f"enhanced_elt_pipeline.py ایمپورټ نه شو: {e}")
    raise


class AfghanistanETLService(win32serviceutil.ServiceFramework):
    _svc_name_ = "AfghanistanETLService"
    _svc_display_name_ = "افغانستان ETL سرویس - 24/7"
    _svc_description_ = "هر ۶ ساعته د کرنې او روغتیا ډاټا تازه کوي – په اتومات ډول"

    def __init__(self, args):
        super().__init__(args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.running = True

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.running = False
        logger.info("سرویس ودرول شو")

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        self.main()

    def main(self):
        logger.info("=== ETL وینډوز سرویس په بریالیتوب سره پیل شو ===")
        logger.info("هر ۶ ساعته به ETL رن شي (یا بدل یې کړئ لاندې)")

        # لومړی ځل سمدستي رن کړه
        self.run_etl_once()

        # بیا هر ۶ ساعته (21600 ثانیې) – دلته بدل کړئ که غواړئ
        # مثال: هر ۱ ساعته → 3600
        #       هر ۱۲ ساعته → 43200
        #       هره ورځ → 86400
        while self.running:
            time.sleep(21600)   # ۶ ساعته
            if self.running:
                self.run_etl_once()

    def run_etl_once(self):
        try:
            start_time = datetime.now()
            logger.info(">>> ETL پروسه پیل شوه - %s", start_time.strftime("%Y-%m-%d %H:%M:%S"))

            run_elt_pipeline()   # اصلي ETL دلته چلیږي

            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(">>> ETL په بریالیتوب سره پای ته ورسېد – وخت تېر شوی: %s", str(duration))

        except Exception as e:
            logger.error("ETL ناکام شو! خطا: %s", str(e), exc_info=True)


if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(AfghanistanETLService)