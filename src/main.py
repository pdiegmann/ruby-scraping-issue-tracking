import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import jsonlines
import os
import shutil
import datetime

data_folder = os.path.join('..', 'data')
archive_folder = os.path.join('..', 'archive')
process = CrawlerProcess(get_project_settings())

def main():
    print "starting"

    if os.path.exists(data_folder) and os.listdir(data_folder) != []:
        shutil.move(data_folder, os.path.join(archive_folder, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')))

    process.crawl('main', start_urls=['https://bugs.ruby-lang.org/projects/ruby-trunk/issues?&set_filter=1&sort=id%3Adesc&f%5B%5D=&c%5B%5D=tracker&c%5B%5D=status&c%5B%5D=subject&c%5B%5D=assigned_to&c%5B%5D=updated_on&group_by='])
    process.start()

if __name__ == "__main__":
    main()
