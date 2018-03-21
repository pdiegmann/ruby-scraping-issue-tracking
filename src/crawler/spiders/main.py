import urlparse
import scrapy
from dateutil import parser
import re

class IndexSpider(scrapy.Spider):
    name = 'main'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse_issue_list)

    def parse_issue_list(self, response):
        for row in response.css('tr.issue'):
            yield response.follow(urlparse.urljoin(response.url, row.css('td.id > a::attr(href)').extract_first()), self.parse_issue)

        next_page = response.css('li.next > a')
        if next_page is not None:
            yield response.follow(urlparse.urljoin(response.url, next_page.css('::attr(href)').extract_first()), self.parse_issue_list)

    def parse_issue(self, response):
        id = self.remove_html(response.css('h2').extract_first().split('#')[-1])
        author = self.parse_user(response.css('div.subject > p.author > a.user.active'))
        dates = response.xpath('//p[@class="author"]//a[contains(@href, "activity?from=")]').css('::attr(title)').extract()
        date_created = parser.parse(dates[0]).strftime('%Y-%m-%d %H:%M:%S %z').strip() if len(dates) > 0 else None
        date_updated = parser.parse(dates[1]).strftime('%Y-%m-%d %H:%M:%S %z').strip() if len(dates) > 1 else None
        title = self.remove_html(response.css('div.subject > div > h3').extract_first()).strip()
        text = response.css('div.description > div.wiki').extract_first()
        status = self.remove_html(response.css('div.status.attribute > div.value').extract_first())
        priority = self.remove_html(response.css('div.priority.attribute > div.value').extract_first())
        assigned_to = self.parse_user(response.css('div.assigned-to.attribute > div.value > a.user.active').extract_first())
        comments = []

        for comment_raw in response.css('div.journal.has-notes:not(has-details)'):
            comments.append(self.parse_comment(comment_raw))

        yield {
            'id': id,
            'title': title,
            'author': author,
            'status': status,
            'created': date_created,
            'updated': date_updated,
            'priority': priority,
            'assigned-to': assigned_to,
            'text': text,
            'comments': comments
        }

    def parse_comment(self, comment_raw):
        id = comment_raw.css('::attr(id)').extract_first().split('-')[-1]
        author = self.parse_user(comment_raw.css('div > h4 > a.active.user'))
        text = comment_raw.css('div#journal-' + id + '-notes').extract_first()
        dates = comment_raw.xpath('//p[@class="author"]//a[contains(@href, "activity?from=")]').css('::attr(title)').extract()
        date_created = parser.parse(dates[0]).strftime('%Y-%m-%d %H:%M:%S %z').strip() if len(dates) > 0 else None
        date_updated = parser.parse(dates[1]).strftime('%Y-%m-%d %H:%M:%S %z').strip() if len(dates) > 1 else None

        return {
            'id': id,
            'author': author,
            'text': text,
            'created': date_created,
            'updated': date_updated
        }

    def parse_user(self, user_raw):
        try:
            return {
                'id': user_raw.css('::attr(href)').extract_first().split('/')[-1],
                'display-name': self.remove_html(user_raw.extract_first().split('(')[-1])[:-2].strip(),
                'name': self.remove_html(user_raw.extract_first().split('(')[0]).strip()
            }
        except:
            return None

    def remove_html(self, string):
        return re.sub(re.compile('<.*?>'), '', string).strip()
