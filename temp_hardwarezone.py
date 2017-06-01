import requests                                                                                
import lxml.html as PARSER                                                                     
import datetime                                                                                
import scrapy
import re
import json
import os.path
import random
import redis
import hashlib


from dateutil.relativedelta import relativedelta
from scrapy.crawler import CrawlerProcess
from scrapy.http import HtmlResponse
from scrapy.item import Item, Field
from scrapy.utils.serialize import ScrapyJSONEncoder


proxy_list = []
check = 0
r = redis.Redis(host='127.0.0.1',port=6379 )

def get_system_data() :
    system_data = {
    "id": 15001,
    "config_category": "category3",
    "name": "http://news.hse.gov.uk/feed/",
    "account_id": "",
    "platform_id": "6ce32185-a789-11e4-a34f-74867a1157ba",
    "platform": "forum",
    "language_id": None,
    "country_id": "6cb61e10-a789-11e4-a34f-74867a1157ba",
    "tags": "",
    "news_config": "0"
    }
    
    return system_data




def get_past_date(str_days_ago):                                                                                           #convert time to a universal format
    Timenow = datetime.datetime.now()
    TODAY = datetime.date.today()
    splitted = str_days_ago.split(',')
    morning = datetime.datetime.combine(TODAY, datetime.time(0,0))
    print(splitted)

    if splitted[0].lower() == 'today':
        date = datetime.datetime.combine(TODAY,datetime.datetime.strptime(splitted[1], " %I:%M %p").time())- relativedelta(hours= 8)
        return str(date.isoformat()) + ".000Z"
    elif splitted[0].lower() == 'yesterday':
        date = datetime.datetime.combine(TODAY,datetime.datetime.strptime(splitted[1], " %I:%M %p").time()) - relativedelta(days=1,hours= 8)
        return str(date.isoformat()) + ".000Z"
    elif splitted[1].lower() in ['hour', 'hours', 'hr', 'hrs', 'h']:
        date = datetime.datetime.now() - relativedelta(hours=int(splitted[0]))- relativedelta(hours= 8)
        return str(date.date().isoformat()) + ".000Z"
    elif splitted[1].lower() in ['day', 'days', 'd']:
        date = TODAY - relativedelta(days=int(splitted[0]))
        return str(date.isoformat()) + ".000Z"
    elif splitted[1].lower() in ['wk', 'wks', 'week', 'weeks', 'w']:
        date = TODAY - relativedelta(weeks=int(splitted[0]))
        return str(date.isoformat()) + ".000Z"
    elif splitted[1].lower() in ['mon', 'mons', 'month', 'months', 'm']:
        date = TODAY - relativedelta(months=int(splitted[0]))
        return str(date.isoformat()) + ".000Z"
    elif splitted[1].lower() in ['yrs', 'yr', 'years', 'year', 'y']:
        date = TODAY - relativedelta(years=int(splitted[0]))
        return str(date.isoformat()) + ".000Z"
    else:
        date = datetime.datetime.strptime(str_days_ago, "%d-%m-%Y, %I:%M %p") - relativedelta(hours=8)
        return str(date.isoformat()) + ".000Z"



def isXpath(content, xpath):                                                                                               #check whether the Xpath expression exists or not
    root = PARSER.fromstring(content)
    return bool(root.xpath(xpath))

def proxy_lister(status) :
    if status == 1:                                                                                                        #returns a random working proxy id, iF status 1, else returns specific proxy id shown here. 
        lines=open("proxies.txt",'r').readlines()
        global proxy_list  
        for line in lines:
            a11=re.findall(r'\w+',line)
        #    print (re.findall(r'\w+',line))
            k33 = 'https://' + a11[0] + '.' + a11[1] + '.' + a11[2] + '.' + a11[3] + ':' + a11[4] + '/'
            proxy_list.append(k33)
        random_proxy = random.choice(proxy_list)
        response = os.system("ping -c 1 " + random_proxy)
        if response == 0:
            return random_proxy
            print("success")
        else:
            return proxy_lister()
            print("check further")
    else :
        return 'https://113.226.119.3:8080/'                                                                               #VERY SLOW CHANGE THIS ADDRESS IF USING THIS FUNCTION

def md5_hasher(url) :                                                                                                      #returns a md5 hash of string input
    return (hashlib.md5(url.encode('utf-8')).hexdigest())

def id_gen(str_forum) :                                                                                                    #returns the id of the forum, when passed through string
    return '1'


class Website(Item):                                                                                                       #class so we can create a dict with key values on the run
    def __setitem__(self, key, value):
        if key not in self.fields:
            self.fields[key] = Field()
        self._values[key] = value

class MySpider(scrapy.Spider):                                                                                             #Let's crawl
    name            = "CrawlAll"
    allowed_domains = ["forums.hardwarezone.com.sg"]                                                                       #So we do not scrape irrelevant links
    start_urls      = ["http://forums.hardwarezone.com.sg/"]

    def parse(self, response):
        global check 
        itemdictionary = []
        pages = Website()   
        linker = (response.url).lower()
        link = linker.rsplit('/')                                                                                          #Check variable to see if newest page or not
        if '.html' in (response.url).lower():  
            #checks if 1) is on the last page if it has pagination OR 2) does not have pagination or 3) Not last page but not scraped
            if  check ==1 or isXpath(requests.get(response.url).content,'//div[@class="pagination"]//ul/li[last()]/a[not(@href)]') or  not isXpath(requests.get(response.url).content,'//div[@class="pagination"]')  : 
                
                posts_on_page = response.xpath('count(//*[@id="posts"]/div[@class= "post-wrapper"])').extract()[0]
                count = float(posts_on_page)                                                                           #Counter to see which post is being scraped inside particular url  
                hashed_url= md5_hasher(response.url)
                filename = 'data/' + id_gen(linker.rsplit('/')[2]) + '-' + hashed_url  +'.json'                        #Where the files at
                directory = os.path.dirname(filename)
                
                for site in response.xpath('//*[@id="posts"]/div[@class= "post-wrapper"]') :                               #Loop through posts
                    count = count - 1
                    item =Website()                                             
                    
                    pid = site.xpath('./table/@id').extract()[0]                                                    #Extracts post's ID
                    
                    pid = ''.join(x for x in pid if x.isdigit())                                                         
                    
                    item['id'] =  id_gen(linker.rsplit('/')[2])+ '-' + pid
                    old_post_id = r.get(link)                                                                              #Get last stored post's ID. If nothing has been stored returns None
                    
                    
                   
                    
                    try :
                        old_post_id =int(old_post_id)
                    except :
                        old_post_id = 0
                    if pid is not '' :
                        pid_int = int(pid)
                    else :
                        pid_int = 0
                    if (old_post_id< pid_int or (old_post_id == 0) )and link is not None:
                        if count == posts_on_page and isXpath(requests.get(response.url).content,'//div[@class="pagination"]//ul/li/a[contains(text(),"Prev")]'): #If next button exists scrape link attatched
                            print("\n\npageback\n\n")
                            check = 1
                            back_page_url = site.xpath('//div[@class="pagination"]//ul/li/a[contains(text(),"Prev")]/@href').extract()[0]
                            back_page_url = "http://forums.hardwarezone.com.sg" + back_page_url
                            next_url=response.urljoin(back_page_url)
                            req = scrapy.Request(next_url, callback=self.parse)
                            #req.meta['proxy'] = proxy_lister(0)
                            yield req
                        g1 = site.xpath('./table//tr/td[@class="thead" and not(@align = "right")]/text()').extract()[2]
                        g1 = g1.strip()
                        item['link'] = "http://forums.hardwarezone.com.sg" + site.xpath('./table//tr/td[@class="thead" and (@align = "right")]/a/@href').extract()[0]  
                        try :
                            item['author']                     = site.xpath('.//*/td[@class = "alt2"]//*/a[@class= "bigusername"]/text()').extract()[0]
                        except :
                            item['author']                     = None
                        try :
                            item['author_desc']                     = site.xpath('.//*/td[@class = "alt2"]//*[@class= "smallfont"][1]/text()').extract()[0]
                        except :
                            item['author_desc']                     = None
                        try :
                            g4                                 = site.xpath('.//*/td[@class = "alt2"]/div[last()]/div[1]/text()').extract()[0]
                            item['author_join_date']                = (g4.rsplit(':')[1]).strip()
                        except :
                            item['author_join_date']                = None
                        try :
                            g5                                 = site.xpath('.//*/td[@class = "alt2"]/div[last()]/div[2]/text()').extract()[0]    
                            item['author_posts']                    = (g5.rsplit(':')[1]).strip()
                        except :
                            item['author_posts']                    = None
                        try : 
                            g11                                = site.xpath('.//*/td[@class = "alt2"]/div[last()-1]//img/@src').extract()[0]   
                            item['author_avatar']                   = "http://forums.hardwarezone.com.sg" + g11
                        except :
                            item['author_avatar']                   = None
                        try :
                            g10                                = site.xpath('.//*/td[@class = "alt2"]//*/a[@class= "bigusername"]/@href').extract()[0]
                            item['author_link']                     = "http://forums.hardwarezone.com.sg" + g10
                        except :
                            item['author_link']                     = None
                        try :    
                            item['enclosure']                     = site.xpath('.//*/td[@class = "alt1"]/div//img/@src').extract()[0]
                        except :
                            item['enclosure']                     = None
                        try :    
                            item['title']                      =site.xpath('.//*/td[@class = "alt1"]/div[1]/strong/text()').extract()[0]
                        except :
                            item['title']                      = None
                        try :    
                            g6                                 = site.xpath('.//*/td[@class = "alt1"]/div//a/@href').extract()[0]
                            item['in_text_links']              = "http://forums.hardwarezone.com.sg" + g7
                        except :
                            item['in_text_links']              = None
                        try :    
                            g7                                 = site.xpath('.//*/td[@class = "alt1"]/div/text()').extract()
                            item['summary']                    = (''.join(g7)).strip()
                        except :
                            item['summary']                    = None
                        try :
                            g8                                 = site.xpath('.//*/td[@class = "alt1"]//*/div[@class="quote"]//*/text()').extract() 
                            g8                                 = (''.join(g8)).strip()
                            if g8 == '' :  
                                item['Quote_in_item']          = None
                            else :
                                item['Quote_in_item']          = g8

                        except :
                            item['Quote_in_post']              = None
                        try :    
                            g9                                 = site.xpath('.//*/td[@class = "alt1"]//*/div[@class="quote"]//*/a/@href').extract()[0]
                            item['Quote_source']               = "http://forums.hardwarezone.com.sg" + g9
                        except :
                            item['Quote_source']               = None
                        try :    
                            item['Post_likes']                 = site.xpath('count(.//*/td[@class = "alt1"]//*/div[@class="alt2 vbseo_liked vbseo_like_own"]//a)').extract()[0]
                        except :
                            item['Post_likes']                 = None
                        try : 
                            item['pubdate']                    = get_past_date(g1)
                        except:
                            item['pubdate']                    = None
                        itemdictionary.append(dict(item))
                        if check == 0 and count == posts_on_page :
                            r.set(link,pid) 
                    else :
                        check =0   
                    try:
                        os.stat(directory)
                    except:
                        os.mkdir(directory)
                pages['system_info'] = get_system_data()
                try :
                    pages['data'] = itemdictionary 
                except :
                    page['data'] = None
                entry = dict(pages)
                print("\n\n\n\ngonnawrite\n\n")
                with open(filename, 'w', encoding='utf-8') as outfile:
                    json.dump(entry, outfile, ensure_ascii = True ) 
                    



#did not use elif because they made threadlists inline

        elif (isXpath(requests.get(response.url).content,'//*[@id = "threadslist"]') or isXpath(requests.get(response.url).content,'//div[@id="forum"]')):
            if isXpath(requests.get(response.url).content,'//*[@id = "threadslist"]'):
                
                for href in response.xpath('//*[@id = "threadslist"]//tr'): 
                    thread_id = href.xpath('./td[3]/@id').extract()
                                      
                    if thread_id != [] :
                        try :
                            thread_id = thread_id[0]
                        except :
                            pass

                        try :
                            q = href.xpath('./td[@class="alt1"]/a/text()').extract()[0]
                            no_of_replies = ''.join(x for x in q if x.isdigit())
                        except :
                            no_of_replies   = ''

                        try :
                            q = href.xpath('./td[last()]/text()').extract()[3]
                            no_of_views = ''.join(x for x in q if x.isdigit())
                        except :
                            no_of_views = ''

                        try :
                            page_url = href.xpath('./td[@title]/div/a[2]/@href').extract()[0]
                            page_url = "http://forums.hardwarezone.com.sg" + page_url
                        except :
                            page_url = ''
                        old_posts = r.get(thread_id)
                        try :
                            old_posts =int(old_posts)
                        except :
                            old_posts = 0
                        if no_of_replies is not '' :
                            no_of_replies_int = int(no_of_replies)
                        else :
                            no_of_replies_int = 0
                        
                        #r.set(thread_id,no_of_replies) Use instead of above line if first execution
                        print(old_posts)
                        print(no_of_replies_int)
                        print(thread_id)
                        print(page_url)
                        if (old_posts< no_of_replies_int or (old_posts == 0)) and page_url is not '':
                            next_url=response.urljoin(page_url)
                            r.set(thread_id,no_of_replies)
                            req = scrapy.Request(next_url, callback=self.parse)
                            #req.meta['proxy'] = proxy_lister(0)
                            yield req                   
                if isXpath(requests.get(response.url).content,'//div[@class="pagination"]/ul/li[@class = "prevnext"]/a[contains(text(),"Next")]/@href'):

                    back_page_url = response.xpath('//div[@class="pagination"]/ul/li[@class = "prevnext"]/a[contains(text(),"Next")]/@href').extract()[0]
                    back_page_url = "http://forums.hardwarezone.com.sg" + back_page_url
                    next_url=response.urljoin(back_page_url)
                    req = scrapy.Request(next_url, callback=self.parse)
                    #req.meta['proxy'] = proxy_lister(0)
                    yield req




            if isXpath(requests.get(response.url).content,'//div[@id="forum"]') :
                for href in response.xpath('//div[@id="forum"]/table[3]//tr'):
                    threadlist_id = href.xpath('./td/@id').extract()
                    if threadlist_id != [] :
                        try :
                            threadlist_id = threadlist_id[0]
                            q = href.xpath('./td[last()-1]/text()').extract()[0]
                            no_of_threads = ''.join(x for x in q if x.isdigit())
                            q = href.xpath('./td[last()]/text()').extract()[0]
                            no_of_posts = ''.join(x for x in q if x.isdigit())
                            page_url = href.xpath('.//*[@class= "alt1Active"]//a/@href').extract()[0]
                            page_url = "http://forums.hardwarezone.com.sg" + page_url
                            
                        except :
                            no_of_threads = ''
                            no_of_posts   = ''
                            page_url = ''
                        old_links = r.get(threadlist_id)
                        try :
                            old_links =int(old_links)
                        except :
                            old_links = 0
                        if no_of_posts is not '' :
                            no_of_posts_int = int(no_of_posts)
                        else :
                            no_of_posts_int = 0
                        
                        #r.set(threadlist_id,no_of_posts) Use instead of above line if first execution
                        if (old_links<no_of_posts_int or (old_links == 0)) and threadlist_id is not None:
                            next_url=response.urljoin(page_url)
                            r.set(threadlist_id,no_of_posts)
                            req = scrapy.Request(next_url, callback=self.parse)
                            #req.meta['proxy'] = proxy_lister(0)
                            yield req
                            
        else :
            for href in response.xpath('//a/@href').extract():
                next_url=response.urljoin(href)
                req = scrapy.Request(next_url, callback=self.parse)
                
                req.meta['proxy'] = proxy_lister(0)
                yield req        
#deleted random else statement because it was scraping too many links at once


        

                    

process = CrawlerProcess({                                                                                
'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(MySpider)                                                                                                       #
process.start()