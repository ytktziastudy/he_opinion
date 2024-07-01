import apache_beam as beam
import aiohttp
import asyncio
from bs4 import BeautifulSoup, SoupStrainer
import json
import re
import sys
import typing


DISALLOWED_TAGS = set(['script'])


def scrape(argv):
    with beam.Pipeline(argv=argv) as p:
        links = crawl_for_links(p
                                | beam.Create(['https://beam.apache.org']),
                                depth=5)
        words = (links | beam.FlatMap(
            lambda l: get_words_sync([l])) | beam.Distinct())
        (words
         | beam.Map(len)
         | beam.combiners.Count.PerElement()
         | beam.Map(lambda x: json.dumps({'length': x[0], 'frequency': x[1]}))
         | 'write_histograms' >> beam.io.WriteToText('beam_wordle_histogram.txt'))
        words | beam.io.WriteToText('beam_wordle_words.txt')


def crawl_for_links(first_link_pc: beam.PCollection[str], depth=2) -> beam.PCollection[str]:
    all_links = [first_link_pc]
    degrees = first_link_pc | 'fetch_first' >> beam.FlatMap(
        lambda link: get_links_sync([link]))
    all_links.append(degrees)
    for i in range(depth):
        degrees = degrees | ('fetch_%s' % (
            i+1)) >> beam.FlatMap(lambda link: get_links_sync([link]))
        all_links.append(degrees)
    return all_links | beam.Flatten() | 'unique_pages' >> beam.Distinct()


def get_links_sync(links: typing.List[str]) -> typing.List[str]:
    return asyncio.run(get_links(links))


async def get_links(links: typing.List[str]):
    result = []
    async with aiohttp.ClientSession() as session:
        for url in links:
            async with session.get(url) as resp:
                for link in BeautifulSoup(await resp.text(), parse_only=SoupStrainer('a')):
                    if link.has_attr('href') and '://beam.apache.org' in link['href']:
                        result.append(link['href'])
    return result


def get_words_sync(links: typing.List[str]) -> typing.List[str]:
    return asyncio.run(get_words(links))


async def get_words(links: typing.List[str]) -> typing.List[str]:
    result = []
    async with aiohttp.ClientSession() as session:
        for url in links:
            async with session.get(url) as resp:
                soup = BeautifulSoup(await resp.text())
                for t in soup.find_all(text=True):
                    if t.parent.name in DISALLOWED_TAGS:
                        continue
                    for word in re.findall('[\w-]+', t.getText()):
                        result.append(word)

    return result


if __name__ == '__main__':
    scrape(sys.argv)
