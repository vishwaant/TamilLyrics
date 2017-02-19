# -*- coding: utf-8 -*-


import luigi
import time,sys,json,os
import datetime,httplib,urllib2
import string,requests
from bs4 import BeautifulSoup
import pickle


output_folder = "../../../Data/Lyrics/Tamil"

class PrepareEnv(luigi.Task):
	
	def run(self):
		if not os.path.exists(output_folder): os.makedirs(output_folder)

	def output(self):
		return luigi.LocalTarget(path=output_folder)



class GetLyricsByLetter(luigi.Task):
	
	def requires(self):
		return PrepareEnv()
	
	def run(self):
		url_prefix="http://www.paadalvarigal.com/"
		film_suffix="films/"
		film_prefix_list = [alpha for alpha in string.lowercase]
		print "Run this"

class GetSongLyrics(luigi.Task):

	date = luigi.DateParameter()

	def requires(self):
		return GetMoviesByLetter(self.date)

	def run(self):
		movie_info = {}
		#pickle_file = output_folder+'/movie_name_urls_{}.p'.format(self.date)
		film_prefix_list = ['http://www.paadalvarigal.com/1915/tharai-erangiya-eeram-song-lyrics.html']
		for film_alpha in film_prefix_list:
			url = film_alpha
			response = requests.get(url)
			soup = BeautifulSoup(response.text,"html5lib")
			divcenter = soup.findAll('div',{"class":"entry clearfix"})
			header = soup.findAll('header',{"class":"post-header"})
			for head in header:
				print head
			for div in divcenter:
				print div.text
		return True

	def output(self):
		return luigi.LocalTarget(path=output_folder+'/eeram')

class GetMoviesInformation(luigi.Task):


	date = luigi.DateParameter()

	def requires(self):
		return GetMoviesByLetter(self.date)

	def run(self):
		movie_info = {}
		#pickle_file = output_folder+'/movie_name_urls_{}.p'.format(self.date)
		film_prefix_list = ['http://www.paadalvarigal.com/v-films/villu']
		for film_alpha in film_prefix_list:
			url = film_alpha
			response = requests.get(url)
			soup = BeautifulSoup(response.text,"html5lib")
			divcenter = soup.findAll('div',{"class":"entry clearfix"})
			for div in divcenter:
				td_list = [ tr.findAll('td') for table in div.findAll('table') for tr in table.findAll('tr')]
				for td in td_list:
					print td
					#print BeautifulSoup(tr,"lxml").findAll('tr')
#				print [td for td in BeautifulSoup(tr_list).findAll('td')]
		#				movie_info['MusicDirector']=
	#					movie_info['Actors']=
	#					movie_info['Director']=
#						movie_info['Producer']=
						#movie_info['Writer']=
						#movie_info['ReleaseDate']=
						#movie_info['MoreDetails']=
				print movie_info
		#		ul = div.findAll('ul')
	#			for unlist in ul:
#					for anchr in unlist.findAll('a'):
#						print anchr['href'],anchr.text
#						movie_urls[anchr.text.replace(" ","-")]=anchr['href']
#				pickle.dump(movie_urls,open(pickle_file,"a"))
		return True

	def output(self):
		return luigi.LocalTarget(path=output_folder+'/villu')



class GetMoviesByLetter(luigi.Task):

	date = luigi.DateParameter()

	def requires(self):
		return PrepareEnv()

	def run(self):
		movie_urls = {}
		pickle_file = output_folder+'/movie_name_urls_{}.p'.format(self.date)
		url_prefix="http://www.paadalvarigal.com/"
		film_suffix="films/"
		film_prefix_list = [alpha for alpha in string.lowercase]
		for film_alpha in film_prefix_list:
			url = url_prefix+film_alpha+"-"+film_suffix
			response = requests.get(url)
			soup = BeautifulSoup(response.text,"html5lib")
			divcenter = soup.findAll('div',{"class":"entry clearfix"})
			for div in divcenter:
				ul = div.findAll('ul')
				for unlist in ul:
					for anchr in unlist.findAll('a'):
						print anchr['href'],anchr.text
						movie_urls[anchr.text.replace(" ","-")]=anchr['href']
				pickle.dump(movie_urls,open(pickle_file,"a"))
		return True

	def output(self):
		return luigi.LocalTarget(path=output_folder+'/movie_name_urls_{}.p'.format(self.date))


class GetMoviesByNumber(luigi.Task):
	
	def requires(self):
		return PrepareEnv()

	def getLyrics(self,url):
		response = requests.get(url)
		print response.text

	def getMovieNames(self,url):
		response = requests.get(url)
		print response.text

	def run(self):
		url_prefix="http://www.paadalvarigal.com/"
		film_suffix="films/"
		film_prefix_list = [alpha for alpha in string.lowercase]
		print "Run this"

class CheckNewMovies(luigi.Task):
	
	def requires(self):
		return PrepareEnv()

	def getLyrics(self,url):
		response = requests.get(url)
		print response.text

	def getMovieNames(self,url):
		response = requests.get(url)
		print response.text

	def run(self):
		url_prefix="http://www.paadalvarigal.com/"
		film_suffix="films/"
		film_prefix_list = [alpha for alpha in string.lowercase]
		print "Run this"

if __name__ == "__main__":
    luigi.run()
