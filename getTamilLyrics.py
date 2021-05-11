# -*- coding: utf-8 -*-
import luigi
import time,sys,json,os
import datetime,httplib,urllib2
import string,requests
from bs4 import BeautifulSoup
import pickle
import os


output_folder = "../../../Data/Lyrics/Tamil"

class PrepareEnv(luigi.Task):
	#execute first to create dirs	
	def run(self):
		if not os.path.exists(output_folder): os.makedirs(output_folder)

	def output(self):
		return luigi.LocalTarget(path=output_folder)


class GetSongLyrics(luigi.Task):

	#this should be the last thing to run as of now fourth
	song_url = luigi.Parameter()
	movie_name = luigi.Parameter()
	song_name = luigi.Parameter()

	def run(self):
		lyrics_file = output_folder+'/'+self.movie_name+"_"+self.song_name+".p"
		info_file = output_folder+'/'+self.movie_name+"_"+self.song_name+".p"
		song_info = {}
		response = requests.get(self.song_url)
		soup = BeautifulSoup(response.text,"html5lib")
		divcenter = soup.find_all('div',{"class":"entry clearfix"})
		header = soup.find_all('header',{"class":"post-header"})
		for head in header:
			for p in head.find_all('p'):
				song_info['MusicLyricsSinger']=p.text.split("|")
		lyrics = [x.text for lyrics in divcenter for x in lyrics.find_all('p')[:-1]]
#		print lyrics
		pickle.dump(lyrics,open(lyrics_file,"w"))
		#print song_info

	def output(self):
		return luigi.LocalTarget(path=output_folder+'/'+self.movie_name+"_"+self.song_name+".p")



class GetMoviesInformation(luigi.WrapperTask):
	#third run

	date = luigi.DateParameter()
	letter = luigi.Parameter()

	def requires(self):
		movie_info = {}
		pickle_file = output_folder+'/movie_name_urls_{}_{}.p'.format(self.date,self.letter)
#		print pickle_file 
		with open(pickle_file,"r") as pkl:
			film_prefix_dict = pickle.load(pkl)
#		print film_prefix_dict
#		sys.exit(1)
		#film_prefix_list = ['http://www.i1paadalvarigal.com/v-films/villu']
		for k,film_alpha in film_prefix_dict.iteritems():
			mname = k
			print mname
			url = film_alpha
			response = requests.get(url)
			soup = BeautifulSoup(response.text,"html5lib")
			divcenter = soup.find_all('div',{"class":"entry clearfix"})
			all_songs = [i.find_all('a') for i in  soup.find_all('nav',{"class":"info-nav clearfix"})][0]
			song_info = [(song["title"].replace(" ","-"),song["href"]) for song in all_songs]
			for each_song in song_info:
				print each_song
				yield GetSongLyrics(song_url=each_song[1],movie_name=mname,song_name=each_song[0])

#			for div in divcenter:
#				td_list = [ tr.find_all('td') for table in div.find_all('table') for tr in table.find_all('tr')]
#				for td in td_list:
#					print td
					#print BeautifulSoup(tr,"lxml").find_all('tr')
#				print [td for td in BeautifulSoup(tr_list).find_all('td')]
		#				movie_info['MusicDirector']=
	#					movie_info['Actors']=
	#					movie_info['Director']=
#						movie_info['Producer']=
						#movie_info['Writer']=
						#movie_info['ReleaseDate']=
						#movie_info['MoreDetails']=
#				print movie_info
#				pickle.dump(movie_urls,open(pickle_file,"a"))
#		return True




class GetMoviesByLetter(luigi.Task):

	##Second run this

	date = luigi.DateParameter()
	#letter = luigi.Parameter()

	def requires(self):
		return PrepareEnv()

	def run(self):
		movie_urls = {}
		url_prefix="http://www.paadalvarigal.com/"
		film_suffix="films/"
		film_prefix_list = [alpha for alpha in string.lowercase]
		#film_prefix_list = self.letter.split(",")
		for film_alpha in film_prefix_list:
			movie_urls = {}
			pickle_file = output_folder+'/movie_name_urls_{}_{}.p'.format(self.date,film_alpha)
			url = url_prefix+film_alpha+"-"+film_suffix
			response = requests.get(url)
			soup = BeautifulSoup(response.text,"html5lib")
			divcenter = soup.find_all('div',{"class":"entry clearfix"})
			for div in divcenter:
				ul = div.find_all('ul')
				for unlist in ul:
					for anchr in unlist.find_all('a'):
						print anchr['href'],anchr.text
						movie_urls[anchr.text.replace(" ","-")]=anchr['href']
			with open(pickle_file,"wb+") as pkl:
				pickle.dump(movie_urls,pkl)
		#print movie_urls

	def output(self):
		return [luigi.LocalTarget(path=output_folder+'/movie_name_urls_{}_'+alpha+'.p'.format(self.date)) for alpha in string.lowercase]


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
