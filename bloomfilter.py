# Python 3 program to build Bloom Filter
# Install mmh3 and bitarray 3rd party module first
# pip install mmh3
# pip install bitarray
import math
import mmh3
from bitarray import bitarray


class BloomFilter(object):

	'''
	Class for Bloom filter, using murmur3 hash function
	'''

	def __init__(self, items_count=0, fp_prob=0, fixed_size=0, fixed_hash_count=0):
		'''
		items_count : int
			Number of items expected to be stored in bloom filter
		fp_prob : float
			False Positive probability in decimal
		'''
		if items_count == 0:
			assert(fixed_size > 0)
			assert(fixed_hash_count > 0)
			self.size = fixed_size
			self.hash_count = fixed_hash_count
		else:
			# False possible probability in decimal
			self.fp_prob = fp_prob

			# Size of bit array to use
			self.size = self.get_size(items_count, fp_prob)

			# number of hash functions to use
			self.hash_count = self.get_hash_count(self.size, items_count)

		# Bit array of given size
		self.bit_array = bitarray(self.size)

		# initialize all bits as 0
		self.bit_array.setall(0)

	def add(self, item):
		'''
		Add an item in the filter
		'''
		digests = []
		for i in range(self.hash_count):

			# create digest for given item.
			# i work as seed to mmh3.hash() function
			# With different seed, digest created is different
			digest = mmh3.hash(str(item), i) % self.size
			digests.append(digest)

			# set the bit True in bit_array
			self.bit_array[digest] = True

	def check(self, item):
		'''
		Check for existence of an item in filter
		'''
		for i in range(self.hash_count):
			digest = mmh3.hash(str(item), i) % self.size
			if self.bit_array[digest] == False:

				# if any of bit is False then,its not present
				# in filter
				# else there is probability that it exist
				return False
		return True

	def estimatedSize(self):
		'''
		Estimate the size of the set represented by this BF
		'''
		t = 0
		for i in range(1,self.size):
			if self.bit_array[i] == True:
				t = t + 1
#		print(t, self.size, self.hash_count)
		return int(-1 * (self.size / self.hash_count) * math.log(1 - t/self.size))

	def intersection(self, bf):
		'''
		Return the intersection of two Bloom filters
		'''
		assert(self.size == bf.size)
		intersectionBF = BloomFilter(0,0,self.size, self.hash_count)

		intersectionBF.bit_array = self.bit_array & bf.bit_array

		# for i in range(self.size):
		# 	if self.bit_array[i] and bf.bit_array[i]:
		# 		intersectionBF.bit_array[i] = True

		return intersectionBF
				
	def union(self, bf):
		'''
		Return the union of two Bloom filters
		'''
		assert(self.size == bf.size)
		unionBF = BloomFilter(0,0,self.size, self.hash_count)

		unionBF.bit_array = self.bit_array | bf.bit_array
		
		# for i in range(self.size):
		# 	if self.bit_array[i] or bf.bit_array[i]:
		# 		unionBF.bit_array[i] = True

		return unionBF
				
	@classmethod
	def get_size(self, n, p):
		'''
		Return the size of bit array(m) to used using
		following formula
		m = -(n * lg(p)) / (lg(2)^2)
		n : int
			number of items expected to be stored in filter
		p : float
			False Positive probability in decimal
		'''
		m = -(n * math.log(p))/(math.log(2)**2)
		return int(m)

	@classmethod
	def get_hash_count(self, m, n):
		'''
		Return the hash function(k) to be used using
		following formula
		k = (m/n) * lg(2)

		m : int
			size of bit array
		n : int
			number of items expected to be stored in filter
		'''
		k = (m/n) * math.log(2)
		return int(k)

	@classmethod
	def get_fp_prob(self, k, m, n):
		'''
		Return the estimated probability for false positives
		using the formula
		fp = (1 - e^(-kn/m))^k

		k : int
      number of hash functions
		m : int
			size of bit array
		n : int
			number of items expected to be stored in filter
		'''
		fp = pow((1 - math.exp(-k * m / n)), k)
		return fp
	
