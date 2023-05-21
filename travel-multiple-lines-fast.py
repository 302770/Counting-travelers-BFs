from bloomfilter import BloomFilter
import random
import numpy
import sys
import time

NUM_LOCATIONS	 = int(sys.argv[1])								 # Size of the network
PROB_LINK			 = float(int(sys.argv[2])/100)		 # Probability of making a link between two nodes (in %)
NUM_TRIPS			 = int(sys.argv[3])								 # Total number of trips in the network
EPOCH_LENGTH	 = int(sys.argv[4])								 # Epoch length in minutes
MAX_DETECTIONS = int(sys.argv[5])								 # Maximum number of detections/epoch supported (global)
PROB_BF_FALSE	 = float(int(sys.argv[6])/10000.0) # Tolerated false positives in Bloom filters (in %%%)
USE_SETS       = int(sys.argv[7])==1             # Decide whether we're going to use sets or Bloom filters
NUM_RUNS       = int(sys.argv[8])                # Number of runs for the same set of parameter values

MAX_TRAVELERS	 = 10000000		 # Maximum number of traveler IDs to generate
START_OF_DAY	 = 5*60				 # Start time of first trip in minutes
END_OF_DAY		 = 24*60			 # Last time that we can have an arrival (in minutes)
LASTDEP_OUT		 = 22*60			 # Last possible departure time at A (still allowing a return trip)
LASTDEP_RET		 = 23*60			 # Last possible departure time at B (still guaranteeing to arrive)
PROB_RETURN		 = 0  				 # Probability that someone will also make a return trip
MIN_TRIPTIME	 = 15  				 # Minimum trip time
MAX_TRIPTIME	 = 30					 # Maximum trip time 
STD_TRIPTIME	 = 0.2				 # Standard deviation expressed in fraction of average trip time
numOfReturners = 0					 # Ground truth when it comes to returners

BFLEN			= 1000		 # Used to fix the length of all Bloom filters (overrules n,p)
NHASH			= 3				 # Used to fix the number of hash functions (overrules n,p)

TID = 0 # Index of traveler ID in a trip
LNK = 1 # Index of link in a trip
DEP = 2 # Index of departure epoch in a trip
ARR = 3 # Index of arrival epoch in a trip

SRC = 0 # Index of the source in a link
DST = 1 # Index of the destination in a link
AVG = 2 # Index of the average trip time in a link
STD = 3 # Index of the standard deviation in a link

def accuracy(real_count, measured_count):
	return max(1 - abs(real_count - measured_count)/real_count, 0)

def epoch(time):
	# Simply return the epoch when given time in minutes
	return int(time/EPOCH_LENGTH)

# We store the network as a collection of directed links, each having a source, destination,
# average trip time, with a specific standard deviation.

outLinks = [[] for i in range(NUM_LOCATIONS)]

# Find the specific link that connects src to dst
def findLink(src, dst):
	outLinksSrc = outLinks[src]
	for l in outLinksSrc:
		if l[DST] == dst:
			return l
	return []

# We want to compute the total number of commuters in a network. To this end, we store detections
# in either sets or Bloom filters. Those sets are ordered, per location, by departure epochs. In
# principle, we can compute the number of commuters per (src,dst) pair. To compute all commuters,
# we build a list of detections per epoch, taking all locations together. This will allow a faster
# computation in comparison to doing this on a per (src,dst)-pair basis.
tripSetBFsLoc = [[BloomFilter(MAX_DETECTIONS, PROB_BF_FALSE) \
									for epochs in range(epoch(END_OF_DAY))] for locations in range(NUM_LOCATIONS)] 
tripSetSetLoc = [[set() \
									for epochs in range(epoch(END_OF_DAY))] for locations in range(NUM_LOCATIONS)]
tripSetBFs = [BloomFilter(MAX_DETECTIONS, PROB_BF_FALSE) for epochs in range(epoch(END_OF_DAY))]
tripSetSet = [set() for epochs in range(epoch(END_OF_DAY))]

if USE_SETS:
	tripSetLoc = tripSetSetLoc # Detections per location, per epoch
	tripSet    = tripSetSet    # Detections per epoch
else:
	tripSetLoc = tripSetBFsLoc # Detections per location, per epoch
	tripSet    = tripSetBFs    # Detections per epoch

# We construct a random undirected network with asymmetric travel times. We optimistically assume
# that the network will be connected, which is true for a reasonably chosen PROB_LINK.
def generateNetwork():
	for node1 in range(NUM_LOCATIONS):
		for node2 in range(node1 + 1, NUM_LOCATIONS):
			if random.random() < PROB_LINK: # construct two links between node1 and node2
				avgtravel = random.randint(MIN_TRIPTIME, MAX_TRIPTIME)
				stdtravel = int(STD_TRIPTIME * avgtravel)
				outLinks[node1].append((node1, node2, avgtravel, stdtravel))
				avgtravel = random.randint(MIN_TRIPTIME, MAX_TRIPTIME)
				stdtravel = int(STD_TRIPTIME * avgtravel)
				outLinks[node2].append((node2, node1, avgtravel, stdtravel))
	print("Network generated")

# Given the number of required trips, we construct a trip from a randomly chosen node to one
# of its neighbors, and with PROB_RETURN probability, also a return trip. All trips get a
# guaranteed unique ID.
def generateTrips():
	global numOfReturners
	global tripSet
	travelerIDSet = random.sample(range(MAX_TRAVELERS), NUM_TRIPS)
	tripSetRaw = set([])
	
	# Generate random trips from one location to another
	for trip in range(1, NUM_TRIPS):
		travelerID		 = travelerIDSet[trip]
		outwardSrc		 = random.randint(0, NUM_LOCATIONS - 1)
		outwardLink		 = random.sample(outLinks[outwardSrc], k = 1)[0]
		depTimeOutward = random.randint(START_OF_DAY, LASTDEP_OUT)
		arrTimeOutward = depTimeOutward + round(numpy.random.normal(outwardLink[AVG], outwardLink[STD]))

		assert(epoch(depTimeOutward) <= epoch(arrTimeOutward))
		tripSetRaw.add((travelerID, outwardLink, epoch(depTimeOutward), epoch(arrTimeOutward)))

		# Check if this traveler is going back
		if random.random() < PROB_RETURN and arrTimeOutward < LASTDEP_RET:
			returnSrc	    = outwardLink[DST]
			returnLink    = findLink(returnSrc, outwardSrc)
			depTimeReturn = random.randint(arrTimeOutward, LASTDEP_RET)
			arrTimeReturn = depTimeReturn + round(numpy.random.normal(returnLink[AVG], returnLink[STD]))

			assert(epoch(depTimeReturn) <= epoch(arrTimeReturn))
			tripSetRaw.add((travelerID, returnLink, epoch(depTimeReturn), epoch(arrTimeReturn)))
			numOfReturners = numOfReturners + 1

	# Finally, construct lists of trips ordered by detection time
	tripSetSorted = list(tripSetRaw)
	tripSetSorted.sort(key = lambda a: a[DEP])

	for trip in tripSetSorted:
		tripSetLoc[trip[LNK][SRC]][trip[DEP]].add(trip[TID])
		tripSetLoc[trip[LNK][DST]][trip[ARR]].add(trip[TID])

	# And aggregate all trips into a single list, ordered by epoch
	for e in range(epoch(END_OF_DAY)):
		for loc in range(NUM_LOCATIONS):
			tripSet[e] = tripSet[e].union(tripSetLoc[loc][e])
	print("Trips generated")	
	return 

def expectedArrEpochs(epochDep):
	# Returns a conservative range of epochs to consider. For aggregating trips, the best we know is
	# that a trip lasts at least MIN_TRIPTIME minutes, and at most MAX_TRIPTIME minutes. We also need
	# to take the maximum possible standard deviation into account.
	# We assume that a trip takes at least 1 epoch from src to dst.
	maxStd   = STD_TRIPTIME * MAX_TRIPTIME
	minEpoch = min(max(epoch(epochDep * EPOCH_LENGTH + MIN_TRIPTIME - 2 * maxStd), \
										 epochDep + 1), epoch(END_OF_DAY))
	maxEpoch = max(min(epoch((epochDep + 1) * EPOCH_LENGTH + MAX_TRIPTIME + 2 * maxStd), \
										 epoch(END_OF_DAY)), epoch(START_OF_DAY))
	assert(epochDep <= minEpoch)
	assert(maxEpoch <= epoch(END_OF_DAY))
#	return range(epochDep+1, epoch(END_OF_DAY)) # Assume every trip lasts at least an epoch
	return range(minEpoch, maxEpoch)

def findOneWayTrips(epochDep, epochArr):
	# Find all trips departing at epochDep and arriving at epochArr
	global tripSet
	
	tripsFromSrc  = tripSet[epochDep]
	tripsToDst	  = tripSet[epochArr]
	oneWayTripSet = tripsFromSrc.intersection(tripsToDst)

	return oneWayTripSet

def findTwoWayTrips(epochDepSrc, epochArrDst, epochDepDst, epochArrSrc):
	# Find all two-way trips consisting of a trip departing at epochDepSrc, arriving at
	# epochArrDst, and later departing at epochDepDst and arriving at epochArrSrc.
	outwardTrips	= findOneWayTrips(epochDepSrc, epochArrDst)
	returnTrips		= findOneWayTrips(epochDepDst, epochArrSrc)
	twoWayTrips		= outwardTrips.intersection(returnTrips)

	# We count trips in two ways: by returning the size of the found intersections (and adding
	# these to a final result), as well as computing the entire set of trips and then taking the
	# size of that set. Using Bloom filters, the second method should be less accurate.
	if USE_SETS:
		return twoWayTrips, len(twoWayTrips)
	else:
		return twoWayTrips, twoWayTrips.estimatedSize()

def findCommuters(epochDepSrc):
	# Find all commuters who left during epochDepSrc

	print(">>>>>", epochDepSrc)

	if USE_SETS:
		commuterSet = set()
	else:
		commuterSet = BloomFilter(MAX_DETECTIONS, PROB_BF_FALSE)

	estSize = 0 # The aggregated estimated size by adding the number of twoway trips
	for epochArrDst in expectedArrEpochs(epochDepSrc):
		# Assume a return trip never starts in the same epoch as its arrival.
		for epochDepDst in range(epochArrDst + 1, epoch(LASTDEP_RET)):
			for epochArrSrc in expectedArrEpochs(epochDepDst):
				twoWayTripSet, size = findTwoWayTrips(epochDepSrc, epochArrDst, epochDepDst, epochArrSrc)
				estSize             = estSize + size
				commuterSet		      = commuterSet.union(twoWayTripSet)
	return commuterSet, estSize
					
def findAllCommuters():
	if USE_SETS:
		commuterSet = set()
	else:
		commuterSet = BloomFilter(MAX_DETECTIONS, PROB_BF_FALSE)
		
	estSize    = 0 # The aggregated estimated size by adding the number of twoway trips
	estSizeSrc = 0 # The aggregated size of the commuterset starting from a specific epoch 
	for epochDepSrc in range(epoch(START_OF_DAY), epoch(END_OF_DAY)):
		commuterSetSrc, size = findCommuters(epochDepSrc)
		estSize              = estSize + size
		if USE_SETS:
			estSizeSrc         = estSizeSrc + len(commuterSetSrc)
		else:
			estSizeSrc         = estSizeSrc + commuterSetSrc.estimatedSize()
		commuterSet          = commuterSet.union(commuterSetSrc)
	return commuterSet, estSizeSrc, estSize

def findSingleTrips(epochDepSrc):
	# Find all single trips who left during epochDepSrc

	if USE_SETS:
		commuterSet = set()
	else:
		commuterSet = BloomFilter(MAX_DETECTIONS, PROB_BF_FALSE)

	estSize = 0 # The aggregated estimated size by adding the number of single trips
	for epochArrDst in expectedArrEpochs(epochDepSrc):
		oneWayTripSet = findOneWayTrips(epochDepSrc, epochArrDst)
		if USE_SETS:
			estSize     = estSize + len(oneWayTripSet)
		else:
			estSize     = estSize + oneWayTripSet.estimatedSize()
		commuterSet		= commuterSet.union(oneWayTripSet)
	return commuterSet, estSize
					
def findAllSingleTrips():
	if USE_SETS:
		commuterSet = set()
	else:
		commuterSet = BloomFilter(MAX_DETECTIONS, PROB_BF_FALSE)
		
	estSize    = 0 # The aggregated estimated size by adding the number of single trips
	estSizeSrc = 0 # The aggregated size of the commuterset starting from a specific epoch 
	for epochDepSrc in range(epoch(START_OF_DAY), epoch(END_OF_DAY)):
		commuterSetSrc, size = findSingleTrips(epochDepSrc)
		estSize              = estSize + size
		if USE_SETS:
			estSizeSrc         = estSizeSrc + len(commuterSetSrc)
		else:
			estSizeSrc         = estSizeSrc + commuterSetSrc.estimatedSize()
		commuterSet          = commuterSet.union(commuterSetSrc)
	return commuterSet, estSizeSrc, estSize

#--------------------------------------------------------------------------------

time_start  = time.time()
optimalSize = 0

generateNetwork()
generateTrips()

if USE_SETS:
	completeSet = set()
else:
	completeSet = BloomFilter(MAX_DETECTIONS, PROB_BF_FALSE)
completeSetSize  = 0
estSizeMid       = 0
estSizeFine      = 0
averageBFSize    = 0
averageNumOnes   = 0
	
for r in range(NUM_RUNS):
	completeSet, estSizeMidTmp, estSizeFineTmp = findAllSingleTrips()
	if USE_SETS:
		completeSetSize = completeSetSize + len(completeSet)
	else:
		completeSetSize = completeSetSize + completeSet.estimatedSize()
		lsBF            = completeSet.ls()
		averageBFSize   = averageBFSize   + lsBF[0]
		averageNumOnes  = averageNumOnes  + lsBF[2]
	estSizeMid  = estSizeMid  + estSizeMidTmp
	estSizeFine = estSizeFine + estSizeFineTmp

completeSetSize = int(completeSetSize / NUM_RUNS)
estSizeMid      = int(estSizeMid / NUM_RUNS)
estSizeFine     = int(estSizeFine / NUM_RUNS)
averageBFSize   = int(averageBFSize / NUM_RUNS)
averageNumOnes  = int(averageNumOnes / NUM_RUNS)

time_elapsed    = (time.time() - time_start)

print(numOfReturners)
print(completeSetSize, estSizeMid, estSizeFine) 

f = open("results-bfs.txt", "a")
outputString = "\n"
if USE_SETS:
	outputString = outputString + "S"
else:
	outputString = outputString + "B"
outputString = outputString + "{:7d}".format(NUM_TRIPS)
outputString = outputString + "{:5d}".format(EPOCH_LENGTH)
outputString = outputString + "{:8d}".format(MAX_DETECTIONS)
outputString = outputString + "{:7.4f}".format(PROB_BF_FALSE)
outputString = outputString + "{:7d}".format(numOfReturners)
outputString = outputString + "{:7d}".format(completeSetSize)
outputString = outputString + "{:8.2f}".format(accuracy(NUM_TRIPS, completeSetSize) * 100)
outputString = outputString + "{:7d}".format(estSizeMid)
outputString = outputString + "{:8.2f}".format(accuracy(NUM_TRIPS, estSizeMid) * 100)
outputString = outputString + "{:7d}".format(estSizeFine)
outputString = outputString + "{:8.2f}".format(accuracy(NUM_TRIPS, estSizeFine) * 100)
outputString = outputString + "{:12.4f}".format((averageNumOnes / averageBFSize))
outputString = outputString + "{:10.2f}".format(time_elapsed)
f.write(outputString)
f.close()
