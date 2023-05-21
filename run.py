import subprocess, sys

# Input parameters:

# NUM_LOCATIONS	 = int(sys.argv[1])								 # Size of the network
# PROB_LINK			 = float(int(sys.argv[2])/100)		 # Probability of making a link between two nodes (in %)
# NUM_TRIPS			 = int(sys.argv[3])								 # Total number of trips in the network
# EPOCH_LENGTH	 = int(sys.argv[4])								 # Epoch length in minutes
# MAX_DETECTIONS = int(sys.argv[5])								 # Maximum number of detections/epoch supported (global)
# PROB_BF_FALSE	 = float(int(sys.argv[6])/10000.0) # Tolerated false positives in Bloom filters (in %%%)
# USE_SETS       = int(sys.argv[7])==1             # Decide whether we're going to use sets or Bloom filters
# NUM_RUNS       = int(sys.argv[8])                # Number of runs for the same set of parameter values

for ntrip in [100, 1000, 10000, 100000]:
	for bfsize in [ 1 ]:
		for prob in [ 10 ]:
			command = ["python3", "travel-multiple-lines-fast.py", "2", "100", str(ntrip), "5", str(int(ntrip/bfsize)), str(prob), "0", "100"]
			print(command)
			subprocess.call(command)


