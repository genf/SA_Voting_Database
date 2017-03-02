
This database organizes data collected to understand voting preferences and behavior in South Africa.



Requirements (may not be a comprehensive list as of yet)
- python 
	- pandas/pymongo
	- json, os, sys, datetime 
- mongodb


File run down

	build_db:

	Contains the functions to set up the database

		mongo_database_setup(): Establishes a connection to mongodb. Creates a database and a collection.


		mongod_insert_df(): Takes a pandas dataframe and inserts it into a local mongodb. This dataframe of conversations will be build up in structure_data/


	structure_data:

	Contains a slew of functions for structuring the polling data

		list_inbound_outbound(): The data is disseminated across files that contain messages inbound and outbound of the system. This function just aggregates a list of those file names so it does not need to be done manually.

		retain_msisdn(): This function is meant to standardize the msisdns.

		create_conversations(): Re-constructs the conversations from data

		import_data(): Takes the conversations created by the above function and inserts them into a dataframe, to then be inserted into a database


	data_analyser:

	Contains functions for parsing and verifying the data.

		parse(): reads from the json files

		get_fields(): helper function to return the fields in a line of json

		check_fields(): verified that the data was correct


	json_reader:

	Contains functions to read json files

		read_json(): returns dictionary of decoded json field-value pairs

		write_gjson_into_file(): unzipps zipped json files


The database is built from the main file. Change the directory path to the appropriate path holding the data on a local machine. I have conditions for reading pathnames from windows and os-es with the posix standard, so the code should adjust accordingly. 

From the main file you can analyze the data. 

To iterate throught he documents in a mongodb database, you need to iterate through the object_ids array:

	"""

	for id_ in object_ids:

		doc = conversations_collection.find_one({"_id": ObjectId(id_)})


	"""

doc is a two-tiered dictionary. The top level dictionary contains two fields: 'user' and 'conversation'

'user' is the address of the user being contacted by the polling system
'conversation' is a dictionary of conversations, where each element is one message, and messages are ordered chronologically

to access these dictionaries, assign:

	"""

	user = doc['user']
	conversation = doc['conversation']

	"""

conversation is of the following structure:

{'msg1': [a bunch of fields], 'msg2':[a bunch of fields]...}

therefore to access a particular value such as "timestamp", one must do the following:

	"""
	timestamp = doc['conversation']['msg-[num]']['timestamp']

	"""

The same goes for any other field.

I've left some preliminary code in the main file as an example.

---------------

Working alongside Professor Aaron Erlich and Ryan Sampana.

For inquiries about the project, contact Aaron Erlich, Professor of Political Science at McGill University (aaron.erlich[@]mcgill.ca). 

-------------------



