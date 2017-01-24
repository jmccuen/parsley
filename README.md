![parsleyIcon.png](https://bitbucket.org/repo/koL5oG/images/3257769548-parsleyIcon.png)

# About Parsley #

Parsley aims to allow you to parse CSV files, JSON Objects, and JSON Arrays into InfoTables as painlessly as possible.
The Parsley Extension for ThingWorx provides a Parsley Resource with the following services:

* ParseJSON 

* ParseCSV

This extension utilizes the Thingworx 6.6.5 SDK. 

# Installing the Parsley Extension #
1. From a web browser, launch ThingWorx.
2. Log into ThingWorx as an administrator.	
3. Go to Import/Export > Import.	  
4. Click Choose File and select Parsley_ExtensionPackage.zip
5. Click Import.

Note: If an Import Successful message does not display, contact your ThingWorx System Administrator.	  

6. Click Yes to refresh Composer after importing the final extension.	 
7. Confirm that the Extension has been imported properly.  Check the Application Log for potential problems.
	
	
# Usage #
The Parsley Extension can be used by invoking any one of the following services through the Snippets tab when creating a new Service on any ThingWorx entity:
•	ParseJSON
•	ParseCSV

![1.png](https://bitbucket.org/repo/koL5oG/images/3054232054-1.png) ![2.png](https://bitbucket.org/repo/koL5oG/images/825040558-2.png)

These services can be used to parse a JSON object or CSV file directly into an InfoTable result.

### ParseCSV ###

![3.png](https://bitbucket.org/repo/koL5oG/images/3594730462-3.png)

### Result ###

![4.png](https://bitbucket.org/repo/koL5oG/images/1318645079-4.png)

### ParseJSON ###

![5.png](https://bitbucket.org/repo/koL5oG/images/3513007393-5.png)

### Result ###

![6.png](https://bitbucket.org/repo/koL5oG/images/4253620784-6.png)

# Compatibility #

This extension was tested for compatibility with the following ThingWorx Platform version(s) and Operating System(s):

* ThingWorx 6.6.5+