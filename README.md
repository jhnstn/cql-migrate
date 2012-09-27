# Migrate

Updates a cassandra database by executing cql 3 files

Requires leiningen v2.x.x and a running cassandra server v1.1.x

#### Features

* Executes cql commands from file 
* Records history of executed migration files

## Usage

### From Standalone

 download the latest jar [here](https://github.com/downloads/snrobot/cql-migrate/cql-migrate-0.1.0-standalone.jar)
 
 then run
 
 ```
 java -jar cql-migrate-<release>-standalone.jar [SWITCHES] ... [COMMAND]
 ````
 for full list of switches and commands run
 
 ```
  java -jar cql-migrate-<release>-standalone.jar [SWITCHES] ... [COMMAND]
 ```
 

====
### From source
 you can use leiningen to build a jar or run directly
 
### Initialize
 The migration tool must be initialized before running any migrations.
 
 From root run 
 
 1. ``` lein repl ```
 2. ``` (initialize) ```
 
 This will only run once otherwise it will throw an error
 
 **TODO** roll this into the migrate function


### Migrate file generation


* Migration files must be valid cql 3. For more information see [cql 3 reference](http://www.datastax.com/docs/1.1/references/cql/index).

* The migrate tool will look for these files in ```./migrations/```


* The migrate tools comes with a function to generate migration file names. To use run:

	1. ```lein repl```
	2. ```(migrate-name "some description")```

	This will print to stdout something like ```201208220190809_some_description.cql```

	If you just want the formated timestamp use ```(migrate-name)```. This will return just the timestamp

**TODO** Add function to create an empty  migration file with a formated name
 
### Migrate


 * ```lien run``` 
 
 
### Test
 
 * ```lien test``` * 
 
  \* requires a running cassandra server
 
 

## License


