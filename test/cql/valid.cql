

USE system;
CREATE KEYSPACE TestMigrationSource WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1;
USE TestMigrationSource;

CREATE COLUMNFAMILY tester (name varchar , id int ,PRIMARY KEY (name)) ;

DROP KEYSPACE TestMigrationSource;

