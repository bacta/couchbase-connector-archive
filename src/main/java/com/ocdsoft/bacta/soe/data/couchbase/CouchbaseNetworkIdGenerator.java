package com.ocdsoft.bacta.soe.data.couchbase;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ocdsoft.bacta.engine.data.DatabaseConnector;
import com.ocdsoft.bacta.engine.object.NetworkIdGenerator;

/**
 * Created by kburkhardt on 2/23/14.
 */
@Singleton
public class CouchbaseNetworkIdGenerator implements NetworkIdGenerator {

    private final DatabaseConnector databaseConnector;

    @Inject
    public CouchbaseNetworkIdGenerator(DatabaseConnector databaseConnector) {
        this.databaseConnector = databaseConnector;
    }

    public long next() {
        return databaseConnector.nextId();
    }

}
