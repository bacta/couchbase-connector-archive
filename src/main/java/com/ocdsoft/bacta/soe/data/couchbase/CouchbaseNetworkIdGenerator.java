package com.ocdsoft.bacta.soe.data.couchbase;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ocdsoft.bacta.engine.data.GameDatabaseConnector;
import com.ocdsoft.bacta.engine.object.NetworkIdGenerator;

/**
 * Created by kburkhardt on 2/23/14.
 */
@Singleton
public class CouchbaseNetworkIdGenerator implements NetworkIdGenerator {

    private final GameDatabaseConnector gameDatabaseConnector;

    @Inject
    public CouchbaseNetworkIdGenerator(GameDatabaseConnector gameDatabaseConnector) {
        this.gameDatabaseConnector = gameDatabaseConnector;
    }

    public long next() {
        return gameDatabaseConnector.nextId();
    }

}
