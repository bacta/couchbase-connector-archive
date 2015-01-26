/*
 * Created by IntelliJ IDEA.
 * User: Kyle
 * Date: 4/3/14
 * Time: 8:50 PM
 */
package com.ocdsoft.bacta.soe.data.couchbase;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.ocdsoft.bacta.engine.object.account.Account;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

public class CouchbaseAccountProvider<T extends Account> implements Provider<T> {

    private static final Logger logger = LoggerFactory.getLogger(CouchbaseAccountProvider.class);

    @Inject
    private CouchbaseConnectionDatabaseConnector connector;

    private final Class accountClass;
    private final Constructor<T> accountConstructor;

    public CouchbaseAccountProvider(T account) throws NoSuchMethodException {
        accountClass = account.getClass();
        accountConstructor = accountClass.getConstructor(Integer.TYPE);
    }

    public T get() {
        try {
            return accountConstructor.newInstance(connector.nextAccountId());
        } catch (Exception e) {
            logger.error("Unable to create account object", e);
        }

        return null;
    }
}
