package com.ocdsoft.bacta.soe.data.couchbase;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.ocdsoft.bacta.engine.conf.BactaConfiguration;
import com.ocdsoft.bacta.engine.object.account.Account;
import com.ocdsoft.bacta.engine.security.authenticator.AccountService;
import com.ocdsoft.bacta.engine.security.password.PasswordHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;


/**
 * Created by Kyle on 4/3/14.
 */
@Singleton
public class CouchbaseAccountService<T extends Account> implements AccountService<T> {

    private final Logger logger = LoggerFactory.getLogger(getClass().getSimpleName());

    @Inject
    private CouchbaseDatabaseConnector connector;

    @Inject
    private Provider<T> accountProvider;

    @Inject
    private PasswordHash passwordHash;

    private final SecureRandom secureRandom = new SecureRandom();

    private final long authTokenDuration;

    private final Class<? extends Account> accountClazz;

    @Inject
    private CouchbaseAccountService(BactaConfiguration configuration, T accountClazz) {
        authTokenDuration = configuration.getLongWithDefault("Bacta/LoginServer", "AuthTokenTTL", 600) * 1000;
        this.accountClazz = accountClazz.getClass();
    }


    @Override
    public T createAccount(String username, String password) {

        T account = accountProvider.get();
        account.setUsername(username);
        try {

            account.setPassword(passwordHash.createHash(password));
            connector.createAdminObject(account.getUsername(), account);
            return account;

        } catch (Exception e) {
            logger.error("Unable to create account", e);
        }
        return null;
    }

    @Override
    public T getAccount(String username) {
        return (T) connector.getAdminObject(username, accountClazz);
    }

    @Override
    public void createAuthToken(T account) {

        String authToken = String.valueOf(Math.abs(secureRandom.nextLong())) + String.valueOf(Math.abs(secureRandom.nextLong()));
        account.setAuthToken(authToken);
        account.setAuthExpiration(System.currentTimeMillis() + authTokenDuration);
        updateAccount(account);
    }

    @Override
    public void updateAccount(T account) {
        connector.updateAdminObject(account.getUsername(), account);
    }

    @Override
    public boolean authenticate(T account, String password) {
        try {
            return passwordHash.validatePassword(password, account.getPassword());
        } catch (Exception e) {
            logger.error("Unable to authenticate account", e);
        }
        return false;
    }

    @Override
    public T validateSession(String authToken) {
        T account = (T) connector.lookupSession(authToken, accountClazz);

        if(account != null && account.getAuthExpiration() < System.currentTimeMillis()) {
            account.setAuthToken("");
            account.setAuthExpiration(System.currentTimeMillis());
            updateAccount(account);

            return null;
        }

        return account;
    }
}
