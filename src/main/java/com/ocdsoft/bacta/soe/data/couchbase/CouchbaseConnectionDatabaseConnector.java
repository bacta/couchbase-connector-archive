package com.ocdsoft.bacta.soe.data.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.protocol.views.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ocdsoft.bacta.engine.conf.BactaConfiguration;
import com.ocdsoft.bacta.engine.data.ConnectionDatabaseConnector;
import com.ocdsoft.bacta.engine.object.account.Account;
import net.spy.memcached.ConnectionObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.*;

/**
 * Created by kburkhardt on 1/23/15.
 */
@Singleton
public final class CouchbaseConnectionDatabaseConnector implements ConnectionDatabaseConnector {

    private static final Logger logger = LoggerFactory.getLogger(CouchbaseConnectionDatabaseConnector.class);

    private final CouchbaseTranscoder transcoder;

    private CouchbaseClient client;
    private final Gson gson;

    private View usernameView;
    private View authTokenView;
    private View characterNamesView;

    @Inject
    public CouchbaseConnectionDatabaseConnector(BactaConfiguration configuration, CouchbaseTranscoder transcoder) throws Exception {
        this.transcoder = transcoder;

        gson = registerSerializers();

        Properties systemProperties = System.getProperties();
        systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SLF4JLogger");
        System.setProperties(systemProperties);

        client = connect(
                configuration.getStringWithDefault("Bacta/Database/Couchbase", "Address", "127.0.0.1"),
                configuration.getIntWithDefault("Bacta/Database/Couchbase", "Port", 8091),
                configuration.getStringWithDefault("Bacta/Database/Couchbase", "ConnectionObjectsBucket", "connectionObjects"));

        init(configuration);
    }

    private Gson registerSerializers() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(InetSocketAddress.class, new InetSocketAddressSerializer());

        return gsonBuilder.create();
    }

    private void init(BactaConfiguration configuration) {

        try {

            if (client.get("ClusterId") == null) {
                client.add("ClusterId", String.valueOf(1));

            }
            if (client.get("AccountId") == null) {
                client.add("AccountId", String.valueOf(1));
            }

            String designDoc = configuration.getString("Bacta/Database/Couchbase", "DesignDoc");
            String userview = configuration.getString("Bacta/Database/Couchbase", "UsernameView");

            DesignDocument design;
            boolean changed = false;

            try {
                design = client.getDesignDoc(designDoc);
            } catch(Exception e) {
                design = new DesignDocument(designDoc);
            }

            try {
                usernameView = client.getView(designDoc, userview);
            } catch (InvalidViewException e) {
                changed = true;
                String map = "function (doc, meta) {\n" +
                        "  if(doc.username) {\n" +
                        "  \temit(meta.id, doc);\n" +
                        "  }\n" +
                        "}";

                ViewDesign viewDesign = new ViewDesign(userview, map);
                design.getViews().add(viewDesign);

                if (client.createDesignDoc(design)) {
                    usernameView = client.getView(designDoc, userview);
                } else {
                    throw new Exception("Unable to create username views");
                }
            }

            String authview = configuration.getString("Bacta/Database/Couchbase", "AuthTokenView");

            try {
                 authTokenView = client.getView(designDoc, authview);
            } catch (InvalidViewException e) {
                changed = true;
                String map = "function (doc, meta) {\n" +
                        "  if(doc.authToken) {\n" +
                        "  \temit(doc.authToken, doc);\n" +
                        "  }\n" +
                        "}";

                ViewDesign viewDesign = new ViewDesign(authview, map);
                design.getViews().add(viewDesign);


            }

            String characterNamesViewName = configuration.getString("Bacta/Database/Couchbase", "CharacterNamesView");

            try {
                characterNamesView = client.getView(designDoc, characterNamesViewName);
            } catch (InvalidViewException e) {
                changed = true;
                String map = "function (doc, meta) {\n" +
                        "  if(doc.type == 'account') {\n" +
                        "     for( var i=0; i < doc.characterList.length; i++) {\n" +
                        "  \temit(doc.characterList[i].name, doc.characterList[i].clusterId);\n" +
                        "     }\n" +
                        "  }\n" +
                        "}";

                ViewDesign viewDesign = new ViewDesign(characterNamesViewName, map);
                design.getViews().add(viewDesign);
            }

            if(changed) {
                if(!client.createDesignDoc(design)) {
                    throw new Exception("Unable to obtain database views");
                }

                usernameView = client.getView(designDoc, userview);
                authTokenView = client.getView(designDoc, authview);
                characterNamesView = client.getView(designDoc, characterNamesViewName);
            }

        } catch(Exception e) {

            logger.error("Unable to initialize database", e);
            System.exit(1);
        }
    }

    /**
     * Connect to the server, or servers given.
     *
     * @param serverAddress the server addresses to connect with.
     * @throws java.io.IOException                 if there is a problem with connecting.
     * @throws java.net.URISyntaxException
     * @throws javax.naming.ConfigurationException
     */
    private CouchbaseClient connect(final String serverAddress, int port, final String bucket) throws Exception {


        URI base = new URI(String.format("http://%s:%s/pools", serverAddress, port));
        List<URI> baseURIs = new ArrayList<URI>();
        baseURIs.add(base);
        CouchbaseConnectionFactory cf = new CouchbaseConnectionFactory(baseURIs, bucket, "");

        CouchbaseClient client = new CouchbaseClient(cf);

        client.addObserver(new ConnectionObserver() {

            public void connectionLost(SocketAddress sa) {
                logger.debug("Connection lost to " + sa.toString() + " '" + bucket + "'");
            }

            public void connectionEstablished(SocketAddress sa, int reconnectCount) {
                logger.debug("Connection established with " + sa.toString() + " '" + bucket + "'");
                logger.debug("Reconnected count: " + reconnectCount);
            }
        });

        return client;
    }

    @Override
    public int nextClusterId() {
        return (int) client.incr("ClusterId", 1);
    }

    @Override
    public int nextAccountId() {
        return (int) client.incr("AccountId", 1);
    }

    @Override
    public <T> T getObject(String key, Class<T> clazz) {

        Object object = client.get(key);
        if(object == null) return null;

        return gson.fromJson(object.toString(), clazz);
    }

    @Override
    public <T> void updateObject(String key, T object) {
        client.set(key, gson.toJson(object));
    }


    @Override
    public <T> void createObject(String key, T object) {
        client.add(key, gson.toJson(object));
    }

    @Override
    public <T extends Account> T lookupSession(String authToken, Class<T> clazz) {
        Query userQuery = new Query();
        userQuery.setIncludeDocs(true);
        userQuery.setKey("\"" + authToken + "\"");
        userQuery.setStale(Stale.FALSE);

        ViewResponse response = client.query(authTokenView, userQuery);
        if(response.size() == 0) {
            return null;
        }

        if(response.size() != 1) {
            logger.error("Duplicate auth tokens in database: " + authToken);
            return null;
        }

        String document = response.removeLastElement().getDocument().toString();
        return gson.fromJson(document, clazz);
    }

    @Override
    public Set<String> getClusterCharacterSet(int clusterId) {
        Query userQuery = new Query();
        userQuery.setIncludeDocs(true);
        userQuery.setStale(Stale.FALSE);

        ViewResponse response = client.query(characterNamesView, userQuery);

        Set<String> characters = new TreeSet<>();

        Iterator<ViewRow> iterator = response.iterator();
        while(iterator.hasNext()) {
            ViewRow row = iterator.next();
            String name = row.getKey();
            int myClusterId = Integer.parseInt(row.getValue());

            if(myClusterId == clusterId) {

                String firstName = name.indexOf(" ") != -1 ? name.substring(0, name.indexOf(" ")) : name;
                if(!characters.add(firstName.toLowerCase())) {
                    logger.error("Duplicate Character name: Cluster=" + clusterId +  " Account: " + row.getId() + " Character: " + firstName);
                }
            }
        }

        return characters;
    }
}
