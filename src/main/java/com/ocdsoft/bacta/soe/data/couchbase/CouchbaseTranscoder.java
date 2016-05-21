package com.ocdsoft.bacta.soe.data.couchbase;

import com.google.inject.Inject;
import com.ocdsoft.bacta.engine.object.NetworkObject;
import com.ocdsoft.bacta.engine.serialize.NetworkSerializer;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.BaseSerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kburkhardt on 7/25/14.
 */

public class CouchbaseTranscoder<T extends NetworkObject> extends BaseSerializingTranscoder implements Transcoder<T> {

    private static final Logger logger = LoggerFactory.getLogger(CouchbaseTranscoder.class);

    @Inject
    private NetworkSerializer networkSerializer;

    public CouchbaseTranscoder() {
        this(CachedData.MAX_SIZE);
    }

    public CouchbaseTranscoder(int max) {
        super(max);
    }

    @Override
    public CachedData encode(T networkObject) {
        logger.trace("Serializing type: " + networkObject.getClass());

        int flags = 0;
        byte[] data = networkSerializer.serialize(networkObject);

        // Flags of some sort?

        return new CachedData(flags, data, CachedData.MAX_SIZE);
    }

    @Override
    public T decode(CachedData d) {

        T object = (T) networkSerializer.deserialize(d.getData());

        logger.trace("Deserializing type: " + object.getClass());

        // Perhaps use flags of some sort?

        return object;
    }
}
