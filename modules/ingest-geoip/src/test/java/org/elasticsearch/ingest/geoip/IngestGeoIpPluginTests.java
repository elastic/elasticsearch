/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.model.AbstractResponse;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin.GeoIpCache;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;

public class IngestGeoIpPluginTests extends ESTestCase {

    public void testCachesAndEvictsResults() {
        GeoIpCache cache = new GeoIpCache(1);
        AbstractResponse response1 = mock(AbstractResponse.class);
        AbstractResponse response2 = mock(AbstractResponse.class);

        //add a key
        AbstractResponse cachedResponse = cache.putIfAbsent(InetAddresses.forString("127.0.0.1"), AbstractResponse.class, ip -> response1);
        assertSame(cachedResponse, response1);
        assertSame(cachedResponse, cache.putIfAbsent(InetAddresses.forString("127.0.0.1"), AbstractResponse.class, ip -> response1));
        assertSame(cachedResponse, cache.get(InetAddresses.forString("127.0.0.1"), AbstractResponse.class));


        // evict old key by adding another value
        cachedResponse = cache.putIfAbsent(InetAddresses.forString("127.0.0.2"), AbstractResponse.class, ip -> response2);
        assertSame(cachedResponse, response2);
        assertSame(cachedResponse, cache.putIfAbsent(InetAddresses.forString("127.0.0.2"), AbstractResponse.class, ip -> response2));
        assertSame(cachedResponse, cache.get(InetAddresses.forString("127.0.0.2"), AbstractResponse.class));

        assertNotSame(response1, cache.get(InetAddresses.forString("127.0.0.1"), AbstractResponse.class));
    }

    public void testThrowsFunctionsException() {
        GeoIpCache cache = new GeoIpCache(1);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> cache.putIfAbsent(InetAddresses.forString("127.0.0.1"), AbstractResponse.class,
                ip -> { throw new IllegalArgumentException("bad"); }));
        assertEquals("bad", ex.getMessage());
    }

    public void testInvalidInit() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->  new GeoIpCache(-1));
        assertEquals("geoip max cache size must be 0 or greater", ex.getMessage());
    }
}
