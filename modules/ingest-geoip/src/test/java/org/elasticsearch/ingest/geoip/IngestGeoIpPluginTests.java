/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
