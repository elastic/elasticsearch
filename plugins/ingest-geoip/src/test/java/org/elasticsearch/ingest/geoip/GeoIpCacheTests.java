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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.maxmind.db.NodeCache;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;

public class GeoIpCacheTests extends ESTestCase {
    public void testCachesAndEvictsResults() throws Exception {
        GeoIpCache cache = new GeoIpCache(1);
        final NodeCache.Loader loader = key -> new IntNode(key);

        JsonNode jsonNode1 = cache.get(1, loader);
        assertSame(jsonNode1, cache.get(1, loader));

        // evict old key by adding another value
        cache.get(2, loader);

        assertNotSame(jsonNode1, cache.get(1, loader));
    }

    public void testThrowsElasticsearchException() throws Exception {
        GeoIpCache cache = new GeoIpCache(1);
        NodeCache.Loader loader = (int key) -> {
            throw new IllegalArgumentException("Illegal key");
        };
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> cache.get(1, loader));
        assertTrue("Expected cause to be of type IllegalArgumentException but was [" + ex.getCause().getClass() + "]",
            ex.getCause() instanceof IllegalArgumentException);
        assertEquals("Illegal key", ex.getCause().getMessage());
    }
}
