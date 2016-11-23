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
package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class SearchTransportServiceTests extends ESTestCase {

    public void testRemoteClusterSeedSetting() {
        // simple validation
        SearchTransportService.REMOTE_CLUSTERS_SEEDS.get(Settings.builder()
            .put("action.search.remote.foo", "192.168.0.1:8080")
            .put("action.search.remote.bar", "[::1]:9090").build());

        expectThrows(IllegalArgumentException.class, () ->
        SearchTransportService.REMOTE_CLUSTERS_SEEDS.get(Settings.builder()
            .put("action.search.remote.foo", "192.168.0.1").build()));
    }

    public void testBuiltRemoteClustersSeeds() {
        Map<String, List<DiscoveryNode>> map = SearchTransportService.builtRemoteClustersSeeds(
            SearchTransportService.REMOTE_CLUSTERS_SEEDS.get(Settings.builder()
            .put("action.search.remote.foo", "192.168.0.1:8080")
            .put("action.search.remote.bar", "[::1]:9090").build()));
        assertEquals(2, map.size());
        assertTrue(map.containsKey("foo"));
        assertTrue(map.containsKey("bar"));
        assertEquals(1, map.get("foo").size());
        assertEquals(1, map.get("bar").size());

        DiscoveryNode foo = map.get("foo").get(0);
        assertEquals(foo.getAddress(), new TransportAddress(new InetSocketAddress("192.168.0.1", 8080)));
        assertEquals(foo.getId(), "foo#192.168.0.1:8080");
        assertEquals(foo.getVersion(), Version.CURRENT.minimumCompatibilityVersion());

        DiscoveryNode bar = map.get("bar").get(0);
        assertEquals(bar.getAddress(), new TransportAddress(new InetSocketAddress("[::1]", 9090)));
        assertEquals(bar.getId(), "bar#[::1]:9090");
        assertEquals(bar.getVersion(), Version.CURRENT.minimumCompatibilityVersion());
    }
}
