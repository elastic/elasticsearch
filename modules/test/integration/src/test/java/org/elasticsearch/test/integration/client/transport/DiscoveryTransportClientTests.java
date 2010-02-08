/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.client.transport;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.testng.annotations.AfterMethod;

import static org.elasticsearch.client.Requests.*;

/**
 * @author kimchy (Shay Banon)
 */
public class DiscoveryTransportClientTests extends AbstractServersTests {

    private TransportClient client;

    @AfterMethod public void closeServers() {
        if (client != null) {
            client.close();
        }
        closeAllServers();
    }

    /*@Test*/

    public void testWithDiscovery() throws Exception {
        startServer("server1");
        client = new TransportClient(ImmutableSettings.settingsBuilder().putBoolean("discovery.enabled", true).build());
        // wait a bit so nodes will be discovered
        Thread.sleep(1000);
        client.admin().indices().create(createIndexRequest("test")).actionGet();
        Thread.sleep(500);

        client.admin().cluster().ping(pingSingleRequest("test").type("person").id("1")).actionGet();
        startServer("server2");
        Thread.sleep(1000);
        client.admin().cluster().ping(pingSingleRequest("test").type("person").id("1")).actionGet();
        closeServer("server1");
        Thread.sleep(10000);
        client.admin().cluster().ping(pingSingleRequest("test").type("person").id("1")).actionGet();
        closeServer("server2");
        client.admin().cluster().ping(pingSingleRequest("test").type("person").id("1")).actionGet();
    }

}
