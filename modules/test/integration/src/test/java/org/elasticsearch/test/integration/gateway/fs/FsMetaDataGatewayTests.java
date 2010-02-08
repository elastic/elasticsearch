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

package org.elasticsearch.test.integration.gateway.fs;

import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.server.internal.InternalServer;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;

/**
 * @author kimchy (Shay Banon)
 */
public class FsMetaDataGatewayTests extends AbstractServersTests {

    @AfterMethod void closeServers() {
        server("server1").stop();
        // since we store (by default) the index snapshot under the gateway, resetting it will reset the index data as well
        ((InternalServer) server("server1")).injector().getInstance(Gateway.class).reset();
        closeAllServers();
    }

    @BeforeMethod void buildServer1() {
        buildServer("server1");
        // since we store (by default) the index snapshot under the gateway, resetting it will reset the index data as well
        ((InternalServer) server("server1")).injector().getInstance(Gateway.class).reset();
    }

    @Test public void testIndexActions() throws Exception {

        buildServer("server1");
        ((InternalServer) server("server1")).injector().getInstance(Gateway.class).reset();
        server("server1").start();

        client("server1").admin().indices().create(createIndexRequest("test")).actionGet();

        closeServer("server1");

        Thread.sleep(1000);

        startServer("server1");
        Thread.sleep(3000);
        try {
            client("server1").admin().indices().create(createIndexRequest("test")).actionGet();
            assert false : "index should exists";
        } catch (IndexAlreadyExistsException e) {
            // all is well
        }

        ((InternalServer) server("server1")).injector().getInstance(Gateway.class).reset();
    }
}
