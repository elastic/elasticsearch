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

package org.elasticsearch.test.integration.ping;

import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingResponse;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingResponse;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingResponse;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.elasticsearch.util.logging.Loggers;
import org.slf4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class PingActionTests extends AbstractServersTests {

    private final Logger logger = Loggers.getLogger(PingActionTests.class);

    @BeforeMethod public void startServers() {
        startServer("server1");
        startServer("server2");
    }

    @AfterMethod public void closeServers() {
        closeAllServers();
    }

    @Test public void testIndexActions() throws Exception {
        logger.info("Creating index [test1]");
        client("server1").admin().indices().create(createIndexRequest("test1")).actionGet();
        logger.info("Creating index [test2]");
        client("server1").admin().indices().create(createIndexRequest("test2")).actionGet();

        logger.info("Sleeping to shards allocate and start");
        Thread.sleep(500);

        logger.info("Pinging single person with id 1");
        SinglePingResponse singleResponse = client("server1").admin().cluster().ping(pingSingleRequest("test1").type("person").id("1")).actionGet();

        logger.info("Broadcast pinging test1 and test2");
        BroadcastPingResponse broadcastResponse = client("server1").admin().cluster().ping(pingBroadcastRequest("test1", "test2")).actionGet();
        assertThat(broadcastResponse.successfulShards(), equalTo(10));
        assertThat(broadcastResponse.failedShards(), equalTo(0));

        logger.info("Broadcast pinging test1");
        broadcastResponse = client("server1").admin().cluster().ping(pingBroadcastRequest("test1")).actionGet();
        assertThat(broadcastResponse.successfulShards(), equalTo(5));
        assertThat(broadcastResponse.failedShards(), equalTo(0));

        logger.info("Replication pinging test1 and test2");
        ReplicationPingResponse replicationResponse = client("server1").admin().cluster().ping(pingReplicationRequest("test1", "test2")).actionGet();
        assertThat(replicationResponse.indices().size(), equalTo(2));
        assertThat(replicationResponse.index("test1").successfulShards(), equalTo(5));
        assertThat(replicationResponse.index("test1").failedShards(), equalTo(0));
    }
}
