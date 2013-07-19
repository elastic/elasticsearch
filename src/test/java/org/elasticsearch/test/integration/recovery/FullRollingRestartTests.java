/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.integration.recovery;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class FullRollingRestartTests extends AbstractNodesTests {

    @After
    public void shutdownNodes() {
        closeAllNodes();
    }

    @Test
    @Slow
    public void testFullRollingRestart() throws Exception {
        startNode("node1");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        for (int i = 0; i < 1000; i++) {
            client("node1").prepareIndex("test", "type1", Long.toString(i))
                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map()).execute().actionGet();
        }
        client("node1").admin().indices().prepareFlush().execute().actionGet();
        for (int i = 1000; i < 2000; i++) {
            client("node1").prepareIndex("test", "type1", Long.toString(i))
                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map()).execute().actionGet();
        }

        // now start adding nodes
        startNode("node2");
        startNode("node3");

        // make sure the cluster state is green, and all has been recovered
        assertThat(client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3").execute().actionGet().isTimedOut(), equalTo(false));

        // now start adding nodes
        startNode("node4");
        startNode("node5");

        // make sure the cluster state is green, and all has been recovered
        assertThat(client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("5").execute().actionGet().isTimedOut(), equalTo(false));

        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(2000l));
        }

        // now start shutting nodes down
        closeNode("node1");
        // make sure the cluster state is green, and all has been recovered
        assertThat(client("node5").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("4").execute().actionGet().isTimedOut(), equalTo(false));
        closeNode("node2");
        // make sure the cluster state is green, and all has been recovered
        assertThat(client("node5").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3").execute().actionGet().isTimedOut(), equalTo(false));


        client("node5").admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client("node5").prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(2000l));
        }

        closeNode("node3");
        // make sure the cluster state is green, and all has been recovered
        assertThat(client("node5").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("2").execute().actionGet().isTimedOut(), equalTo(false));
        closeNode("node4");

        // make sure the cluster state is green, and all has been recovered
        assertThat(client("node5").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForYellowStatus().setWaitForRelocatingShards(0).setWaitForNodes("1").execute().actionGet().isTimedOut(), equalTo(false));

        client("node5").admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client("node5").prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(2000l));
        }
    }
}
