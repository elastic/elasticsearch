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

package org.elasticsearch.recovery;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.elasticsearch.test.AbstractIntegrationTest.ClusterScope;
import org.elasticsearch.test.AbstractIntegrationTest.Scope;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@ClusterScope(scope=Scope.TEST, numNodes = 0)
public class FullRollingRestartTests extends AbstractIntegrationTest {

    @Test
    @Slow
    public void testFullRollingRestart() throws Exception {
        cluster().startNode();
        client().admin().indices().prepareCreate("test").execute().actionGet();
    
        for (int i = 0; i < 1000; i++) {
            client().prepareIndex("test", "type1", Long.toString(i))
                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map()).execute().actionGet();
        }
        client().admin().indices().prepareFlush().execute().actionGet();
        for (int i = 1000; i < 2000; i++) {
            client().prepareIndex("test", "type1", Long.toString(i))
                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map()).execute().actionGet();
        }

        // now start adding nodes
        cluster().startNode();
        cluster().startNode();

        // make sure the cluster state is green, and all has been recovered
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3").execute().actionGet().isTimedOut(), equalTo(false));

        // now start adding nodes
        cluster().startNode();
        cluster().startNode();

        // make sure the cluster state is green, and all has been recovered
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("5").execute().actionGet().isTimedOut(), equalTo(false));

        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(2000l));
        }

        // now start shutting nodes down
        cluster().stopRandomNode();
        // make sure the cluster state is green, and all has been recovered
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("4").execute().actionGet().isTimedOut(), equalTo(false));
        cluster().stopRandomNode();
        // make sure the cluster state is green, and all has been recovered
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3").execute().actionGet().isTimedOut(), equalTo(false));


        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(2000l));
        }

        cluster().stopRandomNode();
        // make sure the cluster state is green, and all has been recovered
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("2").execute().actionGet().isTimedOut(), equalTo(false));
        cluster().stopRandomNode();

        // make sure the cluster state is green, and all has been recovered
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForYellowStatus().setWaitForRelocatingShards(0).setWaitForNodes("1").execute().actionGet().isTimedOut(), equalTo(false));

        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(2000l));
        }
    }
}
