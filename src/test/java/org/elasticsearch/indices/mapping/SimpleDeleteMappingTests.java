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

package org.elasticsearch.indices.mapping;


import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;


/**
 *
 */
public class SimpleDeleteMappingTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleDeleteMapping() throws Exception {
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("value", "test" + i)
                    .endObject()).execute().actionGet();
        }

        ensureGreen();
        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client().prepareCount().setQuery(matchAllQuery()).execute().actionGet();
            assertThat(countResponse.getCount(), equalTo(10l));
        }

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        for (int i = 0; i < 10 && !clusterState.metaData().index("test").mappings().containsKey("type1"); i++, Thread.sleep(100)) ;

        assertThat(clusterState.metaData().index("test").mappings().containsKey("type1"), equalTo(true));

        client().admin().indices().prepareDeleteMapping().setType("type1").execute().actionGet();
        Thread.sleep(500); // for now, we don't have ack logic, so just wait

        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client().prepareCount().setQuery(matchAllQuery()).execute().actionGet();
            assertThat(countResponse.getCount(), equalTo(0l));
        }

        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(clusterState.metaData().index("test").mappings().containsKey("type1"), equalTo(false));
    }
}
