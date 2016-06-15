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
package org.elasticsearch.search.scroll;

import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)
public class ScrollTerminationTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfShards() {
        return 2;
    }

    // https://github.com/elastic/elasticsearch/issues/18885
    public void testScrollTerminates() throws Exception {
        Settings nodeSettings = ImmutableSettings.settingsBuilder()
                .put("nome.data", "false")
                .build();
        ListenableFuture<String> node1 = internalCluster().startNodeAsync(nodeSettings);
        ListenableFuture<String> node2 = internalCluster().startNodeAsync(ImmutableSettings.EMPTY);
        ListenableFuture<String> node3 = internalCluster().startNodeAsync(ImmutableSettings.EMPTY);

        Client client = internalCluster().client(node1.get());
        node2.get();
        node3.get();

        client.prepareIndex("test", "test", "3").setSource("timestamp", "2016-05-19T03:00:00Z", "username", "a").get();
        client.prepareIndex("test", "test", "5").setSource("timestamp", "2016-05-19T05:00:00Z").get();
        assertNoFailures(client.admin().indices().prepareRefresh("test").get());

        SearchResponse r = client.prepareSearch("test")
            .addSort("username", SortOrder.ASC)
            .addSort("timestamp", SortOrder.ASC)
            .setScroll(TimeValue.timeValueMinutes(1))
            .setSize(2)
            .get();
        assertSearchResponse(r);
        assertEquals(2, r.getHits().getHits().length);

        r = client.prepareSearchScroll(r.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
        assertSearchResponse(r);
        assertEquals(0, r.getHits().getHits().length);

        clearScroll(r.getScrollId());
    }

}
