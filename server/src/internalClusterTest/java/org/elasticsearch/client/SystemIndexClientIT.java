/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

//@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SystemIndexClientIT extends ESIntegTestCase {

    private SystemIndexClient systemIndexClient(Collection<SystemIndexDescriptor> ownedSystemIndices) {
//        assertThat("SystemIndexClient requires a NodeClient", client(), instanceOf(NodeClient.class));
        NodeClient client = null;
        for (Client thisClient : internalCluster().getClients()) {
            if (thisClient instanceof NodeClient) {
                client = (NodeClient) thisClient;
                break;
            }
        }
        assertNotNull(client);
        return new SystemIndexClient(client,
            ownedSystemIndices,
            () -> client().admin().cluster().prepareState().get().getState(),
            new IndexNameExpressionResolver());
    }

    public void testCanAccessSystemIndices() throws Exception {
        final String systemIndex = ".system-index";
        CreateIndexResponse createResponse = client().admin().indices().prepareCreate(systemIndex)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_INDEX_SYSTEM, true).build())
            .get();
        assertAcked(createResponse);

        List<SystemIndexDescriptor> descriptors = Collections.singletonList(new SystemIndexDescriptor(".system*", "testing"));
        Client systemIndexClient = systemIndexClient(descriptors);
        IndexResponse indexResponse = systemIndexClient.prepareIndex(systemIndex)
            .setSource("{\"testfield\": 1}", XContentType.JSON).get();
        assertThat(indexResponse.status().getStatus(), allOf(greaterThanOrEqualTo(200), lessThan(300)));
        indexResponse = systemIndexClient.prepareIndex(systemIndex)
            .setSource("{\"testfield\": 2}", XContentType.JSON).get();
        assertThat(indexResponse.status().getStatus(), allOf(greaterThanOrEqualTo(200), lessThan(300)));
        refresh();

//        QueryBuilder query = randomFrom(QueryBuilders.matchQuery("testfield", "testvalue"), QueryBuilders.matchAllQuery());
        QueryBuilder query = QueryBuilders.matchQuery("testfield", 1);

        {
            SearchResponse searchResponse = systemIndexClient.prepareSearch(systemIndex)
                .setQuery(query)
                .get();
            assertHitCount(searchResponse, 1);

            searchResponse = systemIndexClient.prepareSearch(systemIndex)
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort("testfield", SortOrder.ASC)
                .get();
            assertHitCount(searchResponse, 2);
        }

        {
            String indexPattern = ".system*";
            SearchResponse searchResponse = systemIndexClient.prepareSearch(indexPattern)
                .setQuery(query)
                .get();
            assertHitCount(searchResponse, 1);

            searchResponse = systemIndexClient.prepareSearch(".system*")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort("testfield", SortOrder.ASC)
                .get();
            assertHitCount(searchResponse, 2);
        }
    }

    public void testSystemIndexAccessThroughDefaultClientBlocked() {
        final String systemIndex = ".system-index";
        CreateIndexResponse createResponse = client().admin().indices().prepareCreate(systemIndex)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_INDEX_SYSTEM, true).build())
            .get();
        assertAcked(createResponse);

        List<SystemIndexDescriptor> descriptors = Collections.singletonList(new SystemIndexDescriptor(".system*", "testing"));
        Client systemIndexClient = systemIndexClient(descriptors);
        IndexResponse indexResponse = systemIndexClient.prepareIndex(systemIndex)
            .setSource("{\"testfield\": 1}", XContentType.JSON).get();
        assertThat(indexResponse.status().getStatus(), allOf(greaterThanOrEqualTo(200), lessThan(300)));
        indexResponse = systemIndexClient.prepareIndex(systemIndex)
            .setSource("{\"testfield\": 2}", XContentType.JSON).get();
        assertThat(indexResponse.status().getStatus(), allOf(greaterThanOrEqualTo(200), lessThan(300)));
        refresh();

        QueryBuilder query = QueryBuilders.matchQuery("testfield", 1);

        {
            SearchResponse searchResponse = client().prepareSearch(".system*")
                .setQuery(query)
                .get();
            assertHitCount(searchResponse, 0);

            searchResponse = client().prepareSearch(".system*")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort("testfield", SortOrder.ASC)
                .get();
            assertHitCount(searchResponse, 0);
        }

        {
            SearchResponse searchResponse = client().prepareSearch(systemIndex)
                .setQuery(query)
                .get();
            assertHitCount(searchResponse, 0);

            searchResponse = client().prepareSearch(systemIndex)
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort("testfield", SortOrder.ASC)
                .get();
            assertHitCount(searchResponse, 0);
        }
    }


}
