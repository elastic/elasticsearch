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

package org.elasticsearch.indices.state;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class OpenCloseIndexIT extends ESIntegTestCase {

    @Test
    public void testSimpleCloseOpen() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("test1").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test1").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    @Test(expected = IndexNotFoundException.class)
    public void testSimpleCloseMissingIndex() {
        Client client = client();
        client.admin().indices().prepareClose("test1").execute().actionGet();
    }

    @Test(expected = IndexNotFoundException.class)
    public void testSimpleOpenMissingIndex() {
        Client client = client();
        client.admin().indices().prepareOpen("test1").execute().actionGet();
    }

    @Test(expected = IndexNotFoundException.class)
    public void testCloseOneMissingIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        client.admin().indices().prepareClose("test1", "test2").execute().actionGet();
    }

    @Test
    public void testCloseOneMissingIndexIgnoreMissing() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("test1", "test2")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1");
    }

    @Test(expected = IndexNotFoundException.class)
    public void testOpenOneMissingIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        client.admin().indices().prepareOpen("test1", "test2").execute().actionGet();
    }

    @Test
    public void testOpenOneMissingIndexIgnoreMissing() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test1", "test2")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    @Test
    public void testCloseOpenMultipleIndices() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        CloseIndexResponse closeIndexResponse1 = client.admin().indices().prepareClose("test1").execute().actionGet();
        assertThat(closeIndexResponse1.isAcknowledged(), equalTo(true));
        CloseIndexResponse closeIndexResponse2 = client.admin().indices().prepareClose("test2").execute().actionGet();
        assertThat(closeIndexResponse2.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2");
        assertIndexIsOpened("test3");

        OpenIndexResponse openIndexResponse1 = client.admin().indices().prepareOpen("test1").execute().actionGet();
        assertThat(openIndexResponse1.isAcknowledged(), equalTo(true));
        OpenIndexResponse openIndexResponse2 = client.admin().indices().prepareOpen("test2").execute().actionGet();
        assertThat(openIndexResponse2.isAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

    @Test
    public void testCloseOpenWildcard() {
        Client client = client();
        createIndex("test1", "test2", "a");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("test*").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2");
        assertIndexIsOpened("a");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test*").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "a");
    }

    @Test
    public void testCloseOpenAll() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("_all").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2", "test3");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("_all").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

    @Test
    public void testCloseOpenAllWildcard() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("*").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2", "test3");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("*").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testCloseNoIndex() {
        Client client = client();
        client.admin().indices().prepareClose().execute().actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testCloseNullIndex() {
        Client client = client();
        client.admin().indices().prepareClose((String[])null).execute().actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testOpenNoIndex() {
        Client client = client();
        client.admin().indices().prepareOpen().execute().actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testOpenNullIndex() {
        Client client = client();
        client.admin().indices().prepareOpen((String[])null).execute().actionGet();
    }

    @Test
    public void testOpenAlreadyOpenedIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        //no problem if we try to open an index that's already in open state
        OpenIndexResponse openIndexResponse1 = client.admin().indices().prepareOpen("test1").execute().actionGet();
        assertThat(openIndexResponse1.isAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    @Test
    public void testCloseAlreadyClosedIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        //closing the index
        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("test1").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1");

        //no problem if we try to close an index that's already in close state
        closeIndexResponse = client.admin().indices().prepareClose("test1").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1");
    }

    @Test
    public void testSimpleCloseOpenAlias() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        IndicesAliasesResponse aliasesResponse = client.admin().indices().prepareAliases().addAlias("test1", "test1-alias").execute().actionGet();
        assertThat(aliasesResponse.isAcknowledged(), equalTo(true));

        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("test1-alias").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test1-alias").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    @Test
    public void testCloseOpenAliasMultipleIndices() {
        Client client = client();
        createIndex("test1", "test2");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        IndicesAliasesResponse aliasesResponse1 = client.admin().indices().prepareAliases().addAlias("test1", "test-alias").execute().actionGet();
        assertThat(aliasesResponse1.isAcknowledged(), equalTo(true));
        IndicesAliasesResponse aliasesResponse2 = client.admin().indices().prepareAliases().addAlias("test2", "test-alias").execute().actionGet();
        assertThat(aliasesResponse2.isAcknowledged(), equalTo(true));

        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("test-alias").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test-alias").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2");
    }

    private void assertIndexIsOpened(String... indices) {
        checkIndexState(IndexMetaData.State.OPEN, indices);
    }

    private void assertIndexIsClosed(String... indices) {
        checkIndexState(IndexMetaData.State.CLOSE, indices);
    }

    private void checkIndexState(IndexMetaData.State expectedState, String... indices) {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
        for (String index : indices) {
            IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().indices().get(index);
            assertThat(indexMetaData, notNullValue());
            assertThat(indexMetaData.getState(), equalTo(expectedState));
        }
    }

    @Test
    public void testOpenCloseWithDocs() throws IOException, ExecutionException, InterruptedException {
        String mapping = XContentFactory.jsonBuilder().
                startObject().
                startObject("type").
                startObject("properties").
                startObject("test")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject().
                        endObject().
                        endObject()
                .endObject().string();

        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type", mapping));
        ensureGreen();
        int docs = between(10, 100);
        IndexRequestBuilder[] builder = new IndexRequestBuilder[docs];
        for (int i = 0; i < docs ; i++) {
            builder[i] = client().prepareIndex("test", "type", "" + i).setSource("test", "init");
        }
        indexRandom(true, builder);
        if (randomBoolean()) {
            client().admin().indices().prepareFlush("test").setWaitIfOngoing(true).setForce(true).execute().get();
        }
        client().admin().indices().prepareClose("test").execute().get();

        // check the index still contains the records that we indexed
        client().admin().indices().prepareOpen("test").execute().get();
        ensureGreen();
        SearchResponse searchResponse = client().prepareSearch().setTypes("type").setQuery(QueryBuilders.matchQuery("test", "init")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, docs);
    }

    @Test
    public void testOpenCloseIndexWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        int docs = between(10, 100);
        for (int i = 0; i < docs ; i++) {
            client().prepareIndex("test", "type", "" + i).setSource("test", "init").execute().actionGet();
        }

        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", blockSetting);

                // Closing an index is not blocked
                CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose("test").execute().actionGet();
                assertAcked(closeIndexResponse);
                assertIndexIsClosed("test");

                // Opening an index is not blocked
                OpenIndexResponse openIndexResponse = client().admin().indices().prepareOpen("test").execute().actionGet();
                assertAcked(openIndexResponse);
                assertIndexIsOpened("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Closing an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareClose("test"));
                assertIndexIsOpened("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose("test").execute().actionGet();
        assertAcked(closeIndexResponse);
        assertIndexIsClosed("test");

        // Opening an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareOpen("test"));
                assertIndexIsClosed("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }
}