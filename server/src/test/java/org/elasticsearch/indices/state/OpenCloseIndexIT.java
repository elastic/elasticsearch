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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class OpenCloseIndexIT extends ESIntegTestCase {
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
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    public void testSimpleCloseMissingIndex() {
        Client client = client();
        Exception e = expectThrows(IndexNotFoundException.class, () ->
            client.admin().indices().prepareClose("test1").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testSimpleOpenMissingIndex() {
        Client client = client();
        Exception e = expectThrows(IndexNotFoundException.class, () ->
            client.admin().indices().prepareOpen("test1").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testCloseOneMissingIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        Exception e = expectThrows(IndexNotFoundException.class, () ->
            client.admin().indices().prepareClose("test1", "test2").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

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

    public void testOpenOneMissingIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        Exception e = expectThrows(IndexNotFoundException.class, () ->
            client.admin().indices().prepareOpen("test1", "test2").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testOpenOneMissingIndexIgnoreMissing() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test1", "test2")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

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
        assertThat(openIndexResponse1.isShardsAcknowledged(), equalTo(true));
        OpenIndexResponse openIndexResponse2 = client.admin().indices().prepareOpen("test2").execute().actionGet();
        assertThat(openIndexResponse2.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse2.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

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
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "a");
    }

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
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

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
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

    public void testCloseNoIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
            client.admin().indices().prepareClose().execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testCloseNullIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
            client.admin().indices().prepareClose((String[])null).execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testOpenNoIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
            client.admin().indices().prepareOpen().execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testOpenNullIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
            client.admin().indices().prepareOpen((String[])null).execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testOpenAlreadyOpenedIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        //no problem if we try to open an index that's already in open state
        OpenIndexResponse openIndexResponse1 = client.admin().indices().prepareOpen("test1").execute().actionGet();
        assertThat(openIndexResponse1.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse1.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

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
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

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
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2");
    }

    public void testOpenWaitingForActiveShardsFailed() throws Exception {
        Client client = client();
        Settings settings = Settings.builder()
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
        assertAcked(client.admin().indices().prepareCreate("test").setSettings(settings).get());
        assertAcked(client.admin().indices().prepareClose("test").get());

        OpenIndexResponse response = client.admin().indices().prepareOpen("test").setTimeout("100ms").setWaitForActiveShards(2).get();
        assertThat(response.isShardsAcknowledged(), equalTo(false));
        assertBusy(() -> assertThat(client.admin().cluster().prepareState().get().getState().metaData().index("test").getState(),
            equalTo(IndexMetaData.State.OPEN)));
        ensureGreen("test");
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

    public void testOpenCloseWithDocs() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().
                startObject().
                startObject("type").
                startObject("properties").
                startObject("test")
                .field("type", "keyword")
                .endObject().
                        endObject().
                        endObject()
                .endObject());

        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type", mapping, XContentType.JSON));
        ensureGreen();
        int docs = between(10, 100);
        IndexRequestBuilder[] builder = new IndexRequestBuilder[docs];
        for (int i = 0; i < docs ; i++) {
            builder[i] = client().prepareIndex("test", "type", "" + i).setSource("test", "init");
        }
        indexRandom(true, builder);
        if (randomBoolean()) {
            client().admin().indices().prepareFlush("test").setForce(true).execute().get();
        }
        close("test");

        // check the index still contains the records that we indexed
        open("test");
        ensureGreen();
        waitForOpen("test");

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchQuery("test", "init")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, docs);
    }

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
                assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
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
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_READ_ONLY_ALLOW_DELETE, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareOpen("test"));
                assertIndexIsClosed("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }

    private void close(String... indices) {
        logger.info("--> closing: {}", Arrays.toString(indices));
        CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose(indices).execute().actionGet();
        assertAcked(closeIndexResponse);
        assertIndexIsClosed(indices);
    }

    private void open(String... indices) {
        logger.info("--> opening: {}", Arrays.toString(indices));
        OpenIndexResponse openIndexResponse = client().admin().indices().prepareOpen(indices).execute().actionGet();
        assertAcked(openIndexResponse);
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened(indices);
    }

    public void testCloseOpenThenIndex() throws Exception {
        Client client = client();
        createIndex("test");
        waitForRelocation(ClusterHealthStatus.GREEN);
        int docs = between(1, 10);
        IndexRequestBuilder[] builder = new IndexRequestBuilder[docs];
        for (int i = 0; i < docs ; i++) {
            builder[i] = client().prepareIndex("test", "type", "" + i).setSource("test", "init");
        }
        indexRandom(true, builder);

        assertHitCount(client.prepareSearch("test").get(), docs);

        close("test");
        open("test");
        ensureGreen("test");
        waitForOpen("test");

        client().prepareIndex("test", "type", "" + docs + 1).setSource("test", "foo").get();
        refresh("test");
        assertHitCount(client.prepareSearch("test").get(), docs + 1);
    }

    public void testDeleteClosedIndex() {
        Client client = client();
        createIndex("test");
        waitForRelocation(ClusterHealthStatus.GREEN);

        close("test");

        assertAcked(client.admin().indices().prepareDelete("test"));
        // No weirdness with recreating the index with the same name
        createIndex("test");
    }

    public void   testClosedAndIncreaseReplicas() throws Exception {
        assumeTrue("need to be able to increment replicas at least to 1 but there are only " +
            cluster().numDataNodes() + " data node(s)", cluster().numDataNodes() > 1);
        Client client = client();
        createIndex("test", Settings.builder().put("index.number_of_replicas", 0).build());
        ensureGreen("test");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        int docs = randomIntBetween(20, 40);
        for (int i = 0; i < docs; i++) {
            builders.add(client.prepareIndex("test", "_doc", "" + i).setSource("{\"foo\": " + i + "}", XContentType.JSON));
        }
        indexRandom(false, builders);

        close("test");

        int replicaCount = randomIntBetween(1, maximumNumberOfReplicas());
        logger.info("--> updating replicas to {}", replicaCount);
        client.admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.number_of_replicas", replicaCount).build()).get();
        ensureGreen("test");

        open("test");
        waitForOpen("test");

        assertHitCount(client.prepareSearch("test").get(), docs);
    }

    public void testClosedAndRestartDataNode() throws Exception {
        assumeTrue("need to have at least to 2 data nodes " + cluster().numDataNodes() + " data node(s)", cluster().numDataNodes() > 1);
        createIndex("test");
        ensureGreen("test");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        int docs = randomIntBetween(20, 40);
        for (int i = 0; i < docs; i++) {
            builders.add(client().prepareIndex("test", "_doc", "" + i).setSource("{\"foo\": " + i + "}", XContentType.JSON));
        }
        indexRandom(false, builders);

        close("test");

        internalCluster().restartRandomDataNode();
        assertIndexIsClosed("test");

        open("test");
        ensureGreen("test");

        client().prepareIndex("test", "_doc").setSource("{\"foo\": 1000}", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        assertHitCount(client().prepareSearch("test").get(), docs + 1);
    }

    public void testCloseAndRestartCluster() throws Exception {
        assumeTrue("need to have at least to 2 data nodes " + cluster().numDataNodes() + " data node(s)", cluster().numDataNodes() > 1);
        createIndex("test");
        ensureGreen("test");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        int docs = randomIntBetween(20, 40);
        for (int i = 0; i < docs; i++) {
            builders.add(client().prepareIndex("test", "_doc", "" + i).setSource("{\"foo\": " + i + "}", XContentType.JSON));
        }
        indexRandom(false, builders);

        close("test");

        internalCluster().fullRestart();
        assertBusy(() -> assertIndexIsClosed("test"));

        open("test");
        ensureGreen("test");

        client().prepareIndex("test", "_doc").setSource("{\"foo\": 1000}", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        assertHitCount(client().prepareSearch("test").get(), docs + 1);
    }

    public void testCloseOpenWhileClusterStateUpdates() throws Exception {
        Client client = client();
        createIndex("test");
        waitForRelocation(ClusterHealthStatus.GREEN);
        int docs = between(1, 10);
        IndexRequestBuilder[] builder = new IndexRequestBuilder[docs];
        for (int i = 0; i < docs ; i++) {
            builder[i] = client().prepareIndex("test", "_doc", "" + i).setSource("test", "init");
        }
        indexRandom(true, builder);

        assertHitCount(client.prepareSearch("test").get(), docs);

        final AtomicBoolean run = new AtomicBoolean(true);
        Thread clusterUpdateThread = new Thread(() -> {
            // Spin in a loop updating the cluster state
            while (run.get()) {
                client().admin().cluster().prepareReroute().execute();
                try {
                    Thread.sleep(randomIntBetween(500, 1500));
                } catch (InterruptedException e) {
                    // Ignore if this is interrupted
                }
            }
        });

        clusterUpdateThread.start();

        int iters = scaledRandomIntBetween(5, 50);
        for (int i = 0; i < iters; i++) {
            close("test");
            open("test");
            if (randomBoolean()) {
                client().admin().indices().prepareRefresh("test").get();
            }
            if (randomBoolean()) {
                ensureGreen("test");
                logger.info("--> indexing a random doc");
                try {
                    client().prepareIndex("test", "_doc").setSource("foo", "bar").get();
                    docs++;
                } catch (Exception e) {
                    // that's fine, we aren't waiting on close/open and it's asynchronous
                }
            }
        }

        waitForOpen("test");
        ensureGreen("test");
        refresh("test");

        run.set(false);
        clusterUpdateThread.join(10000);

        assertHitCount(client.prepareSearch("test").get(), docs);
    }
}
