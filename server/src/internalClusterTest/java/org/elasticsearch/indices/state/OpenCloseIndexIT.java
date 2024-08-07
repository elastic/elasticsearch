/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.state;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsClosed;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsOpened;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class OpenCloseIndexIT extends ESIntegTestCase {
    public void testSimpleCloseOpen() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("test1").get();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test1").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    public void testSimpleOpenMissingIndex() {
        Exception e = expectThrows(IndexNotFoundException.class, indicesAdmin().prepareOpen("test1"));
        assertThat(e.getMessage(), is("no such index [test1]"));
    }

    public void testOpenOneMissingIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        Exception e = expectThrows(IndexNotFoundException.class, client.admin().indices().prepareOpen("test1", "test2"));
        assertThat(e.getMessage(), is("no such index [test2]"));
    }

    public void testOpenOneMissingIndexIgnoreMissing() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        OpenIndexResponse openIndexResponse = client.admin()
            .indices()
            .prepareOpen("test1", "test2")
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    public void testCloseOpenMultipleIndices() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse closeIndexResponse1 = client.admin().indices().prepareClose("test1").get();
        assertThat(closeIndexResponse1.isAcknowledged(), equalTo(true));
        AcknowledgedResponse closeIndexResponse2 = client.admin().indices().prepareClose("test2").get();
        assertThat(closeIndexResponse2.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2");
        assertIndexIsOpened("test3");

        OpenIndexResponse openIndexResponse1 = client.admin().indices().prepareOpen("test1").get();
        assertThat(openIndexResponse1.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse1.isShardsAcknowledged(), equalTo(true));
        OpenIndexResponse openIndexResponse2 = client.admin().indices().prepareOpen("test2").get();
        assertThat(openIndexResponse2.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse2.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

    public void testCloseOpenWildcard() {
        Client client = client();
        createIndex("test1", "test2", "a");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("test*").get();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2");
        assertIndexIsOpened("a");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test*").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "a");
    }

    public void testCloseOpenAll() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("_all").get();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2", "test3");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("_all").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

    public void testCloseOpenAllWildcard() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("*").get();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2", "test3");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("*").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

    public void testOpenNoIndex() {
        Exception e = expectThrows(ActionRequestValidationException.class, indicesAdmin().prepareOpen());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testOpenNullIndex() {
        Exception e = expectThrows(ActionRequestValidationException.class, indicesAdmin().prepareOpen((String[]) null));
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testOpenAlreadyOpenedIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        // no problem if we try to open an index that's already in open state
        OpenIndexResponse openIndexResponse1 = client.admin().indices().prepareOpen("test1").get();
        assertThat(openIndexResponse1.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse1.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    public void testSimpleCloseOpenAlias() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse aliasesResponse = client.admin().indices().prepareAliases().addAlias("test1", "test1-alias").get();
        assertThat(aliasesResponse.isAcknowledged(), equalTo(true));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("test1-alias").get();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test1-alias").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    public void testCloseOpenAliasMultipleIndices() {
        Client client = client();
        createIndex("test1", "test2");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse aliasesResponse1 = client.admin().indices().prepareAliases().addAlias("test1", "test-alias").get();
        assertThat(aliasesResponse1.isAcknowledged(), equalTo(true));
        AcknowledgedResponse aliasesResponse2 = client.admin().indices().prepareAliases().addAlias("test2", "test-alias").get();
        assertThat(aliasesResponse2.isAcknowledged(), equalTo(true));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("test-alias").get();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test-alias").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2");
    }

    public void testOpenWaitingForActiveShardsFailed() throws Exception {
        Client client = client();
        Settings settings = indexSettings(1, 0).build();
        assertAcked(client.admin().indices().prepareCreate("test").setSettings(settings).get());
        assertAcked(client.admin().indices().prepareClose("test").get());

        OpenIndexResponse response = client.admin()
            .indices()
            .prepareOpen("test")
            .setTimeout(TimeValue.timeValueMillis(100))
            .setWaitForActiveShards(2)
            .get();
        assertThat(response.isShardsAcknowledged(), equalTo(false));
        assertBusy(
            () -> assertThat(
                client.admin().cluster().prepareState().get().getState().metadata().index("test").getState(),
                equalTo(IndexMetadata.State.OPEN)
            )
        );
        ensureGreen("test");
    }

    public void testOpenCloseWithDocs() throws IOException, ExecutionException, InterruptedException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("test")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
        );

        assertAcked(indicesAdmin().prepareCreate("test").setMapping(mapping));
        ensureGreen();
        int docs = between(10, 100);
        IndexRequestBuilder[] builder = new IndexRequestBuilder[docs];
        for (int i = 0; i < docs; i++) {
            builder[i] = prepareIndex("test").setId("" + i).setSource("test", "init");
        }
        indexRandom(true, builder);
        if (randomBoolean()) {
            indicesAdmin().prepareFlush("test").setForce(true).execute().get();
        }
        indicesAdmin().prepareClose("test").execute().get();

        // check the index still contains the records that we indexed
        indicesAdmin().prepareOpen("test").execute().get();
        ensureGreen();
        assertHitCountAndNoFailures(prepareSearch().setQuery(QueryBuilders.matchQuery("test", "init")), docs);
    }

    public void testOpenCloseIndexWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            prepareIndex("test").setId("" + i).setSource("test", "init").get();
        }

        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", blockSetting);

                // Closing an index is not blocked
                AcknowledgedResponse closeIndexResponse = indicesAdmin().prepareClose("test").get();
                assertAcked(closeIndexResponse);
                assertIndexIsClosed("test");

                // Opening an index is not blocked
                OpenIndexResponse openIndexResponse = indicesAdmin().prepareOpen("test").get();
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
                assertBlocked(indicesAdmin().prepareClose("test"));
                assertIndexIsOpened("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        AcknowledgedResponse closeIndexResponse = indicesAdmin().prepareClose("test").get();
        assertAcked(closeIndexResponse);
        assertIndexIsClosed("test");

        // Opening an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(indicesAdmin().prepareOpen("test"));
                assertIndexIsClosed("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }

    public void testTranslogStats() throws Exception {
        final String indexName = "test";
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());

        final int nbDocs = randomIntBetween(0, 50);
        int uncommittedOps = 0;
        for (long i = 0; i < nbDocs; i++) {
            final DocWriteResponse indexResponse = prepareIndex(indexName).setId(Long.toString(i)).setSource("field", i).get();
            assertThat(indexResponse.status(), is(RestStatus.CREATED));

            if (rarely()) {
                indicesAdmin().prepareFlush(indexName).get();
                uncommittedOps = 0;
            } else {
                uncommittedOps += 1;
            }
        }

        final int uncommittedTranslogOps = uncommittedOps;
        assertBusy(() -> {
            IndicesStatsResponse stats = indicesAdmin().prepareStats(indexName).clear().setTranslog(true).get();
            assertThat(stats.getIndex(indexName), notNullValue());
            assertThat(
                stats.getIndex(indexName).getPrimaries().getTranslog().estimatedNumberOfOperations(),
                equalTo(uncommittedTranslogOps)
            );
            assertThat(stats.getIndex(indexName).getPrimaries().getTranslog().getUncommittedOperations(), equalTo(uncommittedTranslogOps));
        });

        assertAcked(indicesAdmin().prepareClose("test"));

        IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
        IndicesStatsResponse stats = indicesAdmin().prepareStats(indexName)
            .setIndicesOptions(indicesOptions)
            .clear()
            .setTranslog(true)
            .get();
        assertThat(stats.getIndex(indexName), notNullValue());
        assertThat(stats.getIndex(indexName).getPrimaries().getTranslog().estimatedNumberOfOperations(), equalTo(0));
        assertThat(stats.getIndex(indexName).getPrimaries().getTranslog().getUncommittedOperations(), equalTo(0));
    }
}
