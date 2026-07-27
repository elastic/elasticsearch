/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.diversification.DiversifyRetrieverBuilder;
import org.elasticsearch.search.diversification.ResultDiversificationType;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder.RetrieverSource;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Reproduces <a href="https://github.com/elastic/elasticsearch/issues/154748">GitHub issue #154748</a>:
 * the {@code diversify} retriever returns empty hits when the diversification field is a
 * {@code semantic_text} field and the coordinating node does not host all of the queried shards.
 *
 * <p>The bug is a serialization failure: when the inner sub-search fetches {@code _inference_fields},
 * the value fetcher returns raw {@code SemanticTextField} objects as {@link org.elasticsearch.common.document.DocumentField}
 * values. When those hits are transported from a data node back to the coordinator,
 * {@code StreamOutput.writeGenericValue} throws because {@code SemanticTextField} was not registered
 * as a generic-writable type. The shard failure is swallowed and the result is empty.
 *
 * <p>This test uses a coordinating-only node ({@code numClientNodes = 1}) as the search client, with
 * all index shards on the separate data nodes, so every matching hit must cross the transport boundary
 * — deterministically exercising the failing serialization path.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 1, supportsDedicatedMasters = false)
public class SemanticTextDiversifyRetrieverIT extends ESIntegTestCase {

    private static final String INFERENCE_ID = "dense-test-endpoint";
    private static final String CONTENT_FIELD = "content";

    // Dimension count must match the mock dense service settings.
    private static final int DIMS = 4;

    // A literal query vector (dims = 4) used as the MMR diversity vector.
    private static final float[] QUERY_VECTOR = { 0.4f, 0.2f, 0.3f, 0.3f };

    private static final Map<String, Object> DENSE_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        DIMS,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    private String indexName;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class, FakeMlPlugin.class);
    }

    @After
    public void cleanUp() {
        if (indexName != null) {
            IntegrationTestUtils.deleteIndex(client(), indexName);
        }
        IntegrationTestUtils.deleteInferenceEndpoint(client(), TaskType.TEXT_EMBEDDING, INFERENCE_ID);
    }

    /**
     * Creates an index with a single {@code semantic_text} field backed by a mock dense embedding endpoint,
     * spreads shards across data nodes, indexes several documents, then runs the {@code diversify}
     * retriever issued through the coordinating-only node.
     *
     * <p>Pre-fix: the coordinating node fails to deserialize the {@code SemanticTextField} values
     * transported from the data nodes → all shards fail → zero hits.
     * Post-fix: all shards succeed and the retriever returns {@code size} hits.
     */
    public void testDiversifyRetrieverOverSemanticTextFieldMultiNode() throws Exception {
        IntegrationTestUtils.createInferenceEndpoint(client(), TaskType.TEXT_EMBEDDING, INFERENCE_ID, DENSE_SERVICE_SETTINGS);

        indexName = randomIdentifier();
        int numDataNodes = internalCluster().numDataNodes();
        Settings indexSettings = Settings.builder().put("index.number_of_shards", numDataNodes).put("index.number_of_replicas", 0).build();

        XContentBuilder mapping = IntegrationTestUtils.generateSemanticTextMapping(Map.of(CONTENT_FIELD, INFERENCE_ID));
        assertAcked(prepareCreate(indexName).setSettings(indexSettings).setMapping(mapping));

        // Index enough documents so the diversify retriever has something to actually trim.
        String[] docs = {
            "Wireless noise cancelling headphones with deep bass",
            "Over-ear headphones with active noise cancellation and long battery",
            "Bluetooth earbuds with transparency mode and spatial audio",
            "Premium studio monitor headphones, wired, flat frequency response",
            "Sport earbuds with secure fit and sweat resistance",
            "Open-back audiophile headphones with wide soundstage",
            "True wireless earbuds with adaptive noise cancelling",
            "Gaming headset with surround sound and detachable mic" };

        BulkRequestBuilder bulk = client().prepareBulk(indexName);
        for (String doc : docs) {
            bulk.add(client().prepareIndex(indexName).setSource(Map.of(CONTENT_FIELD, doc)));
        }
        bulk.setRefreshPolicy("wait_for").get(TEST_REQUEST_TIMEOUT);
        ensureGreen(indexName);

        int diversifySize = 3;
        int rankWindowSize = 10;

        RetrieverSource inner = RetrieverSource.from(
            new StandardRetrieverBuilder(new SemanticQueryBuilder(CONTENT_FIELD, "wireless noise cancelling headphones"))
        );
        DiversifyRetrieverBuilder retriever = new DiversifyRetrieverBuilder(
            inner,
            ResultDiversificationType.MMR,
            CONTENT_FIELD,
            rankWindowSize,
            diversifySize,
            new VectorData(QUERY_VECTOR),
            null,
            0.9f
        );

        SearchSourceBuilder source = new SearchSourceBuilder().retriever(retriever).size(diversifySize);
        SearchRequest request = new SearchRequest(new String[] { indexName }, source);

        // Issue the search from the coordinating-only node: it holds no shards, so every hit
        // must be serialized from a data node across the transport layer. Pre-fix this triggers
        // the SemanticTextField serialization failure and all shards fail.
        assertResponse(internalCluster().coordOnlyNodeClient().search(request), response -> {
            assertThat(
                "Expected no shard failures, but got: " + response.getShardFailures().length,
                response.getFailedShards(),
                equalTo(0)
            );
            assertThat("All shards should have succeeded", response.getSuccessfulShards(), equalTo(response.getTotalShards()));
            assertThat("Diversify retriever should return non-empty hits", response.getHits().getHits().length, greaterThan(0));
            assertThat(
                "Diversify retriever should return exactly [size] hits",
                response.getHits().getHits().length,
                equalTo(diversifySize)
            );
        });
    }
}
