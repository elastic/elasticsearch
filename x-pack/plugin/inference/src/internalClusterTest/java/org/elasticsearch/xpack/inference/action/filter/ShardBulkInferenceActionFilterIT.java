/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticTextInput;
import static org.hamcrest.Matchers.equalTo;

public class ShardBulkInferenceActionFilterIT extends ESIntegTestCase {

    public static final String INDEX_NAME = "test-index";

    @Before
    public void setup() throws Exception {
        Utils.storeSparseModel(client());
        Utils.storeDenseModel(
            client(),
            randomIntBetween(1, 100),
            // dot product means that we need normalized vectors; it's not worth doing that in this test
            randomValueOtherThan(SimilarityMeasure.DOT_PRODUCT, () -> randomFrom(SimilarityMeasure.values())),
            // TODO: Allow element type BIT once TestDenseInferenceServiceExtension supports it
            randomValueOtherThan(DenseVectorFieldMapper.ElementType.BIT, () -> randomFrom(DenseVectorFieldMapper.ElementType.values()))
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(Utils.TestInferencePlugin.class);
    }

    public void testBulkOperations() throws Exception {
        Map<String, Integer> shardsSettings = Collections.singletonMap(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10));
        indicesAdmin().prepareCreate(INDEX_NAME)
            .setMapping(
                String.format(
                    Locale.ROOT,
                    """
                        {
                            "properties": {
                                "sparse_field": {
                                    "type": "semantic_text",
                                    "inference_id": "%s"
                                },
                                "dense_field": {
                                    "type": "semantic_text",
                                    "inference_id": "%s"
                                }
                            }
                        }
                        """,
                    TestSparseInferenceServiceExtension.TestInferenceService.NAME,
                    TestDenseInferenceServiceExtension.TestInferenceService.NAME
                )
            )
            .setSettings(shardsSettings)
            .get();

        int totalBulkReqs = randomIntBetween(2, 100);
        long totalDocs = 0;
        for (int bulkReqs = 0; bulkReqs < totalBulkReqs; bulkReqs++) {
            BulkRequestBuilder bulkReqBuilder = client().prepareBulk();
            int totalBulkSize = randomIntBetween(1, 100);
            for (int bulkSize = 0; bulkSize < totalBulkSize; bulkSize++) {
                String id = Long.toString(totalDocs);
                boolean isIndexRequest = randomBoolean();
                Map<String, Object> source = new HashMap<>();
                source.put("sparse_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
                source.put("dense_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
                if (isIndexRequest) {
                    bulkReqBuilder.add(new IndexRequestBuilder(client()).setIndex(INDEX_NAME).setId(id).setSource(source));
                    totalDocs++;
                } else {
                    boolean isUpsert = randomBoolean();
                    UpdateRequestBuilder request = new UpdateRequestBuilder(client()).setIndex(INDEX_NAME).setDoc(source);
                    if (isUpsert || totalDocs == 0) {
                        request.setDocAsUpsert(true);
                        totalDocs++;
                    } else {
                        // Update already existing document
                        id = Long.toString(randomLongBetween(0, totalDocs - 1));
                    }
                    request.setId(id);
                    bulkReqBuilder.add(request);
                }
            }
            BulkResponse bulkResponse = bulkReqBuilder.get();
            if (bulkResponse.hasFailures()) {
                // Get more details in case something fails
                for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                    if (bulkItemResponse.isFailed()) {
                        fail(
                            bulkItemResponse.getFailure().getCause(),
                            "Failed to index document %s: %s",
                            bulkItemResponse.getId(),
                            bulkItemResponse.getFailureMessage()
                        );
                    }
                }
            }
            assertFalse(bulkResponse.hasFailures());
        }

        client().admin().indices().refresh(new RefreshRequest(INDEX_NAME)).get();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
        SearchResponse searchResponse = client().search(new SearchRequest(INDEX_NAME).source(sourceBuilder)).get();
        try {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(totalDocs));
        } finally {
            searchResponse.decRef();
        }
    }

}
