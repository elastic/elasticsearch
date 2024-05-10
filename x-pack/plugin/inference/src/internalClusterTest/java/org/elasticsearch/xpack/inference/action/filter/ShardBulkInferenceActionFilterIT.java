/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.Model;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ShardBulkInferenceActionFilterIT extends ESIntegTestCase {

    public static final String INDEX_NAME = "test-index";

    @Before
    public void setup() throws Exception {
        storeModel();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TestInferencePlugin.class);
    }

    public void testBulkOperations() throws Exception {
        Map<String, Integer> shardsSettings = Collections.singletonMap(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10));
        indicesAdmin().prepareCreate(INDEX_NAME)
            .setMapping("""
                {
                    "properties": {
                        "foo": {
                            "type": "semantic_text",
                            "inference_id": "test-inference"
                        }
                    }
                }
                """)
            .setSettings(shardsSettings)
            .get();

        int totalBulkReqs = randomIntBetween(2, 100);
        long totalDocs = 0;
        for(int bulkReqs = 0; bulkReqs < totalBulkReqs; bulkReqs++) {
            BulkRequestBuilder bulkReqBuilder = client().prepareBulk();
            int totalBulkSize = randomIntBetween(1, 100);
            for(int bulkSize = 0; bulkSize < totalBulkSize; bulkSize++) {
                String id = Long.toString(totalDocs);
                boolean isIndexRequest = randomBoolean();
                String fieldValue = isIndexRequest && rarely() ? null : randomAlphaOfLengthBetween(0, 1000);
                Map<String, Object> source = Collections.singletonMap("foo", fieldValue);
                if (isIndexRequest) {
                    bulkReqBuilder.add(
                        new IndexRequestBuilder(client())
                            .setIndex(INDEX_NAME)
                            .setId(id)
                            .setSource(source)
                    );
                    totalDocs++;
                } else {
                    boolean isUpsert = randomBoolean();
                    UpdateRequestBuilder request = new UpdateRequestBuilder(client())
                        .setIndex(INDEX_NAME)
                        .setDoc(source);
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
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(totalDocs));
        searchResponse.decRef();
    }

    private void storeModel() throws Exception {
        ModelRegistry modelRegistry = new ModelRegistry(client());

        String inferenceEntityId = "test-inference";
        Model model = new TestSparseInferenceServiceExtension.TestSparseModel(
            inferenceEntityId,
            new TestSparseInferenceServiceExtension.TestServiceSettings(inferenceEntityId, null, false)
        );
        AtomicReference<Boolean> storeModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> modelRegistry.storeModel(model, listener), storeModelHolder, exceptionHolder);

        assertThat(storeModelHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    private <T> void blockingCall(Consumer<ActionListener<T>> function, AtomicReference<T> response, AtomicReference<Exception> error)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(r -> {
            response.set(r);
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        });

        function.accept(listener);
        latch.await();
    }

    public static class TestInferencePlugin extends InferencePlugin {
        public TestInferencePlugin(Settings settings) {
            super(settings);
        }

        @Override
        public List<InferenceServiceExtension.Factory> getInferenceServiceFactories() {
            return List.of(
                TestSparseInferenceServiceExtension.TestInferenceService::new,
                TestDenseInferenceServiceExtension.TestInferenceService::new
            );
        }
    }
}
