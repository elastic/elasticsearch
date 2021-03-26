/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class UpdateByQueryConflictTests extends ReindexTestCase {
    private static final String SCRIPT_LANG = "fake_lang";
    private static final String NOOP_GENERATOR = "modificationScript";
    private static final String RETURN_NOOP_FIELD = "return_noop";
    private static final String SORTING_FIELD = "num";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of(NOOP_GENERATOR, (vars) -> {
                final Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                final Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                if (source.containsKey(RETURN_NOOP_FIELD)) {
                    ctx.put("op", "noop");
                }
                return vars;
            });
        }

        @Override
        public String pluginScriptLang() {
            return SCRIPT_LANG;
        }
    }

    public void testConcurrentDocumentUpdates() throws Exception {
        internalCluster().startMasterOnlyNode();
        // Use a single thread pool for writes so we can enforce a consistent ordering
        internalCluster().startDataOnlyNode(Settings.builder().put("thread_pool.write.size", 1).build());

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());

        final int numDocs = 100;
        final int scrollSize = 50;
        final int maxDocs = 10;

        boolean scriptEnabled = randomBoolean();
        List<IndexRequestBuilder> indexRequests = new ArrayList<>(numDocs);
        int noopDocs = 0;
        for (int i = numDocs; i > 0; i--) {
            Map<String, Object> source = new HashMap<>();
            source.put(SORTING_FIELD, i);
            // Force that the first maxDocs are transformed into a noop
            if (scriptEnabled && noopDocs < maxDocs) {
                // Add a marker on the document to signal that this
                // document should return a noop in the script
                source.put(RETURN_NOOP_FIELD, true);
                noopDocs++;
            }
            indexRequests.add(client().prepareIndex(indexName).setId(Integer.toString(i)).setSource(source));
        }
        indexRandom(true, indexRequests);

        final ThreadPool threadPool = internalCluster().getDataNodeInstance(ThreadPool.class);

        final int writeThreads = threadPool.info(ThreadPool.Names.WRITE).getMax();
        assertThat(writeThreads, equalTo(1));
        final EsThreadPoolExecutor writeThreadPool = (EsThreadPoolExecutor) threadPool.executor(ThreadPool.Names.WRITE);
        final CyclicBarrier barrier = new CyclicBarrier(writeThreads + 1);
        final CountDownLatch latch = new CountDownLatch(1);

        // Block the write thread pool
        writeThreadPool.submit(() -> {
            try {
                barrier.await();
                latch.await();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        // Ensure that the write thread blocking task is currently executing
        barrier.await();

        final SearchResponse searchResponse = client().prepareSearch(indexName)
            .setSize(scrollSize)
            .addSort(SORTING_FIELD, SortOrder.DESC)
            .execute()
            .actionGet();

        // Modify a subset of the target documents concurrently
        final List<SearchHit> originalDocs = Arrays.asList(searchResponse.getHits().getHits());
        final List<SearchHit> docsModifiedConcurrently = new ArrayList<>();
        if (randomBoolean()) {
            // The entire first scroll request conflicts, forcing a second request
            docsModifiedConcurrently.addAll(originalDocs);
        } else {
            if (randomBoolean()) {
                // We can satisfy the entire update by query request with a single scroll response
                // Only the first maxDocs documents would conflict
                docsModifiedConcurrently.addAll(originalDocs.subList(0, maxDocs));
            } else {
                // Only a subset of maxDocs can be updated using the first scroll response
                // Only 1 document per "page" is not modified concurrently
                final int pages = scrollSize / maxDocs;
                for (int i = 0; i < pages; i++) {
                    final int offset = i * 10;
                    docsModifiedConcurrently.addAll(originalDocs.subList(offset, offset + maxDocs - 1));
                }

                // Force to request an additional scroll page
                assertThat(pages, lessThan(maxDocs));
            }
        }

        int conflictingUpdates = 0;
        BulkRequest conflictingUpdatesBulkRequest = new BulkRequest();
        for (SearchHit searchHit : docsModifiedConcurrently) {
            if (searchHit.getSourceAsMap().containsKey(RETURN_NOOP_FIELD) == false) {
                conflictingUpdates++;
            }

            conflictingUpdatesBulkRequest.add(createUpdatedIndexRequest(searchHit));
        }

        // The bulk request is enqueued before the update by query
        final ActionFuture<BulkResponse> bulkFuture = client().bulk(conflictingUpdatesBulkRequest);

        // Ensure that the concurrent writes are enqueued before the update by query request is sent
        assertBusy(() -> assertThat(writeThreadPool.getQueue().size(), equalTo(1)));

        final UpdateByQueryRequestBuilder updateByQueryRequestBuilder = updateByQuery()
            .source(indexName)
            .maxDocs(maxDocs)
            .abortOnVersionConflict(false);

        if (scriptEnabled) {
            final Script script = new Script(ScriptType.INLINE, SCRIPT_LANG, NOOP_GENERATOR, Collections.emptyMap());
            updateByQueryRequestBuilder.script(script);
        }

        final SearchRequestBuilder source = updateByQueryRequestBuilder.source();
        source.setSize(scrollSize);
        source.addSort(SORTING_FIELD, SortOrder.DESC);
        final ActionFuture<BulkByScrollResponse> updateByQueryResponse = updateByQueryRequestBuilder.execute();

        assertBusy(() -> assertThat(writeThreadPool.getQueue().size(), equalTo(2)));

        // Allow tasks from the write thread to make progress
        latch.countDown();

        final BulkResponse bulkItemResponses = bulkFuture.actionGet();
        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
            assertThat(bulkItemResponse.isFailed(), is(false));
        }

        final BulkByScrollResponse bulkByScrollResponse = updateByQueryResponse.actionGet();
        assertThat(bulkByScrollResponse.getUpdated(), is((long) maxDocs));
        assertThat(bulkByScrollResponse.getVersionConflicts(), is((long) conflictingUpdates));
        if (scriptEnabled) {
            assertThat(bulkByScrollResponse.getNoops(), is((long) maxDocs));
        }
    }

    private IndexRequest createUpdatedIndexRequest(SearchHit searchHit) {
        final BytesReference sourceRef = searchHit.getSourceRef();
        final XContentType xContentType = sourceRef != null ? XContentHelper.xContentType(sourceRef) : null;
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(searchHit.getIndex());
        indexRequest.id(searchHit.getId());
        indexRequest.source(sourceRef, xContentType);
        indexRequest.setIfSeqNo(searchHit.getSeqNo());
        indexRequest.setIfPrimaryTerm(searchHit.getPrimaryTerm());
        return indexRequest;
    }
}
