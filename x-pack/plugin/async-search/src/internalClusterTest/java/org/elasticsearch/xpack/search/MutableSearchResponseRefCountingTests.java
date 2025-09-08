
package org.elasticsearch.xpack.search;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationReduceContext;

import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;

import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;

public class MutableSearchResponseRefCountingTests extends ESTestCase {

    private TestThreadPool threadPool;
    private NoOpClient client;

    @Before
    public void setup() {
        this.threadPool = new TestThreadPool(getTestName());
        this.client = new NoOpClient(threadPool);
    }

    @After
    public void cleanup() throws Exception {
        terminate(threadPool);
    }

    public void testFailsToIncrementRefCountAfterClose() {
        int totalShards = 1;
        int skippedShards = 0;

        // Create search response - (refCount -> 1)
        SearchResponse searchResponse = createSearchResponse(totalShards, totalShards, skippedShards);

        // Take a ref - (refCount -> 2)
        MutableSearchResponse msr = new MutableSearchResponse(threadPool.getThreadContext());
        msr.updateShardsAndClusters(totalShards, skippedShards, null);
        msr.updateFinalResponse(searchResponse, false);

        // Drop local ref - (refCount -> 1)
        searchResponse.decRef();

        // Release ref - (refCount -> 0)
        msr.close();

        AsyncSearchResponse resp = msr.toAsyncSearchResponse(createAsyncSearchTask(), System.currentTimeMillis() + 60000, false);
        assertNull(resp.getSearchResponse());
        assertNotNull(resp.getFailure());
        assertThat(resp.getFailure().getMessage(), containsString("async-search result, no longer available"));
    }

    private AsyncSearchTask createAsyncSearchTask() {
        return new AsyncSearchTask(
            1L,
            "search",
            "indices:data/read/search",
            TaskId.EMPTY_TASK_ID,
            () -> "debug",
            TimeValue.timeValueMinutes(1),
            Map.of(),
            Map.of(),
            new AsyncExecutionId("debug", new TaskId("node", 1L)),
            client,
            threadPool,
            isCancelled -> ()
                -> new AggregationReduceContext.ForFinal(null, null, null, null, null, PipelineAggregator.PipelineTree.EMPTY)
        );
    }

    private SearchResponse createSearchResponse(int totalShards, int successfulShards, int skippedShards) {
        return new SearchResponse(
            SearchHits.empty(Lucene.TOTAL_HITS_GREATER_OR_EQUAL_TO_ZERO, Float.NaN),
            null,
            null,
            false,
            false,
            null,
            0,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            1L,
            ShardSearchFailure.EMPTY_ARRAY,
            null
        );
    }
}
