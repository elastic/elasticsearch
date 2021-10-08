/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class CombinedResponseTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() throws Exception {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
        threadPool.shutdown();
    }

    public void testNoMerge() {
        PlainActionFuture<CombinedResponse> listener = new PlainActionFuture<>();
        final ExecutorService executor = threadPool.executor(ThreadPool.Names.SEARCH);
        final CombinedResponse.Builder builder = CombinedResponse.newBuilder(MergeResultsMode.NO_MERGE, randomBoolean(),
            field -> false, executor, listener);
        expectThrows(UnsupportedOperationException.class,
            () -> builder.addMergedResponse(Collections.emptyList(), emptyMap()));

        final Map<String, FieldCapabilitiesIndexResponse> responses = new HashMap<>();
        final Map<String, Exception> failures = new HashMap<>();

        // Response from the first node
        {
            final int numIndices = randomIntBetween(1, 10);
            final List<FieldCapabilitiesIndexResponse> firstResponse = new ArrayList<>();
            for (int i = 0; i < numIndices; i++) {
                String index = randomBoolean() ? "index-" + i : "local-" + i;
                FieldCapabilitiesIndexResponse response = FieldCapTestHelper.randomIndexResponse(index, randomBoolean());
                if (response.canMatch()) {
                    responses.put(index, response);
                }
                firstResponse.add(response);
            }
            builder.addIndexResponses(firstResponse);
        }
        assertFalse(listener.isDone());

        // Response from the second node
        {
            final int numIndices = randomIntBetween(1, 10);
            final List<FieldCapabilitiesIndexResponse> secondResponse = new ArrayList<>();
            for (int i = 0; i < numIndices; i++) {
                String index = randomBoolean() ? "index-" + i : "remote-" + i;
                FieldCapabilitiesIndexResponse response = FieldCapTestHelper.randomIndexResponse(index, randomBoolean());
                if (response.canMatch()) {
                    responses.put(index, response); // we don't override the existing index response
                }
                secondResponse.add(response);
            }
            builder.addIndexResponses(secondResponse);
        }
        assertFalse(listener.isDone());

        // Add some failures
        {
            final int numIndices = randomIntBetween(0, 5);
            for (int i = 0; i < numIndices; i++) {
                final String index = "index-" + i;
                final Exception e = randomFrom(new IllegalStateException("exception for " + index), new IndexNotFoundException(index));
                builder.addIndexFailure(index, e);
                if (responses.containsKey(index) == false) {
                    failures.put(index, e);
                }
            }
        }
        assertFalse(listener.isDone());

        builder.complete();
        assertTrue(listener.isDone());
        final CombinedResponse combinedResponse = listener.actionGet();
        assertThat(combinedResponse.getIndexResponses(), hasSize(responses.size()));
        for (FieldCapabilitiesIndexResponse resp : combinedResponse.getIndexResponses()) {
            assertThat(resp, sameInstance(responses.get(resp.getIndexName())));
        }
        assertThat(combinedResponse.getIndexFailures(), hasSize(failures.size()));
        for (FieldCapabilitiesFailure indexFailure : combinedResponse.getIndexFailures()) {
            for (String index : indexFailure.getIndices()) {
                assertThat(indexFailure.getException(), sameInstance(failures.get(index)));
            }
        }
    }

    public void testSimplePartialMerge() {
        final ExecutorService executor = randomFrom(threadPool.executor(ThreadPool.Names.SEARCH), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        final PlainActionFuture<CombinedResponse> listener = new PlainActionFuture<>();

        Map<String, IndexFieldCapabilities> index1 = new HashMap<>();
        index1.put("seq_no", new IndexFieldCapabilities("seq_no", "long", true, true, false, emptyMap()));
        index1.put("distance", new IndexFieldCapabilities("distance", "int", false, true, true, singletonMap("unit", "meter")));

        Map<String, IndexFieldCapabilities> index2 = new HashMap<>();
        index2.put("seq_no", new IndexFieldCapabilities("seq_no", "long", true, true, false, emptyMap()));
        index2.put("distance", new IndexFieldCapabilities("distance", "int", false, false, true, singletonMap("unit", "km")));
        index2.put("duration", new IndexFieldCapabilities("duration", "int", false, true, true, singletonMap("unit", "hour")));

        Map<String, IndexFieldCapabilities> index3 = new HashMap<>();
        index3.put("seq_no", new IndexFieldCapabilities("seq_no", "long", true, true, false, emptyMap()));
        index3.put("duration", new IndexFieldCapabilities("duration", "short", false, true, true, singletonMap("unit", "day")));
        final CombinedResponse.Builder builder = CombinedResponse.newBuilder(MergeResultsMode.INTERNAL_PARTIAL_MERGE,
            randomBoolean(), field -> false, executor, listener);

        final List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>();
        indexResponses.add(new FieldCapabilitiesIndexResponse("index-1", index1, true));
        indexResponses.add(new FieldCapabilitiesIndexResponse("index-2", index2, true));
        indexResponses.add(new FieldCapabilitiesIndexResponse("index-3", index3, true));
        Collections.shuffle(indexResponses, random());
        builder.addIndexResponses(indexResponses);
        int times = between(1, 5);
        for (int i = 0; i < times; i++) {
            PlainActionFuture<CombinedResponse> subListener = new PlainActionFuture<>();
            CombinedResponse.Builder subBuilder = CombinedResponse.newBuilder(MergeResultsMode.INTERNAL_PARTIAL_MERGE,
                randomBoolean(), field -> false, executor, subListener);
            for (FieldCapabilitiesIndexResponse indexResponse : indexResponses) {
                if (randomBoolean()) {
                    builder.addIndexResponses(Collections.singletonList(indexResponse));
                } else {
                    subBuilder.addIndexResponses(Collections.singletonList(indexResponse));
                }
                if (randomBoolean()) {
                    subBuilder.complete();
                    final CombinedResponse combinedResponse = subListener.actionGet();
                    builder.addMergedResponse(combinedResponse.getIndices(), combinedResponse.getResponseBuilder());
                    subListener = new PlainActionFuture<>();
                    subBuilder = CombinedResponse.newBuilder(MergeResultsMode.INTERNAL_PARTIAL_MERGE,
                        randomBoolean(), field -> false, executor, subListener);
                }
            }
            subBuilder.complete();
            final CombinedResponse combinedResponse = subListener.actionGet();
            builder.addMergedResponse(combinedResponse.getIndices(), combinedResponse.getResponseBuilder());
        }
        builder.complete();
        final CombinedResponse combinedResponse = listener.actionGet();
        assertThat(combinedResponse.getIndexResponses(), empty());
        assertThat(combinedResponse.getResponseMap(), anEmptyMap());
        assertThat(combinedResponse.getIndices(), containsInAnyOrder("index-1", "index-2", "index-3"));

        final Map<String, Map<String, FieldCapabilities.Builder>> builderMap = combinedResponse.getResponseBuilder();
        assertThat(builderMap.keySet(), containsInAnyOrder("seq_no", "distance", "duration"));
        {
            final Map<String, FieldCapabilities.Builder> seqNo = builderMap.get("seq_no");
            assertThat(seqNo.keySet(), containsInAnyOrder("long"));
            final FieldCapabilities.Builder longField = seqNo.get("long");
            assertThat(longField.getIndices(), containsInAnyOrder("index-1", "index-2", "index-3"));
            assertTrue(longField.isMetadataField());
            assertTrue(longField.isSearchable());
            assertFalse(longField.isAggregatable());
            assertThat(longField.meta, anEmptyMap());
            assertThat(longField.indexCaps, containsInAnyOrder(
                newCaps("index-1", true, false), newCaps("index-2", true, false), newCaps("index-3", true, false)));
        }
        {
            final Map<String, FieldCapabilities.Builder> distance = builderMap.get("distance");
            assertThat(distance.keySet(), containsInAnyOrder("int"));
            final FieldCapabilities.Builder intField = distance.get("int");
            assertThat(intField.getIndices(), containsInAnyOrder("index-1", "index-2"));
            assertFalse(intField.isMetadataField());
            assertFalse(intField.isSearchable());
            assertTrue(intField.isAggregatable());
            assertThat(intField.meta, equalTo(singletonMap("unit", Sets.newHashSet("meter", "km"))));
            assertThat(intField.indexCaps, containsInAnyOrder(newCaps("index-1", true, true), newCaps("index-2", false, true)));
        }
        {
            final Map<String, FieldCapabilities.Builder> time = builderMap.get("duration");
            assertThat(time.keySet(), containsInAnyOrder("int", "short"));
            final FieldCapabilities.Builder intField = time.get("int");
            assertThat(intField.getIndices(), containsInAnyOrder("index-2"));
            assertFalse(intField.isMetadataField());
            assertTrue(intField.isSearchable());
            assertTrue(intField.isAggregatable());
            assertThat(intField.meta, equalTo(singletonMap("unit", Sets.newHashSet("hour"))));
            assertThat(intField.indexCaps, containsInAnyOrder(newCaps("index-2", true, true)));

            final FieldCapabilities.Builder shortField = time.get("short");
            assertThat(shortField.getIndices(), containsInAnyOrder("index-3"));
            assertFalse(shortField.isMetadataField());
            assertTrue(shortField.isSearchable());
            assertTrue(shortField.isAggregatable());
            assertThat(shortField.meta, equalTo(singletonMap("unit", Sets.newHashSet("day"))));
            assertThat(shortField.indexCaps, containsInAnyOrder(newCaps("index-3", true, true)));
        }
    }

    public void testRandomPartialMerge() {
        int numIndices = randomIntBetween(1, 100);
        List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indexResponses.add(FieldCapTestHelper.randomIndexResponse("index-" + i, true));
        }
        Collections.shuffle(indexResponses, random());
        final CombinedResponse r1;
        // Single merge
        {
            PlainActionFuture<CombinedResponse> listener = new PlainActionFuture<>();
            CombinedResponse.Builder builder = CombinedResponse.newBuilder(MergeResultsMode.INTERNAL_PARTIAL_MERGE, randomBoolean(),
                field -> false, randomFrom(threadPool.executor(ThreadPool.Names.SEARCH), EsExecutors.DIRECT_EXECUTOR_SERVICE), listener);
            builder.addIndexResponses(indexResponses);
            builder.complete();
            r1 = listener.actionGet();
        }
        // Merge of single merge
        final CombinedResponse r2;
        {
            Collections.shuffle(indexResponses, random());
            ExecutorService executor = randomFrom(threadPool.executor(ThreadPool.Names.SEARCH), EsExecutors.DIRECT_EXECUTOR_SERVICE);
            PlainActionFuture<CombinedResponse> listener = new PlainActionFuture<>();
            CombinedResponse.Builder builder = CombinedResponse.newBuilder(MergeResultsMode.INTERNAL_PARTIAL_MERGE,
                randomBoolean(), field -> false, executor, listener);
            for (FieldCapabilitiesIndexResponse resp : indexResponses) {
                if (randomBoolean()) {
                    builder.addIndexResponses(Collections.singletonList(resp));
                } else {
                    PlainActionFuture<CombinedResponse> subListener = new PlainActionFuture<>();
                    CombinedResponse.Builder subBuilder = CombinedResponse.newBuilder(MergeResultsMode.INTERNAL_PARTIAL_MERGE,
                        randomBoolean(), field -> false, executor, subListener);
                    subBuilder.addIndexResponses(Collections.singletonList(resp));
                    subBuilder.complete();
                    CombinedResponse combinedResponse = subListener.actionGet();
                    builder.addMergedResponse(combinedResponse.getIndices(), combinedResponse.getResponseBuilder());
                }
            }
            builder.complete();
            r2 = listener.actionGet();
        }
        assertEquivalent(r1, r2);
    }

    public void testFullMerge() {
        Map<String, IndexFieldCapabilities> index1 = new HashMap<>();
        index1.put("seq_no", new IndexFieldCapabilities("seq_no", "long", true, true, false, emptyMap()));
        index1.put("distance", new IndexFieldCapabilities("distance", "int", false, true, true, singletonMap("unit", "meter")));

        Map<String, IndexFieldCapabilities> index2 = new HashMap<>();
        index2.put("seq_no", new IndexFieldCapabilities("seq_no", "long", true, true, false, emptyMap()));
        index2.put("distance", new IndexFieldCapabilities("distance", "int", false, false, true, singletonMap("unit", "km")));
        index2.put("duration", new IndexFieldCapabilities("duration", "int", false, true, true, singletonMap("unit", "hour")));

        Map<String, IndexFieldCapabilities> index3 = new HashMap<>();
        index3.put("seq_no", new IndexFieldCapabilities("seq_no", "long", true, true, false, emptyMap()));
        index3.put("timestamp", new IndexFieldCapabilities("timestamp", "long", true, true, false, emptyMap()));
        index3.put("duration", new IndexFieldCapabilities("duration", "short", false, true, false, singletonMap("unit", "day")));

        final List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>();
        indexResponses.add(new FieldCapabilitiesIndexResponse("index-1", index1, true));
        indexResponses.add(new FieldCapabilitiesIndexResponse("index-2", index2, true));
        indexResponses.add(new FieldCapabilitiesIndexResponse("index-3", index3, true));
        Collections.shuffle(indexResponses, random());

        PlainActionFuture<CombinedResponse> fullMergeListener = new PlainActionFuture<>();
        CombinedResponse.Builder fullMergeBuilder = CombinedResponse.newBuilder(MergeResultsMode.FULL_MERGE, false, field -> false,
            randomFrom(threadPool.executor(ThreadPool.Names.SEARCH), EsExecutors.DIRECT_EXECUTOR_SERVICE), fullMergeListener);

        int times = between(1, 5);
        for (int i = 0; i < times; i++) {
            PlainActionFuture<CombinedResponse> subListener = new PlainActionFuture<>();
            CombinedResponse.Builder subBuilder = CombinedResponse.newBuilder(MergeResultsMode.INTERNAL_PARTIAL_MERGE, randomBoolean(),
                field -> false, randomFrom(threadPool.executor(ThreadPool.Names.SEARCH), EsExecutors.DIRECT_EXECUTOR_SERVICE), subListener);
            for (FieldCapabilitiesIndexResponse response : indexResponses) {
                if (randomBoolean()) {
                    fullMergeBuilder.addIndexResponses(Collections.singletonList(response));
                } else {
                    subBuilder.addIndexResponses(Collections.singletonList(response));
                }
                if (randomBoolean()) {
                    subBuilder.complete();
                    CombinedResponse subResponse = subListener.actionGet();
                    fullMergeBuilder.addMergedResponse(subResponse.getIndices(), subResponse.getResponseBuilder());
                    subListener = new PlainActionFuture<>();
                    subBuilder = CombinedResponse.newBuilder(MergeResultsMode.INTERNAL_PARTIAL_MERGE, randomBoolean(), field -> false,
                        randomFrom(threadPool.executor(ThreadPool.Names.SEARCH), EsExecutors.DIRECT_EXECUTOR_SERVICE), subListener);
                }
            }
            subBuilder.complete();
            final CombinedResponse subResponse = subListener.actionGet();
            fullMergeBuilder.addMergedResponse(subResponse.getIndices(), subResponse.getResponseBuilder());
        }
        fullMergeBuilder.complete();
        final CombinedResponse combinedResponse = fullMergeListener.actionGet();
        final Map<String, Map<String, FieldCapabilities>> responseMap = combinedResponse.getResponseMap();
        assertThat(responseMap.keySet(), containsInAnyOrder("seq_no", "distance", "duration", "timestamp"));
        {
            final Map<String, FieldCapabilities> seqNo = responseMap.get("seq_no");
            assertThat(seqNo.keySet(), containsInAnyOrder("long"));
            final FieldCapabilities longField = seqNo.get("long");
            assertNull(longField.indices());
            assertTrue(longField.isMetadataField());
            assertTrue(longField.isSearchable());
            assertFalse(longField.isAggregatable());
            assertThat(longField.meta(), anEmptyMap());
            assertNull(longField.nonSearchableIndices());
            assertNull(longField.nonAggregatableIndices());
        }
        {
            final Map<String, FieldCapabilities> distance = responseMap.get("distance");
            assertThat(distance.keySet(), containsInAnyOrder("int"));
            final FieldCapabilities intField = distance.get("int");
            assertNull(intField.indices());
            assertFalse(intField.isMetadataField());
            assertFalse(intField.isSearchable());
            assertTrue(intField.isAggregatable());
            assertThat(intField.meta(), equalTo(Collections.singletonMap("unit", Sets.newHashSet("km", "meter"))));
            assertThat(intField.nonSearchableIndices(), arrayContaining("index-2"));
            assertNull(intField.nonAggregatableIndices());
        }
        {
            final Map<String, FieldCapabilities> distance = responseMap.get("duration");
            assertThat(distance.keySet(), containsInAnyOrder("int", "short"));
            final FieldCapabilities intField = distance.get("int");
            assertThat(intField.indices(), arrayContaining("index-2"));
            assertFalse(intField.isMetadataField());
            assertTrue(intField.isSearchable());
            assertTrue(intField.isAggregatable());
            assertThat(intField.meta(), equalTo(Collections.singletonMap("unit", Sets.newHashSet("hour"))));
            assertNull(intField.nonSearchableIndices());
            assertNull(intField.nonAggregatableIndices());

            final FieldCapabilities shortField = distance.get("short");
            assertThat(shortField.indices(), arrayContaining("index-3"));
            assertFalse(shortField.isMetadataField());
            assertTrue(shortField.isSearchable());
            assertFalse(shortField.isAggregatable());
            assertThat(shortField.meta(), equalTo(Collections.singletonMap("unit", Sets.newHashSet("day"))));
            assertNull(shortField.nonSearchableIndices());
            assertNull(shortField.nonAggregatableIndices());
        }
        {
            final Map<String, FieldCapabilities> timestamp = responseMap.get("timestamp");
            assertThat(timestamp.keySet(), containsInAnyOrder("long"));
            final FieldCapabilities longField = timestamp.get("long");
            assertNull(longField.indices());
            assertTrue(longField.isMetadataField());
            assertTrue(longField.isSearchable());
            assertFalse(longField.isAggregatable());
            assertThat(longField.meta(), anEmptyMap());
            assertNull(longField.nonSearchableIndices());
            assertNull(longField.nonAggregatableIndices());
        }
    }

    public void testSerialize() throws Exception {

    }

    static FieldCapabilities.IndexCaps newCaps(String index, boolean search, boolean agg) {
        return new FieldCapabilities.IndexCaps(index, search, agg);
    }

    static void assertEquivalent(CombinedResponse r1, CombinedResponse r2) {
        assertThat(r1.getMergeMode(), equalTo(r2.getMergeMode()));
        assertThat(Sets.newHashSet(r1.getIndices()), equalTo(Sets.newHashSet(r2.getIndices())));
        assertThat(r1.getResponseMap(), equalTo(r2.getResponseMap()));
        assertThat(r1.getResponseBuilder(), equalTo(r2.getResponseBuilder()));
        assertThat(Sets.newHashSet(r1.getIndexResponses()), equalTo(Sets.newHashSet(r2.getIndexResponses())));
        assertThat(Sets.newHashSet(r1.getIndexFailures()), equalTo(Sets.newHashSet(r2.getIndexFailures())));
    }
}
