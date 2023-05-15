/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.assembler.Executable;
import org.elasticsearch.xpack.eql.execution.assembler.SampleCriterion;
import org.elasticsearch.xpack.eql.execution.assembler.SampleQueryRequest;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.execution.search.RuntimeUtils;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;
import org.elasticsearch.xpack.eql.session.EmptyPayload;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.eql.session.Payload.Type;
import org.elasticsearch.xpack.ql.util.ActionListeners;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.elasticsearch.action.ActionListener.runAfter;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.Strings.EMPTY_ARRAY;
import static org.elasticsearch.xpack.eql.execution.assembler.SampleQueryRequest.COMPOSITE_AGG_NAME;
import static org.elasticsearch.xpack.eql.execution.search.RuntimeUtils.prepareRequest;

public class SampleIterator implements Executable {

    private final Logger log = LogManager.getLogger(SampleIterator.class);

    private final QueryClient client;
    private final List<SampleCriterion> criteria;
    final Stack<Page> stack = new Stack<>();
    private final int maxCriteria;
    final List<Sample> samples;
    private final int fetchSize;
    private final Limit limit;
    private final int maxSamplesPerKey;
    private long startTime;

    // ---------- CIRCUIT BREAKER -----------

    /**
     * Memory consumption will be calculated every CB_STACK_SIZE_PRECISION hits added to the stack
     * ie. the sum of sizes of pages added to the stack
     * (not considering stack.pop(), so the number of hits added to the stack is different
     * from the number of hits currently present in the stack)
     */
    protected static final int CB_STACK_SIZE_PRECISION = 1000;
    private static final String CB_COMPLETED_LABEL = "sample_completed";
    private static final String CB_INFLIGHT_LABEL = "sample_inflight";
    private final CircuitBreaker circuitBreaker;
    private long samplesRamBytesUsed = 0;
    private long stackRamBytesUsed = 0;
    private long totalRamBytesUsed = 0;
    /**
     * total number of hits (ie. sum of page sizes) added to the stack
     * (not considering stack.pop(), so different from current stack size)
     */
    private long totalPageSize = 0;
    /**
     * total number of hits (ie. sum of page sizes) added to the stack when last memory check was executed
     */
    private long previousTotalPageSize = 0;

    public SampleIterator(
        QueryClient client,
        List<SampleCriterion> criteria,
        int fetchSize,
        Limit limit,
        CircuitBreaker circuitBreaker,
        int maxSamplesPerKey
    ) {
        this.client = client;
        this.criteria = criteria;
        this.maxCriteria = criteria.size();
        this.fetchSize = fetchSize;
        this.samples = new ArrayList<>();
        this.limit = limit;
        this.circuitBreaker = circuitBreaker;
        this.maxSamplesPerKey = maxSamplesPerKey;
    }

    @Override
    public void execute(ActionListener<Payload> listener) {
        startTime = System.currentTimeMillis();
        // clear the memory at the end of the algorithm
        advance(runAfter(listener, () -> {
            stack.clear();
            samples.clear();
            clearCircuitBreaker();
            client.close(listener.delegateFailure((l, r) -> {}));
        }));
    }

    /*
     * Starting point of the iterator, which also goes through all the criteria and gathers initial results pages.
     */
    private void advance(ActionListener<Payload> listener) {
        int currentCriterion = stack.size();
        if (currentCriterion < maxCriteria) {
            // excessive logging for testing purposes
            log.trace("Advancing from step [{}]", currentCriterion);
            SampleCriterion criterion = criteria.get(currentCriterion);
            final SampleQueryRequest request;

            // incorporate the previous step composite keys in the current step query
            if (currentCriterion > 0) {
                request = criterion.midQuery();
                Page previousResults = stack.peek();
                SampleCriterion previousCriterion = criteria.get(currentCriterion - 1);
                request.multipleKeyPairs(previousCriterion.keys(previousResults.hits), previousResults.keys);
            } else {
                request = criterion.firstQuery();
            }

            // final SampleQueryRequest rr = request;
            log.trace("Querying step [{}] {}", currentCriterion, request);
            queryForCompositeAggPage(listener, request);
        } else if (currentCriterion > maxCriteria) {
            throw new EqlIllegalArgumentException("Unexpected step [{}], max steps in this sample [{}]", currentCriterion, maxCriteria);
        } else {
            finalStep(listener);
        }
    }

    private void queryForCompositeAggPage(ActionListener<Payload> listener, final SampleQueryRequest request) {
        client.query(request, wrap(r -> {
            Aggregation a = r.getAggregations().get(COMPOSITE_AGG_NAME);
            if (a instanceof InternalComposite == false) {
                throw new EqlIllegalArgumentException("Unexpected aggregation result type returned [{}]", a.getClass());
            }

            InternalComposite composite = (InternalComposite) a;
            log.trace("Found [{}] composite buckets", composite.getBuckets().size());
            Page nextPage = new Page(composite, request);
            if (nextPage.size > 0) {
                pushToStack(nextPage);
                advance(listener);
            } else {
                if (stack.size() > 0) {
                    nextPage(listener, stack.pop());
                } else {
                    payload(listener);
                }
            }
        }, listener::onFailure));
    }

    protected void pushToStack(Page nextPage) {
        stack.push(nextPage);
        totalPageSize += nextPage.size;
        if (totalPageSize - previousTotalPageSize >= CB_STACK_SIZE_PRECISION) {
            updateMemoryUsage();
            previousTotalPageSize = totalPageSize;
        }
    }

    /*
     * Creates a _msearch request containing maxCriteria (number of filters in the query) * number_of_join_keys queries.
     * For a query with three filters
     *          sample by host [any where uptime > 0] by os [any where port > 5] by op_sys [any where bool == true] by os
     * and one pair of join keys values (host = H1, os = win10) the msearch will look like
     * ...{"bool":{"must":[{"term":{"host":"H1"}},{"term":{"os":"win10"}},{"range":{"uptime":{"gt":0}}}]}},"terminate_after":3,"size":3}
     * ...{"bool":{"must":[{"term":{"host":"H1"}},{"term":{"op_sys":"win10"}},{"range":{"port":{"gt":5}}}]}},"terminate_after": 3,"size":3}
     * ...{"bool":{"must":[{"term":{"host":"H1"}},{"term":{"os":"win10"}},{"term":{"bool":true}}]}},"terminate_after": 3,"size":3}
     */
    private void finalStep(ActionListener<Payload> listener) {
        log.trace("Final step...");
        // query everything and build sequences
        // then remove the previous step from the stack, since we are done with it
        Page page = stack.pop();
        // for each criteria and for each matching pair of join keys, create one query
        List<SearchRequest> searches = new ArrayList<>(maxCriteria * page.hits.size());

        // should this one be a Set instead? Meaning, unique SequenceKeys? The algorithm should already be generating unique keys...
        List<SequenceKey> sampleKeys = new ArrayList<>();
        // get all composite key values as a list of list
        List<List<Object>> allCompositeKeyValues = criteria.get(maxCriteria - 1).keyValues(page.hits);
        for (List<Object> compositeKeyValues : allCompositeKeyValues) {
            // take each filter query and add to it a filter by join keys with the corresponding values
            for (SampleCriterion criterion : criteria) {
                SampleQueryRequest r = criterion.finalQuery();
                r.singleKeyPair(compositeKeyValues, maxCriteria, maxSamplesPerKey);
                searches.add(prepareRequest(r.searchSource(), false, EMPTY_ARRAY));
            }
            sampleKeys.add(new SequenceKey(compositeKeyValues.toArray()));
        }

        int initialSize = samples.size();
        client.multiQuery(searches, ActionListener.wrap(r -> {
            List<List<SearchHit>> sample = new ArrayList<>(maxCriteria);
            MultiSearchResponse.Item[] response = r.getResponses();
            int docGroupsCounter = 1;

            for (int responseIndex = 0; responseIndex < response.length; responseIndex++) {
                MultiSearchResponse.Item item = response[responseIndex];
                final var hits = RuntimeUtils.searchHits(item.getResponse());
                if (hits.size() > 0) {
                    sample.add(hits);
                }
                if (docGroupsCounter == maxCriteria) {
                    List<List<SearchHit>> matches = matchSamples(sample, maxCriteria, maxSamplesPerKey);
                    for (List<SearchHit> match : matches) {
                        if (samples.size() < limit.limit()) {
                            samples.add(new Sample(sampleKeys.get(responseIndex / maxCriteria), match));
                        }
                        if (samples.size() == limit.limit()) {
                            payload(listener);
                            return;
                        }
                    }
                    docGroupsCounter = 1;
                    sample = new ArrayList<>(maxCriteria);
                } else {
                    docGroupsCounter++;
                }
            }

            log.trace("Final step... found [{}] new Samples", samples.size() - initialSize);
            // if this final page is max_page_size in size it means: either it's the last page and it happens to have max_page_size elements
            // or it's just not the last page and we should advance
            var next = page.size == fetchSize ? page : stack.pop();
            log.trace("Final step... getting next page of the " + (next == page ? "current" : "previous") + " page");
            nextPage(listener, next);
        }, listener::onFailure));
    }

    private void updateMemoryUsage() {
        long newSamplesRamSize = RamUsageEstimator.sizeOfCollection(samples);
        addMemory(newSamplesRamSize - samplesRamBytesUsed, CB_COMPLETED_LABEL);
        samplesRamBytesUsed = newSamplesRamSize;

        long newStackRamSize = RamUsageEstimator.sizeOfCollection(stack);
        addMemory(newStackRamSize - stackRamBytesUsed, CB_INFLIGHT_LABEL);
        stackRamBytesUsed = newStackRamSize;
    }

    /*
     * Finds the next set of results using the after_key of the previous set of buckets.
     * It can go back on previous page(s) until either there are no more results, or it finds a page with an after_key to use.
     */
    private void nextPage(ActionListener<Payload> listener, Page page) {
        page.request.nextAfter(page.afterKey);
        log.trace("Getting next page for page [{}] with afterkey [{}]", page, page.afterKey);
        queryForCompositeAggPage(listener, page.request);
    }

    /*
     * Final step for gathering all the documents (fetch phase) of the found samples.
     */
    private void payload(ActionListener<Payload> listener) {
        log.trace("Sending payload for [{}] samples", samples.size());

        if (samples.isEmpty()) {
            listener.onResponse(new EmptyPayload(Type.SAMPLE, timeTook()));
            return;
        }

        // get results through search (to keep using PIT)
        client.fetchHits(
            hits(samples),
            ActionListeners.map(listener, listOfHits -> new SamplePayload(samples, listOfHits, false, timeTook()))
        );
    }

    Iterable<List<HitReference>> hits(List<Sample> samples) {
        return () -> {
            Iterator<Sample> delegate = samples.iterator();

            return new Iterator<>() {

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public List<HitReference> next() {
                    return delegate.next().hits();
                }
            };
        };
    }

    /*
     * Backtracking for choosing unique combination of documents/search hits from a list.
     * For example, for a sample with three filters, there will be at most 3 documents matching each filter. But, because one document
     * can potentially match multiple filters, the same document can be found in all three lists. The aim of the algorithm is to pick
     * three different documents from each group.
     *
     * For example, for the following list of lists
     * [doc1, doc2, doc3]
     * [doc4, doc5]
     * [doc4, doc5, doc3]
     * the algorithm picks [doc1, doc4, doc5].
     *
     * For
     * [doc1, doc1]
     * [doc1, doc1, doc1]
     * [doc1, doc1, doc3]
     * there is no solution.
     */
    static List<List<SearchHit>> matchSamples(List<List<SearchHit>> hits, int hitsCount, int maxSamplesPerKey) {
        if (hits.size() < hitsCount) {
            return null;
        }
        List<List<SearchHit>> result = new ArrayList<>(maxSamplesPerKey);
        match(0, hits, result, new ArrayList<>(hitsCount), hitsCount, maxSamplesPerKey);
        return result;
    }

    private static void match(
        int currentCriterion,
        List<List<SearchHit>> hits,
        List<List<SearchHit>> result,
        List<SearchHit> partial,
        int hitsCount,
        int maxSamplesPerKey
    ) {
        for (SearchHit o : hits.get(currentCriterion)) {
            if (partial.contains(o) == false) {
                partial.add(o);
                if (currentCriterion == hitsCount - 1) {
                    result.add(new ArrayList<>(partial));
                    if (maxSamplesPerKey == result.size()) {
                        return;
                    }
                } else {
                    match(currentCriterion + 1, hits, result, partial, hitsCount, maxSamplesPerKey);
                    if (maxSamplesPerKey == result.size()) {
                        return;
                    }
                }
                partial.remove(partial.size() - 1);
            }
        }
    }

    private void addMemory(long bytes, String label) {
        circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, label);
        totalRamBytesUsed += bytes;
    }

    private void clearCircuitBreaker() {
        circuitBreaker.addWithoutBreaking(-totalRamBytesUsed);
        stackRamBytesUsed = 0;
        samplesRamBytesUsed = 0;
        totalRamBytesUsed = 0;
        totalPageSize = 0;
        previousTotalPageSize = 0;
    }

    private TimeValue timeTook() {
        return new TimeValue(System.currentTimeMillis() - startTime);
    }

    protected static class Page implements Accountable {
        final List<InternalComposite.InternalBucket> hits;
        final int size;
        final Map<String, Object> afterKey;
        final List<String> keys;
        final SampleQueryRequest request;

        long ramBytesUsed = 0;

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Page.class);

        // for test purposes only
        protected Page(int size) {
            hits = null;
            this.size = size;
            afterKey = null;
            keys = null;
            request = null;
        }

        protected Page(InternalComposite compositeAgg, SampleQueryRequest request) {
            hits = compositeAgg.getBuckets();
            size = compositeAgg.getBuckets().size();
            afterKey = compositeAgg.afterKey();
            keys = request.keys();
            this.request = request;
        }

        @Override
        public long ramBytesUsed() {
            if (ramBytesUsed == 0) {
                ramBytesUsed = SHALLOW_SIZE;
                ramBytesUsed += RamUsageEstimator.sizeOfCollection(hits);
                ramBytesUsed += RamUsageEstimator.sizeOfCollection(keys);
                ramBytesUsed += RamUsageEstimator.sizeOfMap(afterKey);
            }
            return ramBytesUsed;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Accountable.super.getChildResources();
        }
    }
}
