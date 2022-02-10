/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sampling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.assembler.AggregatedQueryRequest;
import org.elasticsearch.xpack.eql.execution.assembler.Executable;
import org.elasticsearch.xpack.eql.execution.assembler.SamplingCriterion;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.execution.search.RuntimeUtils;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;
import org.elasticsearch.xpack.eql.session.EmptyPayload;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.eql.session.Payload.Type;
import org.elasticsearch.xpack.ql.util.ActionListeners;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.elasticsearch.action.ActionListener.runAfter;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.Strings.EMPTY_ARRAY;
import static org.elasticsearch.xpack.eql.execution.assembler.AggregatedQueryRequest.COMPOSITE_AGG_NAME;
import static org.elasticsearch.xpack.eql.execution.search.RuntimeUtils.prepareRequest;

public class SamplingIterator implements Executable {

    public static final int MAX_PAGE_SIZE = 1; // for testing purposes it's set to such a low value
    private final Logger log = LogManager.getLogger(SamplingIterator.class);

    private final QueryClient client;
    private final List<SamplingCriterion<AggregatedQueryRequest>> criteria;
    private final Stack<Page> stack = new Stack<>();
    private final int maxStages;
    private final List<Sampling> samplings;

    private long startTime;

    public SamplingIterator(QueryClient client, List<SamplingCriterion<AggregatedQueryRequest>> criteria) {
        this.client = client;
        this.criteria = criteria;
        this.maxStages = criteria.size();
        this.samplings = new ArrayList<>();
    }

    @Override
    public void execute(ActionListener<Payload> listener) {
        startTime = System.currentTimeMillis();
        // clear the memory at the end of the algorithm
        advance(runAfter(listener, () -> {
            stack.clear();
            samplings.clear();
            client.close(listener.delegateFailure((l, r) -> {}));
        }));
    }

    /*
     * Starting point of the iterator, which also goes through all the criterions and gathers initial results pages.
     */
    private void advance(ActionListener<Payload> listener) {
        int currentStage = stack.size();
        if (currentStage < maxStages) {
            // excessive logging for testing purposes
            log.trace("Advancing from stage [{}]", currentStage);
            SamplingCriterion<AggregatedQueryRequest> criterion = criteria.get(currentStage);
            AggregatedQueryRequest request = criterion.firstQuery();

            // incorporate the previous stage composite keys in the current stage query
            if (currentStage > 0) {
                request = criterion.midQuery();
                Page previousResults = stack.peek();
                List<Map<String, Object>> values = new ArrayList<>(previousResults.size);
                for (InternalComposite.InternalBucket bucket : previousResults.hits) {
                    values.add(bucket.getKey());
                }
                request.multipleKeysPairs(values, previousResults.keys);
            }

            final AggregatedQueryRequest rr = request;
            log.trace("Querying stage [{}] {}", currentStage, request);
            client.query(request, wrap(r -> {
                Aggregation a = r.getAggregations().get(COMPOSITE_AGG_NAME);
                if (a instanceof InternalComposite == false) {
                    throw new EqlIllegalArgumentException("Unexpected aggregation result type returned [{}]", a.getClass());
                }

                InternalComposite composite = (InternalComposite) a;
                log.trace("Advancing.... found [{}] hits", composite.getBuckets().size());
                Page nextPage = new Page(composite, rr);
                if (nextPage != null && nextPage.size() > 0) {
                    stack.push(nextPage);
                    advance(listener);
                } else {
                    if (stack.size() > 0) {
                        nextPage(listener, stack.pop());
                    } else {
                        payload(listener);
                    }
                }
            }, listener::onFailure));
        } else if (currentStage > maxStages) {
            throw new EqlIllegalArgumentException("Unexpected stage [{}], max stages in this sampling [{}]", currentStage, maxStages);
        } else {
            finalStage(listener);
        }
    }

    /*
     * Creates a _msearch request containing maxStages (number of filters in the query) * number_of_join_keys queries.
     * For a query with three filters
     *          sampling by host [any where uptime > 0] by os [any where port > 5] by op_sys [any where bool == true] by os
     * and one pair of join keys values (host = H1, os = win10) the msearch will look like
     * ...{"bool":{"must":[{"term":{"host":"H1"}},{"term":{"os":"win10"}},{"range":{"uptime":{"gt":0}}}]}},"terminate_after":3,"size":3}
     * ...{"bool":{"must":[{"term":{"host":"H1"}},{"term":{"op_sys":"win10"}},{"range":{"port":{"gt":5}}}]}},"terminate_after": 3,"size":3}
     * ...{"bool":{"must":[{"term":{"host":"H1"}},{"term":{"os":"win10"}},{"term":{"bool":true}}]}},"terminate_after": 3,"size":3}
     */
    private void finalStage(ActionListener<Payload> listener) {
        log.trace("Final stage...");
        // query everything and build sequences
        // then remove the previous step from the stack, since we are done with it
        Page page = stack.pop();
        // for each criteria and for each matching pair of join keys, create one query
        List<SearchRequest> searches = new ArrayList<>(maxStages * page.hits.size());

        // should this one be a Set instead? Meaning, unique SequenceKeys? The algorithm should already be generating unique keys...
        List<SequenceKey> samplingKeys = new ArrayList<>();

        for (InternalComposite.InternalBucket hit : page.hits) {
            Map<String, Object> hitCompositeKeyValue = hit.getKey(); // the composite key that should be used with all other criteria
            final List<Object> compositeKeyValues = new ArrayList<>(hitCompositeKeyValue.size());
            // put the values in a list, each value's location within the list corresponds to the join keys' same location
            for (String keyName : page.keys) {
                compositeKeyValues.add(hitCompositeKeyValue.get(keyName));
            }

            // take each filter query and add to it a filter by join keys with the corresponding values
            for (SamplingCriterion<AggregatedQueryRequest> criterion : criteria) {
                AggregatedQueryRequest r = criterion.finalQuery();
                r.singleKeysPair(compositeKeyValues, maxStages);
                searches.add(prepareRequest(r.searchSource(), false, EMPTY_ARRAY));
            }
            samplingKeys.add(new SequenceKey(compositeKeyValues.toArray()));
        }

        int initialSize = samplings.size();
        client.multiQuery(searches, ActionListener.wrap(r -> {
            List<List<SearchHit>> finalSamplings = new ArrayList<>();
            List<List<SearchHit>> sampling = new ArrayList<>(maxStages);
            MultiSearchResponse.Item[] response = r.getResponses();

            int i = 0; // response items iterator
            int j = 1; // iterator for groups of maxStages documents

            while (i < response.length) {
                MultiSearchResponse.Item item = response[i];
                List<SearchHit> hits = RuntimeUtils.searchHits(item.getResponse());
                if (hits.size() > 0) {
                    sampling.add(hits);
                }
                if (j == maxStages) {
                    List<SearchHit> match = matchSampling(sampling, maxStages);
                    if (match != null) {
                        finalSamplings.add(match);
                        samplings.add(new Sampling(samplingKeys.get(i / maxStages), match));
                    }
                    j = 1;
                    sampling = new ArrayList<>(maxStages);
                } else {
                    j++;
                }
                i++;
            }

            log.trace("Final stage... found [{}] new Samplings", samplings.size() - initialSize);
            // if this final page is max_page_size in size it means: either it's the last page and it happens to have max_page_size elements
            // or it's just not the last page and we should advance
            if (page.size() == MAX_PAGE_SIZE) {
                log.trace("Final stage... getting next page of the current stage");
                // look at the current stage's page "after_key" and ask for one more page of results
                nextPage(listener, page);
            } else {
                log.trace("Final stage... getting next page of the previous stage");
                // we are done here with this page
                nextPage(listener, stack.pop());
            }
        }, listener::onFailure));
    }

    /*
     * Finds the next set of results using the after_key of the previous set of buckets.
     * It can go back on previous page(s) until either there are no more results, or it finds a page with an after_key to use.
     */
    private void nextPage(ActionListener<Payload> listener, Page page) {
        page.request().nextAfter(page.afterKey());
        log.trace("Getting next page for page [{}] with afterkey [{}]", page, page.afterKey());

        client.query(page.request(), wrap(r -> {
            InternalComposite composite;
            Aggregation a = r.getAggregations().get(COMPOSITE_AGG_NAME);
            if (a instanceof InternalComposite agg) {
                composite = agg;
            } else {
                throw new EqlIllegalArgumentException("Unexpected aggregation result type returned [{}]", a.getClass());
            }
            log.trace("Next page.... found [{}] hits", composite.getBuckets().size());
            Page nextPage = new Page(composite, page.request());

            if (nextPage != null && nextPage.size() > 0) {
                stack.push(nextPage);
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

    /*
     * Final step for gathering all the documents (fetch phase) of the found samplings.
     */
    private void payload(ActionListener<Payload> listener) {
        log.trace("Sending payload for [{}] samplings", samplings.size());

        if (samplings.isEmpty()) {
            listener.onResponse(new EmptyPayload(Type.SEQUENCE, timeTook()));
            return;
        }

        // get results through search (to keep using PIT)
        client.fetchHits(hits(samplings), ActionListeners.map(listener, listOfHits -> {
            SamplingPayload payload = new SamplingPayload(samplings, listOfHits, false, timeTook());
            return payload;
        }));
    }

    Iterable<List<HitReference>> hits(List<Sampling> samplings) {
        return () -> {
            Iterator<Sampling> delegate = samplings.iterator();

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
     * For example, for a sampling with three filters, there will be at most 3 documents matching each filter. But, because one document
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
    static List<SearchHit> matchSampling(List<List<SearchHit>> hits, int hitsCount) {
        if (hits.size() < hitsCount) {
            return null;
        }
        List<SearchHit> result = new ArrayList<>(hitsCount);
        if (match(0, hits, result, hitsCount)) {
            return result;
        }
        return null;
    }

    private static boolean match(int currentStage, List<List<SearchHit>> hits, List<SearchHit> result, int hitsCount) {
        for (SearchHit o : hits.get(currentStage)) {
            if (result.contains(o) == false) {
                result.add(o);
                if (currentStage == hitsCount - 1) {
                    return true;
                } else {
                    if (match(currentStage + 1, hits, result, hitsCount)) {
                        return true;
                    }
                }
            }
        }
        if (result.size() > 0) {
            result.remove(result.get(result.size() - 1));
        }
        return false;
    }

    private TimeValue timeTook() {
        return new TimeValue(System.currentTimeMillis() - startTime);
    }

    private record Page(
        List<InternalComposite.InternalBucket> hits,
        int size,
        Map<String, Object> afterKey,
        List<String> keys,
        AggregatedQueryRequest request
    ) {
        Page(InternalComposite compositeAgg, AggregatedQueryRequest request) {
            this(compositeAgg.getBuckets(), compositeAgg.getBuckets().size(), compositeAgg.afterKey(), request.keys(), request);
        }
    }
}
