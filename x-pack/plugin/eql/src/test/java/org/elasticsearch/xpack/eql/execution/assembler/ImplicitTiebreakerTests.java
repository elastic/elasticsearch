/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.execution.assembler.SequenceSpecTests.TimestampExtractor;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.eql.execution.search.extractor.ImplicitTiebreakerHitExtractor;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceMatcher;
import org.elasticsearch.xpack.eql.execution.sequence.TumblingWindow;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class ImplicitTiebreakerTests extends ESTestCase {

    private static final NoopCircuitBreaker NOOP_CIRCUIT_BREAKER = new NoopCircuitBreaker("ImplicitTiebreakerTests");

    private final List<HitExtractor> keyExtractors = emptyList();
    private final HitExtractor tsExtractor = TimestampExtractor.INSTANCE;
    private final HitExtractor tbExtractor = null;
    private final HitExtractor implicitTbExtractor = ImplicitTiebreakerHitExtractor.INSTANCE;
    private final List<Long> implicitTiebreakerValues;
    private final int stages = randomIntBetween(3, 10);

    public ImplicitTiebreakerTests() {
        this.implicitTiebreakerValues = new ArrayList<>(stages);
        for (int i = 0; i < stages; i++) {
            implicitTiebreakerValues.add(randomLong());
        }
    }

    class TestQueryClient implements QueryClient {

        @Override
        public void query(QueryRequest r, ActionListener<SearchResponse> l) {
            int ordinal = r.searchSource().terminateAfter();
            if (ordinal > 0) {
                int previous = ordinal - 1;
                // except the first request, the rest should have the previous response's search_after _shard_doc value
                assertArrayEquals("Elements at stage " + ordinal + " do not match",
                    r.searchSource().searchAfter(), new Object[] { (long) previous, implicitTiebreakerValues.get(previous) });
            }

            long sortValue = implicitTiebreakerValues.get(ordinal);
            SearchHit searchHit = new SearchHit(ordinal, String.valueOf(ordinal), null, null);
            searchHit.sortValues(new SearchSortValues(
                new Long[] { (long) ordinal, sortValue },
                new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW }));
            SearchHits searchHits = new SearchHits(new SearchHit[] { searchHit }, new TotalHits(1, Relation.EQUAL_TO), 0.0f);
            SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
            SearchResponse s = new SearchResponse(internal, null, 0, 1, 0, 0, null, Clusters.EMPTY);
            l.onResponse(s);
        }

        @Override
        public void fetchHits(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener) {
            List<List<SearchHit>> searchHits = new ArrayList<>();
            for (List<HitReference> ref : refs) {
                List<SearchHit> hits = new ArrayList<>(ref.size());
                for (HitReference hitRef : ref) {
                    hits.add(new SearchHit(-1, hitRef.id(), null, null));
                }
                searchHits.add(hits);
            }
            listener.onResponse(searchHits);
        }
    }

    public void testImplicitTiebreakerBeingSet() {
        QueryClient client = new TestQueryClient();
        List<Criterion<BoxedQueryRequest>> criteria = new ArrayList<>(stages);
        boolean descending = randomBoolean();
        boolean criteriaDescending = descending;

        for (int i = 0; i < stages; i++) {
            final int j = i;
            criteria.add(new Criterion<BoxedQueryRequest>(i,
                new BoxedQueryRequest(() -> SearchSourceBuilder.searchSource()
                    .size(10)
                    .query(matchAllQuery())
                    .terminateAfter(j), "@timestamp", emptyList()),
                keyExtractors,
                tsExtractor,
                tbExtractor,
                implicitTbExtractor,
                criteriaDescending));
            // for DESC (TAIL) sequences only the first criterion is descending the rest are ASC, so flip it after the first query
            if (criteriaDescending && i == 0) {
                criteriaDescending = false;
            }
        }

        SequenceMatcher matcher = new SequenceMatcher(stages, descending, TimeValue.MINUS_ONE, null, NOOP_CIRCUIT_BREAKER);
        TumblingWindow window = new TumblingWindow(client, criteria, null, matcher);
        window.execute(wrap(p -> {}, ex -> {
            throw ExceptionsHelper.convertToRuntime(ex);
        }));
    }
}
