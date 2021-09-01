/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.execution.assembler.BoxedQueryRequest;
import org.elasticsearch.xpack.eql.execution.assembler.Criterion;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.eql.execution.search.extractor.ImplicitTiebreakerHitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class CircuitBreakerTests extends ESTestCase {

    private static final TestCircuitBreaker CIRCUIT_BREAKER = new TestCircuitBreaker();

    private final List<HitExtractor> keyExtractors = emptyList();
    private final HitExtractor tsExtractor = TimestampExtractor.INSTANCE;
    private final HitExtractor implicitTbExtractor = ImplicitTiebreakerHitExtractor.INSTANCE;
    private final int stages = randomIntBetween(3, 10);

    static class TestQueryClient implements QueryClient {

        @Override
        public void query(QueryRequest r, ActionListener<SearchResponse> l) {
            int ordinal = r.searchSource().terminateAfter();
            SearchHit searchHit = new SearchHit(ordinal, String.valueOf(ordinal), null, null);
            searchHit.sortValues(new SearchSortValues(
                    new Long[] { (long) ordinal, 1L },
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

    public void testCircuitBreakerTumblingWindow() {
        QueryClient client = new TestQueryClient();
        List<Criterion<BoxedQueryRequest>> criteria = new ArrayList<>(stages);

        for (int i = 0; i < stages; i++) {
            final int j = i;
            criteria.add(new Criterion<>(i,
                    new BoxedQueryRequest(() -> SearchSourceBuilder.searchSource()
                            .size(10)
                            .query(matchAllQuery())
                            .terminateAfter(j), "@timestamp", emptyList()),
                    keyExtractors,
                    tsExtractor,
                    null,
                    implicitTbExtractor,
                    false));
        }

        SequenceMatcher matcher = new SequenceMatcher(stages, false, TimeValue.MINUS_ONE, null, CIRCUIT_BREAKER);
        TumblingWindow window = new TumblingWindow(client, criteria, null, matcher);
        window.execute(wrap(p -> {}, ex -> {
            throw ExceptionsHelper.convertToRuntime(ex);
        }));

        CIRCUIT_BREAKER.startBreaking();
        RuntimeException e = expectThrows(
                RuntimeException.class,
                () -> window.execute(wrap(p -> {}, ex -> { throw new RuntimeException(ex); }))
        );
        assertEquals(CircuitBreakingException.class, e.getCause().getClass());

        CIRCUIT_BREAKER.stopBreaking();
        window.execute(wrap(p -> {}, ex -> {
            throw ExceptionsHelper.convertToRuntime(ex);
        }));
    }

    public void testCircuitBreakerSequnceMatcher() {
        List<Tuple<KeyAndOrdinal, HitReference>> hits = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            hits.add(new Tuple<>(new KeyAndOrdinal(new SequenceKey(i), new Ordinal(i, o -> 1, 0)), new HitReference("index", i + "")));
        }

        // Break on first iteration
        SequenceMatcher matcher1 = new SequenceMatcher(stages, false, TimeValue.MINUS_ONE, null, new EqlTestCircuitBreaker(10000));
        CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> matcher1.match(0, hits));
        assertEquals("sequence_inflight", e.getMessage());

        // Break on second iteration
        SequenceMatcher matcher2 = new SequenceMatcher(stages, false, TimeValue.MINUS_ONE, null, new EqlTestCircuitBreaker(15000));
        matcher2.match(0, hits);
        e = expectThrows(CircuitBreakingException.class, () -> matcher2.match(0, hits));
        assertEquals("sequence_inflight", e.getMessage());

        // Break on 3rd iteration with clear() called in between
        SequenceMatcher matcher3 = new SequenceMatcher(stages, false, TimeValue.MINUS_ONE, null, new EqlTestCircuitBreaker(15000));
        matcher3.match(0, hits);
        matcher3.clear();
        matcher3.match(0, hits);
        e = expectThrows(CircuitBreakingException.class, () -> matcher3.match(0, hits));
        assertEquals("sequence_inflight", e.getMessage());
    }

    private static class EqlTestCircuitBreaker extends NoopCircuitBreaker {

        private final long limitInBytes;
        private long ramBytesUsed = 0;

        private EqlTestCircuitBreaker(long limitInBytes) {
            super("eql_test");
            this.limitInBytes = limitInBytes;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            ramBytesUsed += bytes;
            if (ramBytesUsed > limitInBytes) {
                throw new CircuitBreakingException(label, getDurability());
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            ramBytesUsed += bytes;
        }
    }

    private static class TimestampExtractor implements HitExtractor {

        static final TimestampExtractor INSTANCE = new TimestampExtractor();

        @Override
        public String getWriteableName() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public String hitName() {
            return null;
        }

        @Override
        public Long extract(SearchHit hit) {
            return (long) hit.docId();
        }
    }
}
