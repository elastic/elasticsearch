/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;
import org.elasticsearch.xpack.eql.execution.assembler.SeriesUtils.SeriesSpec;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.eql.execution.search.Timestamp;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceMatcher;
import org.elasticsearch.xpack.eql.execution.sequence.TumblingWindow;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xpack.eql.EqlTestUtils.booleanArrayOf;

public class SequenceSpecTests extends ESTestCase {

    private static final NoopCircuitBreaker NOOP_CIRCUIT_BREAKER = new NoopCircuitBreaker("SequenceSpecTests");

    private static final String PARAM_FORMATTING = "%1$s";
    private static final String QUERIES_FILENAME = "sequences.series-spec";

    private static final String KEY_FIELD_NAME = "key";

    private final List<Map<Integer, Tuple<String, String>>> events;
    private final List<List<String>> matches;
    private final Map<Integer, String> allEvents;
    private final int lineNumber;
    private final boolean hasKeys;
    private final List<HitExtractor> keyExtractors;
    private final HitExtractor tsExtractor;
    private final HitExtractor tbExtractor;
    private final HitExtractor implicitTbExtractor;

    abstract static class EmptyHitExtractor implements HitExtractor {
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
    }

    static class TimestampExtractor extends EmptyHitExtractor {

        static final TimestampExtractor INSTANCE = new TimestampExtractor();

        @Override
        public Timestamp extract(SearchHit hit) {
            return Timestamp.of(String.valueOf(hit.docId()));
        }
    }

    static class KeyExtractor extends EmptyHitExtractor {
        @Override
        public String extract(SearchHit hit) {
            return hit.getFields().get(KEY_FIELD_NAME).getValues().get(0).toString();
        }
    }

    static class ImplicitTbExtractor extends EmptyHitExtractor {
        static final ImplicitTbExtractor INSTANCE = new ImplicitTbExtractor();

        @Override
        public Long extract(SearchHit hit) {
            return (long) hit.docId();
        }
    }

    class TestCriterion extends SequenceCriterion {
        private final int ordinal;
        private boolean unused = true;

        TestCriterion(final int ordinal) {
            super(
                ordinal,
                new BoxedQueryRequest(
                    () -> SearchSourceBuilder.searchSource()
                        // set a non-negative size
                        .size(10)
                        .query(matchAllQuery())
                        // pass the ordinal through terminate after
                        .terminateAfter(ordinal),
                    "timestamp",
                    emptyList(),
                    emptySet()
                ),
                keyExtractors,
                tsExtractor,
                tbExtractor,
                implicitTbExtractor,
                false,
                false
            );
            this.ordinal = ordinal;
        }

        int ordinal() {
            return ordinal;
        }

        @Override
        public int hashCode() {
            return ordinal;
        }

        public boolean use() {
            boolean u = unused;
            unused = false;
            return u;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            SequenceSpecTests.TestCriterion other = (SequenceSpecTests.TestCriterion) obj;
            return ordinal == other.ordinal;
        }

        @Override
        public String toString() {
            return format(null, "[{}] -> {}", ordinal, events.get(ordinal).values());
        }
    }

    static class EventsAsHits {
        private final List<SearchHit> hits;
        private final Map<Integer, Tuple<String, String>> events;

        EventsAsHits(Map<Integer, Tuple<String, String>> events) {
            this.events = events;
            this.hits = new ArrayList<>(events.size());

            for (Entry<Integer, Tuple<String, String>> entry : events.entrySet()) {
                Tuple<String, String> value = entry.getValue();
                Map<String, DocumentField> documentFields = new HashMap<>();
                documentFields.put(KEY_FIELD_NAME, new DocumentField(KEY_FIELD_NAME, Collections.singletonList(value.v1())));
                // save the timestamp both as docId (int) and as id (string)
                SearchHit searchHit = new SearchHit(entry.getKey(), entry.getKey().toString());
                searchHit.addDocumentFields(documentFields, Map.of());
                hits.add(searchHit);
            }
        }

        public List<SearchHit> hits() {
            return hits;
        }

        @Override
        public String toString() {
            return events.values().toString();
        }
    }

    class TestQueryClient implements QueryClient {

        @Override
        public void query(QueryRequest r, ActionListener<SearchResponse> l) {
            int ordinal = r.searchSource().terminateAfter();
            if (ordinal != Integer.MAX_VALUE) {
                r.searchSource().terminateAfter(Integer.MAX_VALUE);
            }
            Map<Integer, Tuple<String, String>> evs = ordinal != Integer.MAX_VALUE ? events.get(ordinal) : emptyMap();

            EventsAsHits eah = new EventsAsHits(evs);
            SearchHits searchHits = new SearchHits(
                eah.hits.toArray(new SearchHit[0]),
                new TotalHits(eah.hits.size(), Relation.EQUAL_TO),
                0.0f
            );
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
                    hits.add(new SearchHit(-1, hitRef.id()));
                }
                searchHits.add(hits);
            }
            listener.onResponse(searchHits);
        }
    }

    public SequenceSpecTests(String testName, int lineNumber, SeriesSpec spec) {
        this.lineNumber = lineNumber;
        this.events = spec.eventsPerCriterion;
        this.matches = spec.matches;
        this.allEvents = spec.allEvents;
        this.hasKeys = spec.hasKeys;
        this.keyExtractors = hasKeys ? singletonList(new KeyExtractor()) : emptyList();
        this.tsExtractor = TimestampExtractor.INSTANCE;
        this.tbExtractor = null;
        this.implicitTbExtractor = ImplicitTbExtractor.INSTANCE;
    }

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static Iterable<Object[]> parameters() throws Exception {
        return SeriesUtils.readSpec("/" + QUERIES_FILENAME);
    }

    public void test() throws Exception {
        int stages = events.size();
        List<SequenceCriterion> criteria = new ArrayList<>(stages);
        // pass the items for each query through the Criterion
        for (int i = 0; i < stages; i++) {
            // set the index as size in the search source
            criteria.add(new TestCriterion(i));
        }

        // convert the results through a test specific payload
        SequenceMatcher matcher = new SequenceMatcher(
            stages,
            false,
            TimeValue.MINUS_ONE,
            null,
            booleanArrayOf(stages, false),
            NOOP_CIRCUIT_BREAKER
        );

        QueryClient testClient = new TestQueryClient();
        TumblingWindow window = new TumblingWindow(testClient, criteria, null, matcher, Collections.emptyList());

        // finally make the assertion at the end of the listener
        window.execute(ActionTestUtils.assertNoFailureListener(this::checkResults));
    }

    private void checkResults(Payload payload) {
        List<Sequence> seq = Results.fromPayload(payload).sequences();
        String prefix = "Line " + lineNumber + ":";
        assertNotNull(prefix + "no matches found", seq);
        assertEquals(prefix + "different sequences matched ", matches.size(), seq.size());

        for (int i = 0; i < seq.size(); i++) {
            Sequence s = seq.get(i);
            List<String> match = matches.get(i);
            List<String> returned = new ArrayList<>();
            for (int j = 0; j < match.size(); j++) {
                int key = Integer.parseInt(s.events().get(j).id());
                returned.add(allEvents.get(key));
            }

            String keys = "";
            String values = returned.toString();

            if (hasKeys) {
                keys = s.joinKeys().toString();
                keys = keys.substring(0, keys.length() - 1);
                keys += "|";
                values = values.substring(1);
            }

            // remove [ ]
            assertEquals(prefix, match.toString(), keys + values);
        }
    }
}
