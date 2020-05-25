/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;
import org.elasticsearch.xpack.eql.execution.assembler.SeriesUtils.SeriesSpec;
import org.elasticsearch.xpack.eql.execution.payload.Payload;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class SequenceRuntimeTests extends ESTestCase {

    private final List<Map<Integer, Tuple<String, String>>> events;
    private final List<List<String>> matches;
    private final Map<Integer, String> allEvents;
    private final int lineNumber;
    private final boolean hasKeys;
    private final List<HitExtractor> keyExtractors;
    private final HitExtractor tsExtractor;

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
        public Long extract(SearchHit hit) {
            return (long) hit.docId();
        }
    }
    
    static class KeyExtractor extends EmptyHitExtractor {
        @Override
        public String extract(SearchHit hit) {
            return hit.getId();
        }
    }

    class TestCriterion extends Criterion {
        private final int ordinal;

        TestCriterion(int ordinal) {
            super(SearchSourceBuilder.searchSource().size(ordinal), keyExtractors, tsExtractor);
            this.ordinal = ordinal;
        }

        int ordinal() {
            return ordinal;
        }

        @Override
        public int hashCode() {
            return ordinal;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            SequenceRuntimeTests.TestCriterion other = (SequenceRuntimeTests.TestCriterion) obj;
            return ordinal == other.ordinal;
        }

        @Override
        public String toString() {
            return format(null, "[{}] -> {}", ordinal, events.get(ordinal).values());
        }
    }

    static class TestPayload implements Payload<SearchHit> {
        private final List<SearchHit> hits;
        private final Map<Integer, Tuple<String, String>> events;

        TestPayload(Map<Integer, Tuple<String, String>> events) {
            this.events = events;
            this.hits = new ArrayList<>(events.size());

            for (Entry<Integer, Tuple<String, String>> entry : events.entrySet()) {
                Tuple<String, String> value = entry.getValue();
                // save the timestamp as docId (int) and the key as id (string)
                SearchHit searchHit = new SearchHit(entry.getKey(), value.v1(), null, null);
                hits.add(searchHit);
            }
        }

        @Override
        public boolean timedOut() {
            return false;
        }

        @Override
        public TimeValue timeTook() {
            return TimeValue.ZERO;
        }

        @Override
        public Object[] nextKeys() {
            return new Object[0];
        }

        @Override
        public List<SearchHit> values() {
            return hits;
        }

        @Override
        public String toString() {
            return events.values().toString();
        }
    }


    public SequenceRuntimeTests(String testName, int lineNumber, SeriesSpec spec) {
        this.lineNumber = lineNumber;
        this.events = spec.eventsPerCriterion;
        this.matches = spec.matches;
        this.allEvents = spec.allEvents;
        this.hasKeys = spec.hasKeys;
        this.keyExtractors = hasKeys ? singletonList(new KeyExtractor()) : emptyList();
        this.tsExtractor = TimestampExtractor.INSTANCE;
    }

    @ParametersFactory(shuffle = false, argumentFormatting = "%0$s")
    public static Iterable<Object[]> parameters() throws Exception {
        return SeriesUtils.readSpec("/sequences.series-spec");
    }
    
    public void test() {
        int stages = events.size();
        List<Criterion> criteria = new ArrayList<>(stages);
        // pass the items for each query through the Criterion
        for (int i = 0; i < stages; i++) {
            // set the index as size in the search source
            criteria.add(new TestCriterion(i));
        }
        
        // convert the results through a test specific payload
        SequenceRuntime runtime = new SequenceRuntime(criteria, (c, l) -> {
            Map<Integer, Tuple<String, String>> evs = events.get(c.size());
            l.onResponse(new TestPayload(evs));
        });

        // finally make the assertion at the end of the listener
        runtime.execute(wrap(this::checkResults, ex -> {
            throw ExceptionsHelper.convertToRuntime(ex);
        }));
    }

    private void checkResults(Results results) {
        List<Sequence> seq = results.sequences();
        String prefix = "Line " + lineNumber + ":";
        assertNotNull(prefix + "no matches found", seq);
        assertEquals(prefix + "different sequences matched ", matches.size(), seq.size());
        
        for (int i = 0; i < seq.size(); i++) {
            Sequence s = seq.get(i);
            List<String> match = matches.get(i);
            List<String> returned = new ArrayList<>();
            for (int j = 0; j < match.size(); j++) {
                int key = ((Number) TimestampExtractor.INSTANCE.extract(s.events().get(j))).intValue();
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