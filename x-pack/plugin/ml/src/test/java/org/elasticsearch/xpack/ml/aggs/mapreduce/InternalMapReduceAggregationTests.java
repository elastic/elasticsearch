/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class InternalMapReduceAggregationTests extends InternalAggregationTestCase<InternalMapReduceAggregation> {

    private static String[] WORDS = new String[] { "apple", "banana", "orange", "peach", "strawberry" };

    static class WordCountMapReducer implements MapReducer {

        public static String MAP_REDUCER_NAME = "word-count-test-aggregation";
        public static String AGG_NAME = "internal-map-reduce-aggregation-test";

        private Map<String, Long> frequencies = new HashMap<>();

        // for testing
        private Map<String, Long> reducedFrequencies;

        WordCountMapReducer() {}

        WordCountMapReducer(StreamInput in) throws IOException {
            this.frequencies = in.readMap(StreamInput::readString, StreamInput::readLong);
        }

        @Override
        public String getWriteableName() {
            return MAP_REDUCER_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(frequencies, StreamOutput::writeString, StreamOutput::writeLong);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Aggregation.CommonFields.BUCKETS.getPreferredName(), frequencies);
            return builder;
        }

        @Override
        public String getAggregationWritableName() {
            return AGG_NAME;
        }

        @Override
        public void map(Stream<Tuple<String, List<Object>>> keyValues) {
            keyValues.forEach(
                v -> { v.v2().stream().forEach(word -> { frequencies.compute((String) word, (k, c) -> (c == null) ? 1 : c + 1); }); }
            );
        }

        @Override
        public void reduce(Stream<MapReducer> partitions) {
            reducedFrequencies = new HashMap<>();

            partitions.forEach(p -> {
                WordCountMapReducer wc = (WordCountMapReducer) p;
                wc.frequencies.forEach((key, value) -> reducedFrequencies.merge(key, value, (v1, v2) -> v1 + v2));
            });
        }

        public Map<String, Long> getFrequencies() {
            return frequencies;
        }

        public Map<String, Long> getReducedFrequencies() {
            return reducedFrequencies;
        }
    }

    static class ParsedWordCountMapReduceAggregation extends ParsedAggregation {

        private Map<String, Long> frequencies;

        @SuppressWarnings("unchecked")
        static ParsedWordCountMapReduceAggregation fromXContent(XContentParser parser, final String name) throws IOException {

            Map<String, Object> values = parser.map();
            Map<String, Long> frequencies = ((Map<String, Object>) values.get(Aggregation.CommonFields.BUCKETS.getPreferredName()))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> ((Integer) e.getValue()).longValue()));

            ParsedWordCountMapReduceAggregation parsed = new ParsedWordCountMapReduceAggregation(
                frequencies,
                (Map<String, Object>) values.get(InternalAggregation.CommonFields.META.getPreferredName())
            );
            parsed.setName(name);
            return parsed;
        }

        ParsedWordCountMapReduceAggregation(Map<String, Long> frequencies, Map<String, Object> metadata) {
            this.frequencies = frequencies;
            this.metadata = metadata;
        }

        @Override
        public String getType() {
            return WordCountMapReducer.AGG_NAME;
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(Aggregation.CommonFields.BUCKETS.getPreferredName(), getFrequencies());
            return builder;
        }

        public Map<String, Long> getFrequencies() {
            return frequencies;
        }
    }

    @Override
    protected InternalMapReduceAggregation createTestInstance(String name, Map<String, Object> metadata) {
        MapReducer mr = new WordCountMapReducer();
        int randomTextLength = randomIntBetween(1, 100);
        List<String> randomText = new ArrayList<>(randomTextLength);

        for (int i = 0; i < randomTextLength; ++i) {
            randomText.add(randomFrom(WORDS));
        }

        mr.map(randomText.stream().map(word -> Tuple.tuple("text", Collections.singletonList(word))));
        return new InternalMapReduceAggregation(name, metadata, mr);
    }

    @Override
    protected void assertReduced(InternalMapReduceAggregation reduced, List<InternalMapReduceAggregation> inputs) {
        Map<String, Long> expectedFrequencies = new HashMap<>();
        for (InternalMapReduceAggregation in : inputs) {
            WordCountMapReducer wc = (WordCountMapReducer) in.getMapReducer();
            wc.frequencies.forEach((key, value) -> expectedFrequencies.merge(key, value, (v1, v2) -> v1 + v2));
        }
        WordCountMapReducer wc = (WordCountMapReducer) reduced.getMapReducer();
        assertMapEquals(expectedFrequencies, wc.getReducedFrequencies());
    }

    @Override
    protected void assertFromXContent(InternalMapReduceAggregation aggregation, ParsedAggregation parsedAggregation) throws IOException {
        ParsedWordCountMapReduceAggregation parsed = (ParsedWordCountMapReduceAggregation) parsedAggregation;
        assertThat(parsed.getName(), equalTo(aggregation.getName()));

        WordCountMapReducer wc = (WordCountMapReducer) aggregation.getMapReducer();
        assertMapEquals(wc.getFrequencies(), parsed.getFrequencies());
    }

    @Override
    protected SearchPlugin registerPlugin() {
        return new MachineLearning(Settings.EMPTY);
    }

    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWritables = new ArrayList<>(super.getNamedWriteables());

        namedWritables.add(
            new NamedWriteableRegistry.Entry(MapReducer.class, WordCountMapReducer.MAP_REDUCER_NAME, WordCountMapReducer::new)
        );
        namedWritables.add(
            new NamedWriteableRegistry.Entry(InternalAggregation.class, WordCountMapReducer.AGG_NAME, InternalMapReduceAggregation::new)
        );

        return namedWritables;
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(
                Aggregation.class,
                new ParseField(WordCountMapReducer.AGG_NAME),
                (p, c) -> ParsedWordCountMapReduceAggregation.fromXContent(p, (String) c)
            )
        );
    }

    private static void assertMapEquals(Map<String, Long> expected, Map<String, Long> actual) {
        assertThat(expected.size(), equalTo(actual.size()));
        for (Entry<String, Long> entry : expected.entrySet()) {
            assertTrue(actual.containsKey(entry.getKey()));
            assertEquals(entry.getValue(), actual.get(entry.getKey()));
        }
    }
}
