/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.MachineLearningTests;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.InternalItemSetMapReduceAggregationTests.WordCountMapReducer.WordCounts;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.ValueFormatter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class InternalItemSetMapReduceAggregationTests extends InternalAggregationTestCase<
    InternalItemSetMapReduceAggregation<WordCounts, WordCounts, WordCounts, WordCounts>> {

    private static String[] WORDS = new String[] { "apple", "banana", "orange", "peach", "strawberry" };

    @Override
    protected InternalItemSetMapReduceAggregation<WordCounts, WordCounts, WordCounts, WordCounts> mutateInstance(
        InternalItemSetMapReduceAggregation<WordCounts, WordCounts, WordCounts, WordCounts> instance
    ) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    static class WordCountMapReducer extends AbstractItemSetMapReducer<WordCounts, WordCounts, WordCounts, WordCounts> {

        static class WordCounts implements ToXContent, Writeable, Closeable {

            final Map<String, Long> frequencies;

            WordCounts() {
                frequencies = new HashMap<>();
            }

            WordCounts(Map<String, Long> frequencies) {
                this.frequencies = frequencies;
            }

            WordCounts(StreamInput in) throws IOException {
                this.frequencies = in.readMap(StreamInput::readLong);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeMap(frequencies, StreamOutput::writeLong);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field(Aggregation.CommonFields.BUCKETS.getPreferredName(), frequencies);
                return builder;
            }

            @Override
            public void close() throws IOException {}
        }

        public static final String MAP_REDUCER_NAME = "word-count-test-aggregation";
        public static final String AGG_NAME = "internal-map-reduce-aggregation-test";

        WordCountMapReducer() {
            super(AGG_NAME, MAP_REDUCER_NAME);
        }

        WordCountMapReducer(StreamInput in) throws IOException {
            super(AGG_NAME, MAP_REDUCER_NAME);
        }

        @Override
        public WordCounts mapInit(BigArrays bigArrays) {
            return new WordCounts();
        }

        @Override
        public WordCounts map(Stream<Tuple<Field, List<Object>>> keyValues, WordCounts wordCounts) {
            keyValues.forEach(v -> v.v2().forEach(word -> wordCounts.frequencies.merge((String) word, 1L, Long::sum)));
            return wordCounts;
        }

        @Override
        public WordCounts readMapReduceContext(StreamInput in, BigArrays bigArrays) throws IOException {
            return new WordCounts(in);
        }

        @Override
        protected WordCounts readResult(StreamInput in, BigArrays bigArrays) throws IOException {
            return new WordCounts(in);
        }

        @Override
        public WordCounts reduceInit(BigArrays bigArrays) {
            return new WordCounts();
        }

        @Override
        public WordCounts reduce(Stream<WordCounts> partitions, WordCounts wordCounts, Supplier<Boolean> isCanceledSupplier) {
            partitions.forEach(
                p -> { p.frequencies.forEach((key, value) -> wordCounts.frequencies.merge(key, value, (v1, v2) -> v1 + v2)); }
            );

            return wordCounts;
        }

        @Override
        public WordCounts reduceFinalize(WordCounts wordCounts, List<Field> fields, Supplier<Boolean> isCanceledSupplier)
            throws IOException {
            return wordCounts;
        }

        @Override
        protected WordCounts mapFinalize(WordCounts mapReduceContext, List<OrdinalLookupFunction> ordinalLookup) {
            return mapReduceContext;
        }

        @Override
        protected WordCounts combine(Stream<WordCounts> partitions, WordCounts mapReduceContext, Supplier<Boolean> isCanceledSupplier) {
            return reduce(partitions, mapReduceContext, isCanceledSupplier);
        }

    }

    @Override
    protected InternalItemSetMapReduceAggregation<WordCounts, WordCounts, WordCounts, WordCounts> createTestInstance(
        String name,
        Map<String, Object> metadata
    ) {
        WordCountMapReducer mr = new WordCountMapReducer();
        int randomTextLength = randomIntBetween(1, 100);
        List<String> randomText = new ArrayList<>(randomTextLength);

        for (int i = 0; i < randomTextLength; ++i) {
            randomText.add(randomFrom(WORDS));
        }

        WordCounts context = mr.mapInit(/* unused: bigarrays */ null);
        Field field1 = new Field("field", 0, DocValueFormat.RAW, ValueFormatter.BYTES_REF);

        context = mr.map(randomText.stream().map(word -> Tuple.tuple(field1, Collections.singletonList(word))), context);
        return new InternalItemSetMapReduceAggregation<>(name, metadata, mr, context, context, Collections.singletonList(field1), false);
    }

    @Override
    protected void assertReduced(
        InternalItemSetMapReduceAggregation<WordCounts, WordCounts, WordCounts, WordCounts> reduced,
        List<InternalItemSetMapReduceAggregation<WordCounts, WordCounts, WordCounts, WordCounts>> inputs
    ) {
        WordCounts wcReduced = reduced.getMapReduceResult();
        Map<String, Long> expectedFrequencies2 = new HashMap<>();

        inputs.forEach(mr -> {
            WordCounts wcInput = mr.getMapFinalContext();
            wcInput.frequencies.forEach((key, value) -> expectedFrequencies2.merge(key, value, (v1, v2) -> v1 + v2));
        });

        assertMapEquals(expectedFrequencies2, wcReduced.frequencies);
    }

    @Override
    protected SearchPlugin registerPlugin() {
        return MachineLearningTests.createTrialLicensedMachineLearning(Settings.EMPTY);
    }

    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWritables = new ArrayList<>(super.getNamedWriteables());

        namedWritables.add(
            new NamedWriteableRegistry.Entry(
                InternalAggregation.class,
                WordCountMapReducer.AGG_NAME,
                in -> new InternalItemSetMapReduceAggregation<>(in, (mapReducerReader) -> {
                    in.readString();
                    return new WordCountMapReducer(mapReducerReader);
                })
            )
        );

        return namedWritables;
    }

    private static void assertMapEquals(Map<String, Long> expected, Map<String, Long> actual) {
        assertThat(expected.size(), equalTo(actual.size()));
        for (Entry<String, Long> entry : expected.entrySet()) {
            assertTrue(actual.containsKey(entry.getKey()));
            assertEquals(entry.getKey(), entry.getValue(), actual.get(entry.getKey()));
        }
    }
}
