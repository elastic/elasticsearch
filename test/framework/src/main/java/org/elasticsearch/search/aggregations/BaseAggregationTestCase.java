/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class BaseAggregationTestCase<AB extends AbstractAggregationBuilder<AB>> extends AbstractBuilderTestCase {

    protected static final String IP_FIELD_NAME = "mapped_ip";

    protected abstract AB createTestAggregatorBuilder();

    /**
     * Generic test that creates new AggregatorFactory from the test
     * AggregatorFactory and checks both for equality and asserts equality on
     * the two queries.
     */
    public void testFromXContent() throws IOException {
        AB testAgg = createTestAggregatorBuilder();
        AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder().addAggregator(testAgg);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        factoriesBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentBuilder shuffled = shuffleXContent(builder);
        try (XContentParser parser = createParser(shuffled)) {
            AggregationBuilder newAgg = parse(parser);
            assertNotSame(newAgg, testAgg);
            assertEquals(testAgg, newAgg);
            assertEquals(testAgg.hashCode(), newAgg.hashCode());
        }
    }

    public void testSupportsConcurrentExecution() {
        int cardinality = randomIntBetween(-1, 100);
        AB builder = createTestAggregatorBuilder();
        boolean supportsConcurrency = builder.supportsParallelCollection(field -> cardinality);
        AggregationBuilder bucketBuilder = new HistogramAggregationBuilder("test");
        assertTrue(bucketBuilder.supportsParallelCollection(field -> cardinality));
        bucketBuilder.subAggregation(builder);
        assertThat(bucketBuilder.supportsParallelCollection(field -> cardinality), equalTo(supportsConcurrency));
    }

    /**
     * Create at least 2 aggregations and test equality and hash
     */
    public void testFromXContentMulti() throws IOException {
        AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder();
        List<AB> testAggs = createTestAggregatorBuilders();

        for (AB testAgg : testAggs) {
            factoriesBuilder.addAggregator(testAgg);
        }

        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        factoriesBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentBuilder shuffled = shuffleXContent(builder);

        AggregatorFactories.Builder parsed;
        try (XContentParser parser = createParser(shuffled)) {
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = AggregatorFactories.parseAggregators(parser);
        }

        assertThat(parsed.getAggregatorFactories(), hasSize(testAggs.size()));
        assertThat(parsed.getPipelineAggregatorFactories(), hasSize(0));
        assertEquals(factoriesBuilder, parsed);
        assertEquals(factoriesBuilder.hashCode(), parsed.hashCode());
    }

    /**
     * Create at least 2 aggregations and test equality and hash
     */
    public void testSerializationMulti() throws IOException {
        AggregatorFactories.Builder builder = AggregatorFactories.builder();
        List<AB> testAggs = createTestAggregatorBuilders();

        for (AB testAgg : testAggs) {
            builder.addAggregator(testAgg);
        }

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            builder.writeTo(output);

            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry())) {
                AggregatorFactories.Builder newBuilder = new AggregatorFactories.Builder(in);

                assertEquals(builder, newBuilder);
                assertEquals(builder.hashCode(), newBuilder.hashCode());
                assertNotSame(builder, newBuilder);
            }
        }
    }

    /**
     * Generic test that checks that the toString method renders the XContent
     * correctly.
     */
    public void testToString() throws IOException {
        AB testAgg = createTestAggregatorBuilder();
        String toString = randomBoolean() ? Strings.toString(testAgg) : testAgg.toString();
        AggregationBuilder newAgg;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), toString)) {
            newAgg = parse(parser);
        }
        assertNotSame(newAgg, testAgg);
        assertEquals(testAgg, newAgg);
        assertEquals(testAgg.hashCode(), newAgg.hashCode());
    }

    protected AggregationBuilder parse(XContentParser parser) throws IOException {
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        assertThat(parsed.getAggregatorFactories(), hasSize(1));
        assertThat(parsed.getPipelineAggregatorFactories(), hasSize(0));
        AggregationBuilder newAgg = parsed.getAggregatorFactories().iterator().next();
        assertNull(parser.nextToken());
        assertNotNull(newAgg);
        return newAgg;
    }

    /**
     * Test serialization and deserialization of the test AggregatorFactory.
     */
    public void testSerialization() throws IOException {
        AB testAgg = createTestAggregatorBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(testAgg);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry())) {
                AggregationBuilder deserialized = in.readNamedWriteable(AggregationBuilder.class);
                assertEquals(testAgg, deserialized);
                assertEquals(testAgg.hashCode(), deserialized.hashCode());
                assertNotSame(testAgg, deserialized);
                @SuppressWarnings("unchecked") // They are .equal so its safe
                AB castDeserialized = (AB) deserialized;
                assertToXContentAfterSerialization(testAgg, castDeserialized);
            }
        }
    }

    /**
     * Make sure serialization preserves toXContent.
     */
    protected void assertToXContentAfterSerialization(AB original, AB deserialized) throws IOException {
        assertEquals(Strings.toString(original), Strings.toString(deserialized));
    }

    public void testEqualsAndHashcode() throws IOException {
        // TODO we only change name and boost, we should extend by any sub-test supplying a "mutate" method that randomly changes one
        // aspect of the object under test
        checkEqualsAndHashCode(createTestAggregatorBuilder(), this::copyAggregation);
    }

    public void testShallowCopy() {
        AB original = createTestAggregatorBuilder();
        AggregationBuilder clone = original.shallowCopy(original.factoriesBuilder, original.metadata);
        assertNotSame(original, clone);
        assertEquals(original, clone);
    }

    public void testPlainDeepCopyEquivalentToStreamCopy() throws IOException {
        AB original = createTestAggregatorBuilder();
        AggregationBuilder deepClone = AggregationBuilder.deepCopy(original, Function.identity());
        assertNotSame(deepClone, original);
        AggregationBuilder streamClone = copyAggregation(original);
        assertNotSame(streamClone, original);
        assertEquals(streamClone, deepClone);
    }

    // we use the streaming infra to create a copy of the query provided as
    // argument
    protected AB copyAggregation(AB agg) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            agg.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry())) {
                @SuppressWarnings("unchecked")
                AB secondAgg = (AB) namedWriteableRegistry().getReader(AggregationBuilder.class, agg.getWriteableName()).read(in);
                return secondAgg;
            }
        }
    }

    public String randomNumericField() {
        int randomInt = randomInt(3);
        return switch (randomInt) {
            case 0 -> DATE_FIELD_NAME;
            case 1 -> DOUBLE_FIELD_NAME;
            case 2 -> INT_FIELD_NAME;
            default -> INT_FIELD_NAME;
        };
    }

    protected void randomFieldOrScript(ValuesSourceAggregationBuilder<?> factory, String field) {
        int choice = randomInt(2);
        switch (choice) {
            case 0 -> factory.field(field);
            case 1 -> {
                factory.field(field);
                factory.script(mockScript("_value + 1"));
            }
            case 2 -> factory.script(mockScript("doc[" + field + "] + 1"));
            default -> throw new AssertionError("Unknown random operation [" + choice + "]");
        }
    }

    private List<AB> createTestAggregatorBuilders() {
        int numberOfAggregatorBuilders = randomIntBetween(2, 10);

        // ensure that we do not create 2 aggregations with the same name
        Set<String> names = new HashSet<>();
        List<AB> aggBuilders = new ArrayList<>();

        while (names.size() < numberOfAggregatorBuilders) {
            AB aggBuilder = createTestAggregatorBuilder();

            if (names.add(aggBuilder.getName())) {
                aggBuilders.add(aggBuilder);
            }
        }
        return aggBuilders;
    }
}
