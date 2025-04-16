/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public abstract class AbstractXContentTestCase<T extends ToXContent> extends ESTestCase {
    public static final int NUMBER_OF_TEST_RUNS = 20;

    public static <T> XContentTester<T> xContentTester(
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
        Supplier<T> instanceSupplier,
        CheckedBiConsumer<T, XContentBuilder, IOException> toXContent,
        CheckedFunction<XContentParser, T, IOException> fromXContent
    ) {
        return new XContentTester<>(createParser, x -> instanceSupplier.get(), (testInstance, xContentType) -> {
            try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
                toXContent.accept(testInstance, builder);
                return BytesReference.bytes(builder);
            }
        }, fromXContent);
    }

    public static <T extends ToXContent> XContentTester<T> xContentTester(
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
        Supplier<T> instanceSupplier,
        CheckedFunction<XContentParser, T, IOException> fromXContent
    ) {
        return xContentTester(createParser, instanceSupplier, ToXContent.EMPTY_PARAMS, fromXContent);
    }

    public static <T extends ToXContent> XContentTester<T> xContentTester(
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
        Supplier<T> instanceSupplier,
        ToXContent.Params toXContentParams,
        CheckedFunction<XContentParser, T, IOException> fromXContent
    ) {
        return new XContentTester<>(
            createParser,
            x -> instanceSupplier.get(),
            (testInstance, xContentType) -> XContentHelper.toXContent(testInstance, xContentType, toXContentParams, false),
            fromXContent
        );
    }

    public static <T extends ToXContent> XContentTester<T> xContentTester(
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
        Function<XContentType, T> instanceSupplier,
        ToXContent.Params toXContentParams,
        CheckedFunction<XContentParser, T, IOException> fromXContent
    ) {
        return new XContentTester<>(
            createParser,
            instanceSupplier,
            (testInstance, xContentType) -> XContentHelper.toXContent(testInstance, xContentType, toXContentParams, false),
            fromXContent
        );
    }

    public static <T extends ChunkedToXContent> XContentTester<T> chunkedXContentTester(
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
        Function<XContentType, T> instanceSupplier,
        ToXContent.Params toXContentParams,
        CheckedFunction<XContentParser, T, IOException> fromXContent
    ) {
        return new XContentTester<>(createParser, instanceSupplier, (testInstance, xContentType) -> {
            try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
                var serialization = testInstance.toXContentChunked(toXContentParams);
                if (testInstance.isFragment()) {
                    builder.startObject();
                }
                while (serialization.hasNext()) {
                    serialization.next().toXContent(builder, toXContentParams);
                }
                if (testInstance.isFragment()) {
                    builder.endObject();
                }
                return BytesReference.bytes(builder);
            }
        }, fromXContent);
    }

    /**
     * Tests converting to and from xcontent.
     */
    public static class XContentTester<T> {
        private final CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser;
        private final Function<XContentType, T> instanceSupplier;
        private final CheckedBiFunction<T, XContentType, BytesReference, IOException> toXContent;
        private final CheckedFunction<XContentParser, T, IOException> fromXContent;

        private int numberOfTestRuns = NUMBER_OF_TEST_RUNS;
        private boolean supportsUnknownFields = false;
        private String[] shuffleFieldsExceptions = Strings.EMPTY_ARRAY;
        private Predicate<String> randomFieldsExcludeFilter = field -> false;
        private BiConsumer<T, T> assertEqualsConsumer = (expectedInstance, newInstance) -> {
            assertNotSame(newInstance, expectedInstance);
            assertEquals(expectedInstance, newInstance);
            assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
        };
        private boolean assertToXContentEquivalence = true;
        private Consumer<T> dispose = t -> {};

        private XContentTester(
            CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
            Function<XContentType, T> instanceSupplier,
            CheckedBiFunction<T, XContentType, BytesReference, IOException> toXContent,
            CheckedFunction<XContentParser, T, IOException> fromXContent
        ) {
            this.createParser = createParser;
            this.instanceSupplier = instanceSupplier;
            this.toXContent = toXContent;
            this.fromXContent = fromXContent;
        }

        public void test() throws IOException {
            for (int runs = 0; runs < numberOfTestRuns; runs++) {
                XContentType xContentType = randomFrom(XContentType.values()).canonical();
                T testInstance = null;
                try {
                    if (xContentType.equals(XContentType.YAML)) {
                        testInstance = randomValueOtherThanMany(instance -> {
                            // unicode character U+0085 (NEXT LINE (NEL)) doesn't survive YAML round trip tests (see #97716)
                            // get a new random instance if we detect this character in the xContent output
                            try {
                                return toXContent.apply(instance, xContentType).utf8ToString().contains("\u0085");
                            } catch (IOException e) {
                                throw new AssertionError(e);
                            }
                        }, () -> instanceSupplier.apply(xContentType));
                    } else {
                        testInstance = instanceSupplier.apply(xContentType);
                    }
                    BytesReference originalXContent = toXContent.apply(testInstance, xContentType);
                    BytesReference shuffledContent = insertRandomFieldsAndShuffle(
                        originalXContent,
                        xContentType,
                        supportsUnknownFields,
                        shuffleFieldsExceptions,
                        randomFieldsExcludeFilter,
                        createParser
                    );
                    final T parsed;
                    try (XContentParser parser = createParser.apply(XContentFactory.xContent(xContentType), shuffledContent)) {
                        parsed = fromXContent.apply(parser);
                    }
                    try {
                        assertEqualsConsumer.accept(testInstance, parsed);
                        if (assertToXContentEquivalence) {
                            assertToXContentEquivalent(
                                toXContent.apply(testInstance, xContentType),
                                toXContent.apply(parsed, xContentType),
                                xContentType
                            );
                        }
                    } finally {
                        dispose.accept(parsed);
                    }
                } finally {
                    if (testInstance != null) {
                        dispose.accept(testInstance);
                    }
                }
            }
        }

        public XContentTester<T> numberOfTestRuns(int numberOfTestRuns) {
            this.numberOfTestRuns = numberOfTestRuns;
            return this;
        }

        public XContentTester<T> supportsUnknownFields(boolean supportsUnknownFields) {
            this.supportsUnknownFields = supportsUnknownFields;
            return this;
        }

        public XContentTester<T> shuffleFieldsExceptions(String[] shuffleFieldsExceptions) {
            this.shuffleFieldsExceptions = shuffleFieldsExceptions;
            return this;
        }

        public XContentTester<T> randomFieldsExcludeFilter(Predicate<String> randomFieldsExcludeFilter) {
            this.randomFieldsExcludeFilter = randomFieldsExcludeFilter;
            return this;
        }

        public XContentTester<T> assertEqualsConsumer(BiConsumer<T, T> assertEqualsConsumer) {
            this.assertEqualsConsumer = assertEqualsConsumer;
            return this;
        }

        public XContentTester<T> assertToXContentEquivalence(boolean assertToXContentEquivalence) {
            this.assertToXContentEquivalence = assertToXContentEquivalence;
            return this;
        }

        public XContentTester<T> dispose(Consumer<T> dispose) {
            this.dispose = dispose;
            return this;
        }
    }

    public static <T extends ToXContent> void testFromXContent(
        int numberOfTestRuns,
        Supplier<T> instanceSupplier,
        boolean supportsUnknownFields,
        String[] shuffleFieldsExceptions,
        Predicate<String> randomFieldsExcludeFilter,
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParserFunction,
        CheckedFunction<XContentParser, T, IOException> fromXContent,
        BiConsumer<T, T> assertEqualsConsumer,
        boolean assertToXContentEquivalence,
        ToXContent.Params toXContentParams
    ) throws IOException {
        testFromXContent(
            numberOfTestRuns,
            instanceSupplier,
            supportsUnknownFields,
            shuffleFieldsExceptions,
            randomFieldsExcludeFilter,
            createParserFunction,
            fromXContent,
            assertEqualsConsumer,
            assertToXContentEquivalence,
            toXContentParams,
            t -> {}
        );
    }

    public static <T extends ToXContent> void testFromXContent(
        int numberOfTestRuns,
        Supplier<T> instanceSupplier,
        boolean supportsUnknownFields,
        String[] shuffleFieldsExceptions,
        Predicate<String> randomFieldsExcludeFilter,
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParserFunction,
        CheckedFunction<XContentParser, T, IOException> fromXContent,
        BiConsumer<T, T> assertEqualsConsumer,
        boolean assertToXContentEquivalence,
        ToXContent.Params toXContentParams,
        Consumer<T> dispose
    ) throws IOException {
        xContentTester(createParserFunction, instanceSupplier, toXContentParams, fromXContent).numberOfTestRuns(numberOfTestRuns)
            .supportsUnknownFields(supportsUnknownFields)
            .shuffleFieldsExceptions(shuffleFieldsExceptions)
            .randomFieldsExcludeFilter(randomFieldsExcludeFilter)
            .assertEqualsConsumer(assertEqualsConsumer)
            .assertToXContentEquivalence(assertToXContentEquivalence)
            .dispose(dispose)
            .test();
    }

    /**
     * Generic test that creates new instance from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public final void testFromXContent() throws IOException {
        testFromXContent(
            NUMBER_OF_TEST_RUNS,
            this::createTestInstance,
            supportsUnknownFields(),
            getShuffleFieldsExceptions(),
            getRandomFieldsExcludeFilter(),
            this::createParser,
            this::parseInstance,
            this::assertEqualInstances,
            assertToXContentEquivalence(),
            getToXContentParams(),
            this::dispose
        );
    }

    /**
     * Callback invoked after a test instance is no longer needed that can be overridden to release resources associated with the instance.
     * @param instance test instance that is no longer used
     */
    protected void dispose(T instance) {}

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    protected abstract T createTestInstance();

    private T parseInstance(XContentParser parser) throws IOException {
        T parsedInstance = doParseInstance(parser);
        assertNull(parser.nextToken());
        return parsedInstance;
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     */
    protected abstract T doParseInstance(XContentParser parser) throws IOException;

    protected void assertEqualInstances(T expectedInstance, T newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    protected boolean assertToXContentEquivalence() {
        return true;
    }

    /**
     * Indicates whether the parser supports unknown fields or not. In case it does, such behaviour will be tested by
     * inserting random fields before parsing and checking that they don't make parsing fail.
     */
    protected abstract boolean supportsUnknownFields();

    /**
     * Returns a predicate that given the field name indicates whether the field has to be excluded from random fields insertion or not
     */
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return Predicates.never();
    }

    /**
     * Fields that have to be ignored when shuffling as part of testFromXContent
     */
    protected String[] getShuffleFieldsExceptions() {
        return Strings.EMPTY_ARRAY;
    }

    /**
     * Params that have to be provided when calling {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)}
     */
    protected ToXContent.Params getToXContentParams() {
        return ToXContent.EMPTY_PARAMS;
    }

    static BytesReference insertRandomFieldsAndShuffle(
        BytesReference xContent,
        XContentType xContentType,
        boolean supportsUnknownFields,
        String[] shuffleFieldsExceptions,
        Predicate<String> randomFieldsExcludeFilter,
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParserFunction
    ) throws IOException {
        BytesReference withRandomFields;
        if (supportsUnknownFields) {
            // add a few random fields to check that the parser is lenient on new fields
            withRandomFields = XContentTestUtils.insertRandomFields(xContentType, xContent, randomFieldsExcludeFilter, random());
        } else {
            withRandomFields = xContent;
        }
        try (XContentParser parserWithRandomFields = createParserFunction.apply(XContentFactory.xContent(xContentType), withRandomFields)) {
            return BytesReference.bytes(ESTestCase.shuffleXContent(parserWithRandomFields, false, shuffleFieldsExceptions));
        }
    }

}
