/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public abstract class AbstractXContentTestCase<T extends ToXContent> extends ESTestCase {

    protected static final int NUMBER_OF_TEST_RUNS = 20;

    public static <T extends ToXContent> void testFromXContent(int numberOfTestRuns, Supplier<T> instanceSupplier,
                                                               boolean supportsUnknownFields, String[] shuffleFieldsExceptions,
                                                               Predicate<String> randomFieldsExcludeFilter,
                                                               CheckedBiFunction<XContent, BytesReference, XContentParser, IOException>
                                                                       createParserFunction,
                                                               CheckedFunction<XContentParser, T, IOException> parseFunction,
                                                               BiConsumer<T, T> assertEqualsConsumer,
                                                               boolean assertToXContentEquivalence,
                                                               ToXContent.Params toXContentParams) throws IOException {
        for (int runs = 0; runs < numberOfTestRuns; runs++) {
            T testInstance = instanceSupplier.get();
            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference shuffledContent = insertRandomFieldsAndShuffle(testInstance, xContentType, supportsUnknownFields,
                    shuffleFieldsExceptions, randomFieldsExcludeFilter, createParserFunction, toXContentParams);
            XContentParser parser = createParserFunction.apply(XContentFactory.xContent(xContentType), shuffledContent);
            T parsed = parseFunction.apply(parser);
            assertEqualsConsumer.accept(testInstance, parsed);
            if (assertToXContentEquivalence) {
                assertToXContentEquivalent(
                        XContentHelper.toXContent(testInstance, xContentType, toXContentParams, false),
                        XContentHelper.toXContent(parsed, xContentType, toXContentParams, false),
                        xContentType);
            }
        }
    }

    /**
     * Generic test that creates new instance from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public final void testFromXContent() throws IOException {
        testFromXContent(NUMBER_OF_TEST_RUNS, this::createTestInstance, supportsUnknownFields(), getShuffleFieldsExceptions(),
                getRandomFieldsExcludeFilter(), this::createParser, this::parseInstance, this::assertEqualInstances,
                assertToXContentEquivalence(), getToXContentParams());
    }

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
        return field -> false;
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

    static BytesReference insertRandomFieldsAndShuffle(ToXContent testInstance, XContentType xContentType,
            boolean supportsUnknownFields, String[] shuffleFieldsExceptions, Predicate<String> randomFieldsExcludeFilter,
            CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParserFunction,
            ToXContent.Params toXContentParams) throws IOException {
        BytesReference xContent = XContentHelper.toXContent(testInstance, xContentType, toXContentParams, false);
        BytesReference withRandomFields;
        if (supportsUnknownFields) {
            // add a few random fields to check that the parser is lenient on new fields
            withRandomFields = XContentTestUtils.insertRandomFields(xContentType, xContent, randomFieldsExcludeFilter, random());
        } else {
            withRandomFields = xContent;
        }
        XContentParser parserWithRandonFields = createParserFunction.apply(XContentFactory.xContent(xContentType), withRandomFields);
        return BytesReference.bytes(ESTestCase.shuffleXContent(parserWithRandonFields, false, shuffleFieldsExceptions));
    }

}
