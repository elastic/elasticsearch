/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.function.Predicate;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public abstract class AbstractSerializingTestCase<T extends ToXContent & Writeable> extends AbstractWireSerializingTestCase<T> {

    /**
     * Generic test that creates new instance from the test instance and checks
     * both for equality and asserts equality on the two instances.
     */
    public final void testFromXContent() throws IOException {
        xContentTester(this::createParser, this::createXContextTestInstance, getToXContentParams(), this::doParseInstance)
            .numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .supportsUnknownFields(supportsUnknownFields())
            .shuffleFieldsExceptions(getShuffleFieldsExceptions())
            .randomFieldsExcludeFilter(getRandomFieldsExcludeFilter())
            .assertEqualsConsumer(this::assertEqualInstances)
            .assertToXContentEquivalence(assertToXContentEquivalence())
            .test();
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     */
    protected abstract T doParseInstance(XContentParser parser) throws IOException;

    /**
     * Creates a random instance to use in the xcontent tests.
     * Override this method if the random instance that you build
     * should be aware of the {@link XContentType} used in the test.
     */
    protected T createXContextTestInstance(XContentType xContentType) {
        return createTestInstance();
    }

    /**
     * Indicates whether the parser supports unknown fields or not. In case it does, such behaviour will be tested by
     * inserting random fields before parsing and checking that they don't make parsing fail.
     */
    protected boolean supportsUnknownFields() {
        return false;
    }

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
     * Params that have to be provided when calling calling {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)}
     */
    protected ToXContent.Params getToXContentParams() {
        return ToXContent.EMPTY_PARAMS;
    }

    /**
     * Whether or not to assert equivalence of the {@link org.elasticsearch.common.xcontent.XContent} of the test instance and the instance
     * parsed from the {@link org.elasticsearch.common.xcontent.XContent} of the test instance.
     *
     * @return true if equivalence should be asserted, otherwise false
     */
    protected boolean assertToXContentEquivalence() {
        return true;
    }

    /**
     * @return a random date between 1970 and ca 2065
     */
    protected Date randomDate() {
        return new Date(randomLongBetween(0, 3000000000000L));
    }

    /**
     * @return a random instant between 1970 and ca 2065
     */
    protected Instant randomInstant() {
        return Instant.ofEpochSecond(randomLongBetween(0, 3000000000L), randomLongBetween(0, 999999999));
    }
}
