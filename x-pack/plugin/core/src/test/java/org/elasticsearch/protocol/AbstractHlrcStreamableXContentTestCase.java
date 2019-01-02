/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol;

import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public abstract class AbstractHlrcStreamableXContentTestCase<T extends ToXContent & Streamable, H>
        extends AbstractStreamableXContentTestCase<T> {

    /**
     * Generic test that creates new instance of HLRC request/response from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public final void testHlrcFromXContent() throws IOException {
        xContentTester(this::createParser, this::createTestInstance, getToXContentParams(),
            p -> convertHlrcToInternal(doHlrcParseInstance(p)))
            .numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .supportsUnknownFields(supportsUnknownFields())
            .shuffleFieldsExceptions(getShuffleFieldsExceptions())
            .randomFieldsExcludeFilter(getRandomFieldsExcludeFilter())
            .assertEqualsConsumer(this::assertEqualInstances)
            .assertToXContentEquivalence(true)
            .test();
    }

    /**
     * Parses to a new HLRC instance using the provided {@link XContentParser}
     */
    public abstract H doHlrcParseInstance(XContentParser parser) throws IOException;

    /**
     * Converts a HLRC instance to a XPack instance
     */
    public abstract T convertHlrcToInternal(H instance);

    //TODO this would be final ideally: why do both responses need to parse from xcontent, only one (H) should? I think that T#fromXContent
    //are only there for testing and could go away? Then the additional testHlrcFromXContent is also no longer needed.
    @Override
    protected T doParseInstance(XContentParser parser) throws IOException {
        return convertHlrcToInternal(doHlrcParseInstance(parser));
    }
}
