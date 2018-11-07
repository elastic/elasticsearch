/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public abstract class AbstractHlrcXContentTestCase<T extends ToXContent, H> extends AbstractXContentTestCase<T> {

    /**
     * Generic test that creates new instance of HLRC request/response from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public final void testHlrcFromXContent() throws IOException {
        AbstractXContentTestCase.testFromXContent(NUMBER_OF_TEST_RUNS, this::createTestInstance, supportsUnknownFields(),
            getShuffleFieldsExceptions(), getRandomFieldsExcludeFilter(), this::createParser,
            p -> convertHlrcToInternal(doHlrcParseInstance(p)),
            this::assertEqualInstances, true, getToXContentParams());
    }

    /**
     * Parses to a new HLRC instance using the provided {@link XContentParser}
     */
    public abstract H doHlrcParseInstance(XContentParser parser) throws IOException;

    /**
     * Converts a HLRC instance to a XPack instance
     */
    public abstract T convertHlrcToInternal(H instance);
}
