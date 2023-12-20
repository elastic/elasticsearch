/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public abstract class AbstractXContentSerializingTestCase<T extends ToXContent & Writeable> extends AbstractSerializationTestCase<T> {

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     */
    protected abstract T doParseInstance(XContentParser parser) throws IOException;

    @Override
    protected T createXContextTestInstance(XContentType xContentType) {
        return createTestInstance();
    }

    @Override
    protected AbstractXContentTestCase.XContentTester<T> createXContentTester() {
        return AbstractXContentTestCase.xContentTester(
            this::createParser,
            this::createXContextTestInstance,
            getToXContentParams(),
            this::doParseInstance
        );
    }

    @Override
    protected ToXContent asXContent(T instance) {
        return instance;
    }
}
