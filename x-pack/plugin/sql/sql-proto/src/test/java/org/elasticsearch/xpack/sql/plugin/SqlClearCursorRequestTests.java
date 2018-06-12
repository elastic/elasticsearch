/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Consumer;

public class SqlClearCursorRequestTests extends AbstractSerializingTestCase<SqlClearCursorRequest> {
    public Mode testMode;

    @Before
    public void setup() {
        testMode = randomFrom(Mode.values());
    }

    @Override
    protected SqlClearCursorRequest createTestInstance() {
        return new SqlClearCursorRequest(testMode, randomAlphaOfLength(100));
    }

    @Override
    protected Writeable.Reader<SqlClearCursorRequest> instanceReader() {
        return SqlClearCursorRequest::new;
    }

    @Override
    protected SqlClearCursorRequest doParseInstance(XContentParser parser) {
        return SqlClearCursorRequest.fromXContent(parser, testMode);
    }

    @Override
    protected SqlClearCursorRequest mutateInstance(SqlClearCursorRequest instance) throws IOException {
        @SuppressWarnings("unchecked")
        Consumer<SqlClearCursorRequest> mutator = randomFrom(
                request -> request.mode(randomValueOtherThan(request.mode(), () -> randomFrom(Mode.values()))),
                request -> request.setCursor(randomValueOtherThan(request.getCursor(), SqlQueryResponseTests::randomStringCursor))
        );
        SqlClearCursorRequest newRequest = new SqlClearCursorRequest(instance.mode(), instance.getCursor());
        mutator.accept(newRequest);
        return newRequest;

    }
}

