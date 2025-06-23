/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.junit.Before;

import java.util.function.Consumer;

import static org.elasticsearch.xpack.sql.proto.RequestInfo.CLIENT_IDS;

public class SqlClearCursorRequestTests extends AbstractXContentSerializingTestCase<TestSqlClearCursorRequest> {

    public RequestInfo requestInfo;

    @Before
    public void setup() {
        requestInfo = new RequestInfo(randomFrom(Mode.values()), randomFrom(randomFrom(CLIENT_IDS), randomAlphaOfLengthBetween(10, 20)));
    }

    @Override
    protected TestSqlClearCursorRequest createXContextTestInstance(XContentType xContentType) {
        SqlTestUtils.assumeXContentJsonOrCbor(xContentType);
        return super.createXContextTestInstance(xContentType);
    }

    @Override
    protected TestSqlClearCursorRequest createTestInstance() {
        TestSqlClearCursorRequest result = new TestSqlClearCursorRequest(requestInfo, randomAlphaOfLength(100));
        result.binaryCommunication(randomBoolean());
        return result;
    }

    @Override
    protected Writeable.Reader<TestSqlClearCursorRequest> instanceReader() {
        return TestSqlClearCursorRequest::new;
    }

    @Override
    protected TestSqlClearCursorRequest doParseInstance(XContentParser parser) {
        return SqlTestUtils.clone(TestSqlClearCursorRequest.fromXContent(parser), instanceReader(), getNamedWriteableRegistry());
    }

    private RequestInfo randomRequestInfo() {
        return new RequestInfo(randomFrom(Mode.values()), randomFrom(randomFrom(CLIENT_IDS), requestInfo.clientId()));
    }

    @Override
    protected TestSqlClearCursorRequest mutateInstance(TestSqlClearCursorRequest instance) {
        @SuppressWarnings("unchecked")
        Consumer<TestSqlClearCursorRequest> mutator = randomFrom(
            request -> request.requestInfo(randomValueOtherThan(request.requestInfo(), this::randomRequestInfo)),
            request -> request.setCursor(randomValueOtherThan(request.getCursor(), SqlQueryResponseTests::randomStringCursor)),
            request -> request.binaryCommunication(randomValueOtherThan(request.binaryCommunication(), () -> randomBoolean()))
        );
        TestSqlClearCursorRequest newRequest = new TestSqlClearCursorRequest(instance.requestInfo(), instance.getCursor());
        newRequest.binaryCommunication(instance.binaryCommunication());
        mutator.accept(newRequest);
        return newRequest;
    }
}
