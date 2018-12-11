/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.sql.proto.RequestInfo.CLIENT_IDS;

public class SqlClearCursorRequestTests extends AbstractSerializingTestCase<SqlClearCursorRequest> {
    
    public RequestInfo requestInfo;

    @Before
    public void setup() {
        requestInfo = new RequestInfo(randomFrom(Mode.values()),
                randomFrom(randomFrom(CLIENT_IDS), randomAlphaOfLengthBetween(10, 20)));
    }

    @Override
    protected SqlClearCursorRequest createTestInstance() {
        return new SqlClearCursorRequest(requestInfo, randomAlphaOfLength(100));
    }

    @Override
    protected Writeable.Reader<SqlClearCursorRequest> instanceReader() {
        return SqlClearCursorRequest::new;
    }

    @Override
    protected SqlClearCursorRequest doParseInstance(XContentParser parser) {
        return SqlClearCursorRequest.fromXContent(parser);
    }
    
    private RequestInfo randomRequestInfo() {
        return new RequestInfo(randomFrom(Mode.values()), randomFrom(randomFrom(CLIENT_IDS), requestInfo.clientId()));
    }

    @Override
    protected SqlClearCursorRequest mutateInstance(SqlClearCursorRequest instance) throws IOException {
        @SuppressWarnings("unchecked")
        Consumer<SqlClearCursorRequest> mutator = randomFrom(
                request -> request.requestInfo(randomValueOtherThan(request.requestInfo(), this::randomRequestInfo)),
                request -> request.setCursor(randomValueOtherThan(request.getCursor(), SqlQueryResponseTests::randomStringCursor))
        );
        SqlClearCursorRequest newRequest = new SqlClearCursorRequest(instance.requestInfo(), instance.getCursor());
        mutator.accept(newRequest);
        return newRequest;
    }
}

