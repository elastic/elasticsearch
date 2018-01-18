/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Consumer;

public class SqlListColumnsRequestTests extends AbstractSerializingTestCase<SqlListColumnsRequest> {
    public AbstractSqlRequest.Mode testMode;

    @Before
    public void setup() {
        testMode = randomFrom(AbstractSqlRequest.Mode.values());
    }

    @Override
    protected SqlListColumnsRequest createTestInstance() {
        return new SqlListColumnsRequest(testMode, randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<SqlListColumnsRequest> instanceReader() {
        return SqlListColumnsRequest::new;
    }

    @Override
    protected SqlListColumnsRequest doParseInstance(XContentParser parser) {
        return SqlListColumnsRequest.fromXContent(parser, testMode);
    }

    @Override
    protected SqlListColumnsRequest mutateInstance(SqlListColumnsRequest instance) throws IOException {
        @SuppressWarnings("unchecked")
        Consumer<SqlListColumnsRequest> mutator = randomFrom(
                request -> request.mode(randomValueOtherThan(request.mode(), () -> randomFrom(AbstractSqlRequest.Mode.values()))),
                request -> request.setColumnPattern(randomValueOtherThan(request.getColumnPattern(), () -> randomAlphaOfLength(10))),
                request -> request.setTablePattern(randomValueOtherThan(request.getTablePattern(), () -> randomAlphaOfLength(10)))
        );
        SqlListColumnsRequest newRequest =
                new SqlListColumnsRequest(instance.mode(), instance.getTablePattern(), instance.getColumnPattern());
        mutator.accept(newRequest);
        return newRequest;
    }
}
