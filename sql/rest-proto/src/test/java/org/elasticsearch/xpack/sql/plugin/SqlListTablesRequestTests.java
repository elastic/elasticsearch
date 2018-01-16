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

public class SqlListTablesRequestTests extends AbstractSerializingTestCase<SqlListTablesRequest> {

    public AbstractSqlRequest.Mode testMode;

    @Before
    public void setup() {
        testMode = randomFrom(AbstractSqlRequest.Mode.values());
    }

    @Override
    protected SqlListTablesRequest createTestInstance() {
        return new SqlListTablesRequest(testMode, randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<SqlListTablesRequest> instanceReader() {
        return SqlListTablesRequest::new;
    }

    @Override
    protected SqlListTablesRequest doParseInstance(XContentParser parser) {
        return SqlListTablesRequest.fromXContent(parser, testMode);
    }

    @Override
    protected SqlListTablesRequest mutateInstance(SqlListTablesRequest instance) throws IOException {
        @SuppressWarnings("unchecked")
        Consumer<SqlListTablesRequest> mutator = randomFrom(
                request -> request.mode(randomValueOtherThan(request.mode(), () -> randomFrom(AbstractSqlRequest.Mode.values()))),
                request -> request.setPattern(randomValueOtherThan(request.getPattern(), () -> randomAlphaOfLength(10)))
        );
        SqlListTablesRequest newRequest = new SqlListTablesRequest(instance.mode(), instance.getPattern());
        mutator.accept(newRequest);
        return newRequest;
    }
}
