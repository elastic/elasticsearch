/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;
import org.elasticsearch.xpack.sql.plugin.SqlPlugin;

import static org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponseTests.randomCursor;

public class SqlRequestTests extends AbstractStreamableTestCase<SqlRequest> {
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SqlPlugin.getNamedWriteables());
    }

    @Override
    protected SqlRequest createTestInstance() {
        return new SqlRequest(randomAlphaOfLength(10), randomDateTimeZone(), randomCursor())
                .fetchSize(between(1, Integer.MAX_VALUE));
    }

    @Override
    protected SqlRequest createBlankInstance() {
        return new SqlRequest();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MutateFunction<SqlRequest> getMutateFunction() {
        return randomFrom(
                request -> getCopyFunction().copy(request)
                        .query(randomValueOtherThan(request.query(), () -> randomAlphaOfLength(5))),
                request -> getCopyFunction().copy(request)
                        .timeZone(randomValueOtherThan(request.timeZone(), ESTestCase::randomDateTimeZone)),
                request -> getCopyFunction().copy(request)
                        .cursor(randomValueOtherThan(request.cursor(), SqlResponseTests::randomCursor)),
                request -> getCopyFunction().copy(request)
                        .fetchSize(randomValueOtherThan(request.fetchSize(), () -> between(1, Integer.MAX_VALUE))));
    }
}
