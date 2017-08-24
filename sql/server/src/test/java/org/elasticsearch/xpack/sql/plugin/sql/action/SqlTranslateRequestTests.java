/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;

public class SqlTranslateRequestTests extends AbstractStreamableTestCase<SqlTranslateRequest> {

    @Override
    protected SqlTranslateRequest createTestInstance() {
        return new SqlTranslateRequest(randomAlphaOfLength(10), randomDateTimeZone(), between(1, Integer.MAX_VALUE));
    }

    @Override
    protected SqlTranslateRequest createBlankInstance() {
        return new SqlTranslateRequest();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MutateFunction<SqlTranslateRequest> getMutateFunction() {
        return randomFrom(
                request -> (SqlTranslateRequest) getCopyFunction().copy(request)
                        .query(randomValueOtherThan(request.query(), () -> randomAlphaOfLength(5))),
                request -> (SqlTranslateRequest) getCopyFunction().copy(request)
                        .timeZone(randomValueOtherThan(request.timeZone(), ESTestCase::randomDateTimeZone)),
                request -> (SqlTranslateRequest) getCopyFunction().copy(request)
                        .fetchSize(randomValueOtherThan(request.fetchSize(), () -> between(1, Integer.MAX_VALUE))));
    }
}