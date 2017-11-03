/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;

public class SqlTranslateRequestTests extends AbstractStreamableTestCase<SqlTranslateAction.Request> {

    @Override
    protected SqlTranslateAction.Request createTestInstance() {
        return new SqlTranslateAction.Request(randomAlphaOfLength(10), randomDateTimeZone(), between(1, Integer.MAX_VALUE),
                randomTV(), randomTV());
    }

    private TimeValue randomTV() {
        return TimeValue.parseTimeValue(randomTimeValue(), null, "test");
    }

    @Override
    protected SqlTranslateAction.Request createBlankInstance() {
        return new SqlTranslateAction.Request();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MutateFunction<SqlTranslateAction.Request> getMutateFunction() {
        return randomFrom(
                request -> (SqlTranslateAction.Request) getCopyFunction().copy(request)
                        .query(randomValueOtherThan(request.query(), () -> randomAlphaOfLength(5))),
                request -> (SqlTranslateAction.Request) getCopyFunction().copy(request)
                        .timeZone(randomValueOtherThan(request.timeZone(), ESTestCase::randomDateTimeZone)),
                request -> (SqlTranslateAction.Request) getCopyFunction().copy(request)
                        .fetchSize(randomValueOtherThan(request.fetchSize(), () -> between(1, Integer.MAX_VALUE))),
                request -> (SqlTranslateAction.Request) getCopyFunction().copy(request)
                        .requestTimeout(randomValueOtherThan(request.requestTimeout(), () -> randomTV())),
                request -> (SqlTranslateAction.Request) getCopyFunction().copy(request)
                        .pageTimeout(randomValueOtherThan(request.pageTimeout(), () -> randomTV())));
    }
}
