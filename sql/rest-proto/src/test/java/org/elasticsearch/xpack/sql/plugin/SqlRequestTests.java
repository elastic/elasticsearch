/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;
import org.elasticsearch.xpack.sql.test.SqlTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlRequestTests extends AbstractStreamableTestCase<SqlRequest> {
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.addAll(searchModule.getNamedWriteables());
        return new NamedWriteableRegistry(namedWriteables);
    }

    @Override
    protected SqlRequest createTestInstance() {
        return new SqlRequest(randomAlphaOfLength(10), SqlTestUtils.randomFilterOrNull(random()), randomDateTimeZone(),
                between(1, Integer.MAX_VALUE), randomTV(), randomTV(), randomAlphaOfLength(10));
    }

    private TimeValue randomTV() {
        return TimeValue.parseTimeValue(randomTimeValue(), null, "test");
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
                        .cursor(randomValueOtherThan(request.cursor(), SqlResponseTests::randomStringCursor)),
                request -> (SqlRequest) getCopyFunction().copy(request)
                        .query(randomValueOtherThan(request.query(), () -> randomAlphaOfLength(5))),
                request -> (SqlRequest) getCopyFunction().copy(request)
                        .timeZone(randomValueOtherThan(request.timeZone(), ESTestCase::randomDateTimeZone)),
                request -> (SqlRequest) getCopyFunction().copy(request)
                        .fetchSize(randomValueOtherThan(request.fetchSize(), () -> between(1, Integer.MAX_VALUE))),
                request -> (SqlRequest) getCopyFunction().copy(request)
                        .requestTimeout(randomValueOtherThan(request.requestTimeout(), () -> randomTV())),
                request -> (SqlRequest) getCopyFunction().copy(request)
                        .pageTimeout(randomValueOtherThan(request.pageTimeout(), () -> randomTV())),
                request -> (SqlRequest) getCopyFunction().copy(request).filter(randomValueOtherThan(request.filter(),
                        () -> request.filter() == null ? SqlTestUtils.randomFilter(random()) : SqlTestUtils.randomFilterOrNull(random()))));
    }
}
