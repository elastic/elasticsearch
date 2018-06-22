/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.type.DataType;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.sql.plugin.SqlTestUtils.randomFilter;
import static org.elasticsearch.xpack.sql.plugin.SqlTestUtils.randomFilterOrNull;

public class SqlQueryRequestTests extends AbstractSerializingTestCase<SqlQueryRequest> {

    public Mode testMode;

    @Before
    public void setup() {
        testMode = randomFrom(Mode.values());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected SqlQueryRequest createTestInstance() {
        return new SqlQueryRequest(testMode, randomAlphaOfLength(10), randomParameters(),
                SqlTestUtils.randomFilterOrNull(random()), randomTimeZone(),
                between(1, Integer.MAX_VALUE), randomTV(), randomTV(), randomAlphaOfLength(10)
        );
    }

    public List<SqlTypedParamValue> randomParameters() {
        if (randomBoolean()) {
            return Collections.emptyList();
        } else {
            int len = randomIntBetween(1, 10);
            List<SqlTypedParamValue> arr = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                @SuppressWarnings("unchecked") Supplier<SqlTypedParamValue> supplier = randomFrom(
                        () -> new SqlTypedParamValue(DataType.BOOLEAN, randomBoolean()),
                        () -> new SqlTypedParamValue(DataType.LONG, randomLong()),
                        () -> new SqlTypedParamValue(DataType.DOUBLE, randomDouble()),
                        () -> new SqlTypedParamValue(DataType.NULL, null),
                        () -> new SqlTypedParamValue(DataType.KEYWORD, randomAlphaOfLength(10))
                );
                arr.add(supplier.get());
            }
            return Collections.unmodifiableList(arr);
        }
    }

    @Override
    protected Writeable.Reader<SqlQueryRequest> instanceReader() {
        return SqlQueryRequest::new;
    }

    private TimeValue randomTV() {
        return TimeValue.parseTimeValue(randomTimeValue(), null, "test");
    }

    @Override
    protected SqlQueryRequest doParseInstance(XContentParser parser) {
        return SqlQueryRequest.fromXContent(parser, testMode);
    }

    @Override
    protected SqlQueryRequest mutateInstance(SqlQueryRequest instance) {
        @SuppressWarnings("unchecked")
        Consumer<SqlQueryRequest> mutator = randomFrom(
                request -> request.mode(randomValueOtherThan(request.mode(), () -> randomFrom(Mode.values()))),
                request -> request.query(randomValueOtherThan(request.query(), () -> randomAlphaOfLength(5))),
                request -> request.params(randomValueOtherThan(request.params(), this::randomParameters)),
                request -> request.timeZone(randomValueOtherThan(request.timeZone(), ESTestCase::randomTimeZone)),
                request -> request.fetchSize(randomValueOtherThan(request.fetchSize(), () -> between(1, Integer.MAX_VALUE))),
                request -> request.requestTimeout(randomValueOtherThan(request.requestTimeout(), this::randomTV)),
                request -> request.filter(randomValueOtherThan(request.filter(),
                        () -> request.filter() == null ? randomFilter(random()) : randomFilterOrNull(random()))),
                request -> request.cursor(randomValueOtherThan(request.cursor(), SqlQueryResponseTests::randomStringCursor))
        );
        SqlQueryRequest newRequest = new SqlQueryRequest(instance.mode(), instance.query(), instance.params(), instance.filter(),
                instance.timeZone(), instance.fetchSize(), instance.requestTimeout(), instance.pageTimeout(), instance.cursor());
        mutator.accept(newRequest);
        return newRequest;
    }
}
