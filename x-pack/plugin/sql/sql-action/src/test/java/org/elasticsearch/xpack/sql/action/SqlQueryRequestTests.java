/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

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
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.sql.action.SqlTestUtils.randomFilter;
import static org.elasticsearch.xpack.sql.action.SqlTestUtils.randomFilterOrNull;
import static org.elasticsearch.xpack.sql.proto.RequestInfo.CLI;
import static org.elasticsearch.xpack.sql.proto.RequestInfo.CANVAS;

public class SqlQueryRequestTests extends AbstractSerializingTestCase<SqlQueryRequest> {

    public RequestInfo requestInfo;
    public String clientId;

    @Before
    public void setup() {
        clientId = randomFrom(CLI, CANVAS, randomAlphaOfLengthBetween(10, 20));
        requestInfo = new RequestInfo(randomFrom(Mode.values()), clientId);
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
        return new SqlQueryRequest(randomAlphaOfLength(10), randomParameters(), SqlTestUtils.randomFilterOrNull(random()),
                randomTimeZone(), between(1, Integer.MAX_VALUE),
                randomTV(), randomTV(), randomAlphaOfLength(10), requestInfo
        );
    }
    
    private RequestInfo randomRequestInfo() {
        return new RequestInfo(randomFrom(Mode.values()), randomFrom(CLI, CANVAS, clientId));
    }

    public List<SqlTypedParamValue> randomParameters() {
        if (randomBoolean()) {
            return Collections.emptyList();
        } else {
            int len = randomIntBetween(1, 10);
            List<SqlTypedParamValue> arr = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                @SuppressWarnings("unchecked") Supplier<SqlTypedParamValue> supplier = randomFrom(
                        () -> new SqlTypedParamValue("boolean", randomBoolean()),
                        () -> new SqlTypedParamValue("long", randomLong()),
                        () -> new SqlTypedParamValue("double", randomDouble()),
                        () -> new SqlTypedParamValue("null", null),
                        () -> new SqlTypedParamValue("keyword", randomAlphaOfLength(10))
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
        return SqlQueryRequest.fromXContent(parser, requestInfo);
    }

    @Override
    protected SqlQueryRequest mutateInstance(SqlQueryRequest instance) {
        @SuppressWarnings("unchecked")
        Consumer<SqlQueryRequest> mutator = randomFrom(
                request -> request.requestInfo(randomValueOtherThan(request.requestInfo(), this::randomRequestInfo)),
                request -> request.query(randomValueOtherThan(request.query(), () -> randomAlphaOfLength(5))),
                request -> request.params(randomValueOtherThan(request.params(), this::randomParameters)),
                request -> request.timeZone(randomValueOtherThan(request.timeZone(), ESTestCase::randomTimeZone)),
                request -> request.fetchSize(randomValueOtherThan(request.fetchSize(), () -> between(1, Integer.MAX_VALUE))),
                request -> request.requestTimeout(randomValueOtherThan(request.requestTimeout(), this::randomTV)),
                request -> request.filter(randomValueOtherThan(request.filter(),
                        () -> request.filter() == null ? randomFilter(random()) : randomFilterOrNull(random()))),
                request -> request.cursor(randomValueOtherThan(request.cursor(), SqlQueryResponseTests::randomStringCursor))
        );
        SqlQueryRequest newRequest = new SqlQueryRequest(instance.query(), instance.params(), instance.filter(),
                instance.timeZone(), instance.fetchSize(), instance.requestTimeout(), instance.pageTimeout(), instance.cursor(),
                instance.requestInfo());
        mutator.accept(newRequest);
        return newRequest;
    }

    public void testTimeZoneNullException() {
        final SqlQueryRequest sqlQueryRequest = createTestInstance();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> sqlQueryRequest.timeZone(null));
        assertEquals("time zone may not be null.", e.getMessage());
    }
}
