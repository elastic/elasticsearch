/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.TestUtils.randomRuntimeMappings;
import static org.elasticsearch.xpack.sql.action.Protocol.MIN_KEEP_ALIVE;
import static org.elasticsearch.xpack.sql.action.SqlTestUtils.randomFilter;
import static org.elasticsearch.xpack.sql.action.SqlTestUtils.randomFilterOrNull;
import static org.elasticsearch.xpack.sql.proto.RequestInfo.CLIENT_IDS;

public class SqlQueryRequestTests extends AbstractXContentSerializingTestCase<TestSqlQueryRequest> {

    public RequestInfo requestInfo;

    @Before
    public void setup() {
        requestInfo = new RequestInfo(randomFrom(Mode.values()), randomFrom(randomFrom(CLIENT_IDS), randomAlphaOfLengthBetween(10, 20)));
    }

    @Override
    protected TestSqlQueryRequest createXContextTestInstance(XContentType xContentType) {
        SqlTestUtils.assumeXContentJsonOrCbor(xContentType);
        return super.createXContextTestInstance(xContentType);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected TestSqlQueryRequest createTestInstance() {
        return new TestSqlQueryRequest(
            randomAlphaOfLength(10),
            randomParameters(),
            SqlTestUtils.randomFilterOrNull(random()),
            randomRuntimeMappings(),
            randomZone(),
            randomAlphaOfLength(10),
            between(1, Integer.MAX_VALUE),
            randomTV(),
            randomTV(),
            randomBoolean(),
            randomAlphaOfLength(10),
            requestInfo,
            randomBoolean(),
            false, // deprecated
            randomTV(),
            randomBoolean(),
            randomTVGreaterThan(MIN_KEEP_ALIVE),
            randomBoolean()
        );
    }

    @Override
    protected Writeable.Reader<TestSqlQueryRequest> instanceReader() {
        return TestSqlQueryRequest::new;
    }

    @Override
    protected TestSqlQueryRequest doParseInstance(XContentParser parser) {
        return SqlTestUtils.clone(TestSqlQueryRequest.fromXContent(parser), instanceReader(), getNamedWriteableRegistry());
    }

    @Override
    protected TestSqlQueryRequest mutateInstance(TestSqlQueryRequest instance) {
        @SuppressWarnings("unchecked")
        Consumer<SqlQueryRequest> mutator = randomFrom(
            request -> mutateRequestInfo(instance, request),
            request -> request.query(randomValueOtherThan(request.query(), () -> randomAlphaOfLength(5))),
            request -> request.params(randomValueOtherThan(request.params(), this::randomParameters)),
            request -> request.zoneId(randomValueOtherThan(request.zoneId(), ESTestCase::randomZone)),
            request -> request.catalog(randomValueOtherThan(request.catalog(), () -> randomAlphaOfLength(10))),
            request -> request.fetchSize(randomValueOtherThan(request.fetchSize(), () -> between(1, Integer.MAX_VALUE))),
            request -> request.requestTimeout(randomValueOtherThan(request.requestTimeout(), this::randomTV)),
            request -> request.filter(
                randomValueOtherThan(
                    request.filter(),
                    () -> request.filter() == null ? randomFilter(random()) : randomFilterOrNull(random())
                )
            ),
            request -> request.columnar(randomValueOtherThan(request.columnar(), ESTestCase::randomBoolean)),
            request -> request.cursor(randomValueOtherThan(request.cursor(), SqlQueryResponseTests::randomStringCursor)),
            request -> request.waitForCompletionTimeout(randomValueOtherThan(request.waitForCompletionTimeout(), this::randomTV)),
            request -> request.keepOnCompletion(randomValueOtherThan(request.keepOnCompletion(), ESTestCase::randomBoolean)),
            request -> request.keepAlive(randomValueOtherThan(request.keepAlive(), () -> randomTVGreaterThan(MIN_KEEP_ALIVE))),
            request -> request.allowPartialSearchResults(
                randomValueOtherThan(request.allowPartialSearchResults(), ESTestCase::randomBoolean)
            )
        );
        TestSqlQueryRequest newRequest = new TestSqlQueryRequest(
            instance.query(),
            instance.params(),
            instance.filter(),
            instance.runtimeMappings(),
            instance.zoneId(),
            instance.catalog(),
            instance.fetchSize(),
            instance.requestTimeout(),
            instance.pageTimeout(),
            instance.columnar(),
            instance.cursor(),
            instance.requestInfo(),
            instance.fieldMultiValueLeniency(),
            instance.indexIncludeFrozen(),
            instance.waitForCompletionTimeout(),
            instance.keepOnCompletion(),
            instance.keepAlive(),
            instance.allowPartialSearchResults()
        );
        mutator.accept(newRequest);
        return newRequest;
    }

    private AbstractSqlQueryRequest mutateRequestInfo(SqlQueryRequest oldRequest, SqlQueryRequest newRequest) {
        RequestInfo requestInfo = randomValueOtherThan(newRequest.requestInfo(), this::randomRequestInfo);
        newRequest.requestInfo(requestInfo);
        if (Mode.isDriver(oldRequest.requestInfo().mode()) && Mode.isDriver(requestInfo.mode()) == false) {
            for (SqlTypedParamValue param : oldRequest.params()) {
                param.hasExplicitType(false);
            }
        }
        if (Mode.isDriver(oldRequest.requestInfo().mode()) == false && Mode.isDriver(requestInfo.mode())) {
            for (SqlTypedParamValue param : oldRequest.params()) {
                param.hasExplicitType(true);
            }
        }

        return newRequest;
    }

    public void testTimeZoneNullException() {
        final SqlQueryRequest sqlQueryRequest = createTestInstance();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> sqlQueryRequest.zoneId(null));
        assertEquals("time zone may not be null.", e.getMessage());
    }

    private RequestInfo randomRequestInfo() {
        return new RequestInfo(randomFrom(Mode.values()), randomFrom(randomFrom(CLIENT_IDS), requestInfo.clientId()));
    }

    private TimeValue randomTV() {
        return TimeValue.parseTimeValue(randomTimeValue(), null, "test");
    }

    private TimeValue randomTVGreaterThan(TimeValue min) {
        TimeValue value;
        do {
            value = randomTV();
        } while (value.getMillis() < min.getMillis());
        return value;
    }

    public List<SqlTypedParamValue> randomParameters() {
        if (randomBoolean()) {
            return Collections.emptyList();
        } else {
            int len = randomIntBetween(1, 10);
            List<SqlTypedParamValue> arr = new ArrayList<>(len);
            boolean hasExplicitType = Mode.isDriver(this.requestInfo.mode());
            for (int i = 0; i < len; i++) {
                @SuppressWarnings("unchecked")
                Supplier<SqlTypedParamValue> supplier = randomFrom(
                    () -> new SqlTypedParamValue("boolean", randomBoolean(), hasExplicitType),
                    () -> new SqlTypedParamValue("long", randomLong(), hasExplicitType),
                    () -> new SqlTypedParamValue("double", randomDouble(), hasExplicitType),
                    () -> new SqlTypedParamValue("null", null, hasExplicitType),
                    () -> new SqlTypedParamValue("keyword", randomAlphaOfLength(10), hasExplicitType)
                );
                arr.add(supplier.get());
            }
            return Collections.unmodifiableList(arr);
        }
    }
}
