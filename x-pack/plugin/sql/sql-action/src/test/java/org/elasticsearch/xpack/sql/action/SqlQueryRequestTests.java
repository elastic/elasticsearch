/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.elasticsearch.xpack.sql.action.SqlTestUtils.randomFilter;
import static org.elasticsearch.xpack.sql.action.SqlTestUtils.randomFilterOrNull;
import static org.elasticsearch.xpack.sql.proto.RequestInfo.CLIENT_IDS;

public class SqlQueryRequestTests extends AbstractWireSerializingTestCase<SqlQueryRequest> {

    public RequestInfo requestInfo;

    @Before
    public void setup() {
        requestInfo = new RequestInfo(randomFrom(Mode.values()),
                randomFrom(randomFrom(CLIENT_IDS), randomAlphaOfLengthBetween(10, 20)));
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
    protected SqlQueryRequest createTestInstance() {
        return new SqlQueryRequest(randomAlphaOfLength(10), randomParameters(), SqlTestUtils.randomFilterOrNull(random()),
                randomZone(), between(1, Integer.MAX_VALUE), randomTV(),
                randomTV(), randomBoolean(), randomAlphaOfLength(10), requestInfo,
                randomBoolean(), randomBoolean()
        );
    }
    
    @Override
    protected Writeable.Reader<SqlQueryRequest> instanceReader() {
        return SqlQueryRequest::new;
    }

    @Override
    protected SqlQueryRequest mutateInstance(SqlQueryRequest instance) {
        @SuppressWarnings("unchecked")
        Consumer<SqlQueryRequest> mutator = randomFrom(
                request -> mutateRequestInfo(instance, request),
                request -> request.query(randomValueOtherThan(request.query(), () -> randomAlphaOfLength(5))),
                request -> request.params(randomValueOtherThan(request.params(), this::randomParameters)),
                request -> request.zoneId(randomValueOtherThan(request.zoneId(), ESTestCase::randomZone)),
                request -> request.fetchSize(randomValueOtherThan(request.fetchSize(), () -> between(1, Integer.MAX_VALUE))),
                request -> request.requestTimeout(randomValueOtherThan(request.requestTimeout(), this::randomTV)),
                request -> request.filter(randomValueOtherThan(request.filter(),
                        () -> request.filter() == null ? randomFilter(random()) : randomFilterOrNull(random()))),
                request -> request.columnar(randomValueOtherThan(request.columnar(), () -> randomBoolean())),
                request -> request.cursor(randomValueOtherThan(request.cursor(), SqlQueryResponseTests::randomStringCursor))
        );
        SqlQueryRequest newRequest = new SqlQueryRequest(instance.query(), instance.params(), instance.filter(),
                instance.zoneId(), instance.fetchSize(), instance.requestTimeout(), instance.pageTimeout(), instance.columnar(),
                instance.cursor(), instance.requestInfo(), instance.fieldMultiValueLeniency(), instance.indexIncludeFrozen());
        mutator.accept(newRequest);
        return newRequest;
    }
    
    private AbstractSqlQueryRequest mutateRequestInfo(SqlQueryRequest oldRequest, SqlQueryRequest newRequest) {
        RequestInfo requestInfo = randomValueOtherThan(newRequest.requestInfo(), this::randomRequestInfo);
        newRequest.requestInfo(requestInfo);
        if (Mode.isDriver(oldRequest.requestInfo().mode()) && Mode.isDriver(requestInfo.mode()) == false) {
            for(SqlTypedParamValue param : oldRequest.params()) {
                param.hasExplicitType(false);
            }
        }
        if (Mode.isDriver(oldRequest.requestInfo().mode()) == false && Mode.isDriver(requestInfo.mode())) {
            for(SqlTypedParamValue param : oldRequest.params()) {
                param.hasExplicitType(true);
            }
        }
        
        return newRequest;
    }
    
    public void testFromXContent() throws IOException {
        xContentTester(this::createParser, this::createTestInstance, SqlQueryRequestTests::toXContent, this::doParseInstance)
            .numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .supportsUnknownFields(false)
            .shuffleFieldsExceptions(Strings.EMPTY_ARRAY)
            .randomFieldsExcludeFilter(field -> false)
            .assertEqualsConsumer(this::assertEqualInstances)
            .assertToXContentEquivalence(true)
            .test();
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

    public List<SqlTypedParamValue> randomParameters() {
        if (randomBoolean()) {
            return Collections.emptyList();
        } else {
            int len = randomIntBetween(1, 10);
            List<SqlTypedParamValue> arr = new ArrayList<>(len);
            boolean hasExplicitType = Mode.isDriver(this.requestInfo.mode());
            for (int i = 0; i < len; i++) {
                @SuppressWarnings("unchecked") Supplier<SqlTypedParamValue> supplier = randomFrom(
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

    private SqlQueryRequest doParseInstance(XContentParser parser) {
        return SqlQueryRequest.fromXContent(parser);
    }

    /**
     * This is needed because {@link SqlQueryRequest#toXContent(XContentBuilder, org.elasticsearch.common.xcontent.ToXContent.Params)}
     * is not serializing {@link SqlTypedParamValue} according to the request's {@link Mode} and it shouldn't, in fact.
     * For testing purposes, different serializing methods for {@link SqlTypedParamValue} are necessary so that
     * {@link SqlQueryRequest#fromXContent(XContentParser)} populates {@link SqlTypedParamValue#hasExplicitType()}
     * properly.
     */
    private static void toXContent(SqlQueryRequest request, XContentBuilder builder) throws IOException {
        builder.startObject();
        if (request.query() != null) {
            builder.field("query", request.query());
        }
        builder.field("mode", request.mode().toString());
        if (request.clientId() != null) {
            builder.field("client_id", request.clientId());
        }
        if (request.params() != null && request.params().isEmpty() == false) {
            builder.startArray("params");
            for (SqlTypedParamValue val : request.params()) {
                if (Mode.isDriver(request.mode())) {
                    builder.startObject();
                    builder.field("type", val.type);
                    builder.field("value", val.value);
                    builder.endObject();
                } else {
                    builder.value(val.value);
                }
            }
            builder.endArray();
        }
        if (request.zoneId() != null) {
            builder.field("time_zone", request.zoneId().getId());
        }
        if (request.fetchSize() != Protocol.FETCH_SIZE) {
            builder.field("fetch_size", request.fetchSize());
        }
        if (request.requestTimeout() != Protocol.REQUEST_TIMEOUT) {
            builder.field("request_timeout", request.requestTimeout().getStringRep());
        }
        if (request.pageTimeout() != Protocol.PAGE_TIMEOUT) {
            builder.field("page_timeout", request.pageTimeout().getStringRep());
        }
        if (request.filter() != null) {
            builder.field("filter");
            request.filter().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        if (request.columnar() != null) {
            builder.field("columnar", request.columnar());
        }
        if (request.fieldMultiValueLeniency()) {
            builder.field("field_multi_value_leniency", request.fieldMultiValueLeniency());
        }
        if (request.indexIncludeFrozen()) {
            builder.field("index_include_frozen", request.indexIncludeFrozen());
        }
        if (request.binaryCommunication() != null) {
            builder.field("binary_format", request.binaryCommunication());
        }
        if (request.cursor() != null) {
            builder.field("cursor", request.cursor());
        }
        builder.endObject();
    }
}
