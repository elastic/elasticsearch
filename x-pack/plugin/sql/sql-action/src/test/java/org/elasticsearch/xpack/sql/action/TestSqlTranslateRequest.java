/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.proto.CoreProtocol;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class TestSqlTranslateRequest extends SqlTranslateRequest implements ToXContentObject {

    public TestSqlTranslateRequest(
        String query,
        List<SqlTypedParamValue> params,
        QueryBuilder filter,
        Map<String, Object> runtimeMappings,
        ZoneId zoneId,
        int fetchSize,
        TimeValue requestTimeout,
        TimeValue pageTimeout,
        RequestInfo requestInfo
    ) {
        super(query, params, filter, runtimeMappings, zoneId, fetchSize, requestTimeout, pageTimeout, requestInfo);
    }

    public TestSqlTranslateRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // the null parameters are not defined on translate request
        // and sql-proto class serialization will skip these
        org.elasticsearch.xpack.sql.proto.SqlQueryRequest protoInstance = new org.elasticsearch.xpack.sql.proto.SqlQueryRequest(
            this.query(),
            this.params(),
            this.zoneId(),
            this.catalog(),
            this.fetchSize(),
            ProtoShim.toProto(this.requestTimeout()),
            ProtoShim.toProto(this.pageTimeout()),
            null,
            null,
            this.requestInfo(),
            CoreProtocol.FIELD_MULTI_VALUE_LENIENCY,
            CoreProtocol.INDEX_INCLUDE_FROZEN,
            null,
            null,
            false,
            null
        );
        return SqlTestUtils.toXContentBuilder(builder, this, protoInstance);
    }
}
