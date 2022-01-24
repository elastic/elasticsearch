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
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class TestSqlQueryRequest extends SqlQueryRequest implements ToXContentObject {

    public TestSqlQueryRequest(
        String query,
        List<SqlTypedParamValue> params,
        QueryBuilder filter,
        Map<String, Object> runtimeMappings,
        ZoneId zoneId,
        String catalog,
        int fetchSize,
        TimeValue requestTimeout,
        TimeValue pageTimeout,
        Boolean columnar,
        String cursor,
        RequestInfo requestInfo,
        boolean fieldMultiValueLeniency,
        boolean indexIncludeFrozen,
        TimeValue waitForCompletionTimeout,
        boolean keepOnCompletion,
        TimeValue keepAlive
    ) {
        super(
            query,
            params,
            filter,
            runtimeMappings,
            zoneId,
            catalog,
            fetchSize,
            requestTimeout,
            pageTimeout,
            columnar,
            cursor,
            requestInfo,
            fieldMultiValueLeniency,
            indexIncludeFrozen,
            waitForCompletionTimeout,
            keepOnCompletion,
            keepAlive
        );
    }

    public TestSqlQueryRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        org.elasticsearch.xpack.sql.proto.SqlQueryRequest protoInstance = new org.elasticsearch.xpack.sql.proto.SqlQueryRequest(
            this.query(),
            this.params(),
            this.zoneId(),
            this.catalog(),
            this.fetchSize(),
            ProtoShim.toProto(this.requestTimeout()),
            ProtoShim.toProto(this.pageTimeout()),
            this.columnar(),
            this.cursor(),
            this.requestInfo(),
            this.fieldMultiValueLeniency(),
            this.indexIncludeFrozen(),
            this.binaryCommunication(),
            ProtoShim.toProto(this.waitForCompletionTimeout()),
            this.keepOnCompletion(),
            ProtoShim.toProto(this.keepAlive())
        );
        return SqlTestUtils.toXContentBuilder(builder, this, protoInstance);
    }
}
