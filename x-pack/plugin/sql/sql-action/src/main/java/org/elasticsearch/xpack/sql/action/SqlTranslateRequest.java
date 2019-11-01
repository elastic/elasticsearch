/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request for the sql action for translating SQL queries into ES requests
 */
public class SqlTranslateRequest extends AbstractSqlQueryRequest {
    private static final ObjectParser<SqlTranslateRequest, Void> PARSER = objectParser(SqlTranslateRequest::new);

    public SqlTranslateRequest() {
        super();
    }

    public SqlTranslateRequest(String query, List<SqlTypedParamValue> params, QueryBuilder filter, ZoneId zoneId,
                               int fetchSize, TimeValue requestTimeout, TimeValue pageTimeout, RequestInfo requestInfo) {
        super(query, params, filter, zoneId, fetchSize, requestTimeout, pageTimeout, requestInfo);
    }

    public SqlTranslateRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if ((false == Strings.hasText(query()))) {
            validationException = addValidationError("query is required", validationException);
        }
        return validationException;
    }

    @Override
    public String getDescription() {
        return "SQL Translate [" + query() + "][" + filter() + "]";
    }

    public static SqlTranslateRequest fromXContent(XContentParser parser) {
        SqlTranslateRequest request = PARSER.apply(parser, null);
        return request;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // This is needed just to test parsing of SqlTranslateRequest, so we can reuse SqlQuerySerialization
        return new SqlQueryRequest(query(), params(), zoneId(), fetchSize(), requestTimeout(), pageTimeout(), 
            filter(), 
            null, 
            null, 
            requestInfo(),
            false, 
            false,
            null).toXContent(builder, params);
    }
}
