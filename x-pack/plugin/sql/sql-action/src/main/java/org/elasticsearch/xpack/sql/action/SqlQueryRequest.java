/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to perform an sql query
 */
public class SqlQueryRequest extends AbstractSqlQueryRequest {
    private static final ObjectParser<SqlQueryRequest, Void> PARSER = objectParser(SqlQueryRequest::new);
    static final ParseField COLUMNAR = new ParseField("columnar");
    static final ParseField FIELD_MULTI_VALUE_LENIENCY = new ParseField("field_multi_value_leniency");


    static {
        PARSER.declareString(SqlQueryRequest::cursor, CURSOR);
        PARSER.declareBoolean(SqlQueryRequest::columnar, COLUMNAR);
        PARSER.declareBoolean(SqlQueryRequest::fieldMultiValueLeniency, FIELD_MULTI_VALUE_LENIENCY);
    }

    private String cursor = "";
    /*
     * Using the Boolean object here so that SqlTranslateRequest to set this to null (since it doesn't need a "columnar" parameter).
     * See {@code SqlTranslateRequest.toXContent}
     */
    private Boolean columnar = Boolean.FALSE;
    private boolean fieldMultiValueLeniency = Protocol.FIELD_MULTI_VALUE_LENIENCY;

    public SqlQueryRequest() {
        super();
    }

    public SqlQueryRequest(String query, List<SqlTypedParamValue> params, QueryBuilder filter, ZoneId zoneId,
                           int fetchSize, TimeValue requestTimeout, TimeValue pageTimeout, Boolean columnar,
                           String cursor, RequestInfo requestInfo, boolean fieldMultiValueLeniency) {
        super(query, params, filter, zoneId, fetchSize, requestTimeout, pageTimeout, requestInfo);
        this.cursor = cursor;
        this.columnar = columnar;
        this.fieldMultiValueLeniency = fieldMultiValueLeniency;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if ((false == Strings.hasText(query())) && Strings.hasText(cursor) == false) {
            validationException = addValidationError("one of [query] or [cursor] is required", validationException);
        }
        return validationException;
    }

    /**
     * The key that must be sent back to SQL to access the next page of
     * results.
     */
    public String cursor() {
        return cursor;
    }

    /**
     * The key that must be sent back to SQL to access the next page of
     * results.
     */
    public SqlQueryRequest cursor(String cursor) {
        if (cursor == null) {
            throw new IllegalArgumentException("cursor may not be null.");
        }
        this.cursor = cursor;
        return this;
    }
    
    /**
     * Should format the values in a columnar fashion or not (default false).
     * Depending on the format used (csv, tsv, txt, json etc) this setting will be taken into
     * consideration or not, depending on whether it even makes sense for that specific format or not.
     */
    public Boolean columnar() {
        return columnar;
    }

    public SqlQueryRequest columnar(boolean columnar) {
        this.columnar = columnar;
        return this;
    }


    public SqlQueryRequest fieldMultiValueLeniency(boolean leniency) {
        this.fieldMultiValueLeniency = leniency;
        return this;
    }

    public boolean fieldMultiValueLeniency() {
        return fieldMultiValueLeniency;
    }

    public SqlQueryRequest(StreamInput in) throws IOException {
        super(in);
        cursor = in.readString();
        columnar = in.readOptionalBoolean();
        fieldMultiValueLeniency = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cursor);
        out.writeOptionalBoolean(columnar);
        out.writeBoolean(fieldMultiValueLeniency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cursor, columnar);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
                && Objects.equals(cursor, ((SqlQueryRequest) obj).cursor)
                && Objects.equals(columnar, ((SqlQueryRequest) obj).columnar)
                && fieldMultiValueLeniency == ((SqlQueryRequest) obj).fieldMultiValueLeniency;
    }

    @Override
    public String getDescription() {
        return "SQL [" + query() + "][" + filter() + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // This is needed just to test round-trip compatibility with proto.SqlQueryRequest
        return new org.elasticsearch.xpack.sql.proto.SqlQueryRequest(query(), params(), zoneId(), fetchSize(), requestTimeout(),
            pageTimeout(), filter(), columnar(), cursor(), requestInfo(), fieldMultiValueLeniency())
                .toXContent(builder, params);
    }

    public static SqlQueryRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}