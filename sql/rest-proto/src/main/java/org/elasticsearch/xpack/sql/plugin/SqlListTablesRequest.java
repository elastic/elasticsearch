/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Request to get a list of SQL-supported indices
 * <p>
 * It needs to be CompositeIndicesRequest because we resolve wildcards a non-standard SQL
 * manner
 */
public class SqlListTablesRequest extends ActionRequest implements ToXContentObject, CompositeIndicesRequest {
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SqlListTablesRequest, Void> PARSER
            = new ConstructingObjectParser<>("sql_list_tables", true, objects -> new SqlListTablesRequest((String) objects[0]));

    public static final ParseField TABLE_PATTERN = new ParseField("table_pattern");

    static {
        PARSER.declareString(constructorArg(), TABLE_PATTERN);
    }

    private String pattern;

    public SqlListTablesRequest() {
    }

    public SqlListTablesRequest(String pattern) {
        this.pattern = pattern;
    }


    public SqlListTablesRequest(StreamInput in) throws IOException {
        super(in);
        this.pattern = in.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (pattern == null) {
            validationException = addValidationError("[pattern] is required", validationException);
        }
        return validationException;
    }

    /**
     * The pattern for the results
     */
    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(pattern);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {

        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlListTablesRequest that = (SqlListTablesRequest) o;
        return Objects.equals(pattern, that.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern);
    }

    @Override
    public String getDescription() {
        return "SQL List Tables[" + getPattern() + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("table_pattern", pattern);
        }
        return builder.endObject();
    }

    public static SqlListTablesRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}