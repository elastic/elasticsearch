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
 * Request to get a list of SQL-supported columns of an index
 * <p>
 * It needs to be CompositeIndicesRequest because we resolve wildcards a non-standard SQL
 * manner
 */
public class SqlListColumnsRequest extends ActionRequest implements ToXContentObject, CompositeIndicesRequest {
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SqlListColumnsRequest, Void> PARSER =
            new ConstructingObjectParser<>("sql_list_tables", true, objects -> new SqlListColumnsRequest(
                    (String) objects[0],
                    (String) objects[1]
            ));

    public static final ParseField TABLE_PATTERN = new ParseField("table_pattern");
    public static final ParseField COLUMN_PATTERN = new ParseField("column_pattern");

    static {
        PARSER.declareString(constructorArg(), TABLE_PATTERN);
        PARSER.declareString(constructorArg(), COLUMN_PATTERN);
    }

    private String tablePattern;
    private String columnPattern;


    public SqlListColumnsRequest() {
    }

    public SqlListColumnsRequest(String tablePattern, String columnPattern) {
        this.tablePattern = tablePattern;
        this.columnPattern = columnPattern;
    }


    public SqlListColumnsRequest(StreamInput in) throws IOException {
        super(in);
        this.tablePattern = in.readString();
        this.columnPattern = in.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (tablePattern == null) {
            validationException = addValidationError("[index_pattern] is required", validationException);
        }
        if (columnPattern == null) {
            validationException = addValidationError("[column_pattern] is required", validationException);
        }
        return validationException;
    }

    /**
     * The index pattern for the results
     */
    public String getTablePattern() {
        return tablePattern;
    }

    public void setTablePattern(String tablePattern) {
        this.tablePattern = tablePattern;
    }

    /**
     * The column pattern for the results
     */
    public String getColumnPattern() {
        return columnPattern;
    }

    public void setColumnPattern(String columnPattern) {
        this.columnPattern = columnPattern;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(tablePattern);
        out.writeString(columnPattern);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlListColumnsRequest that = (SqlListColumnsRequest) o;
        return Objects.equals(tablePattern, that.tablePattern) &&
                Objects.equals(columnPattern, that.columnPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePattern, columnPattern);
    }

    @Override
    public String getDescription() {
        return "SQL List Columns[" + getTablePattern() + ", " + getColumnPattern() + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("table_pattern", tablePattern);
            builder.field("column_pattern", columnPattern);
        }
        return builder.endObject();
    }

    public static SqlListColumnsRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}