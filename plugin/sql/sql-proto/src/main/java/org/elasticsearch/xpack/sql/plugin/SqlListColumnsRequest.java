/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.internal.Nullable;
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
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Request to get a list of SQL-supported columns of an index
 * <p>
 * It needs to be CompositeIndicesRequest because we resolve wildcards a non-standard SQL
 * manner
 */
public class SqlListColumnsRequest extends AbstractSqlRequest implements ToXContentObject, CompositeIndicesRequest {
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SqlListColumnsRequest, Mode> PARSER =
            new ConstructingObjectParser<>("sql_list_tables", true, (objects, mode) -> new SqlListColumnsRequest(
                    mode,
                    (String) objects[0],
                    (String) objects[1]
            ));

    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField("table_pattern"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("column_pattern"));
    }

    @Nullable
    private String tablePattern;
    @Nullable
    private String columnPattern;


    public SqlListColumnsRequest() {
    }

    public SqlListColumnsRequest(Mode mode, String tablePattern, String columnPattern) {
        super(mode);
        this.tablePattern = tablePattern;
        this.columnPattern = columnPattern;
    }


    public SqlListColumnsRequest(StreamInput in) throws IOException {
        super(in);
        this.tablePattern = in.readOptionalString();
        this.columnPattern = in.readOptionalString();
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
        out.writeOptionalString(tablePattern);
        out.writeOptionalString(columnPattern);
    }

    @Override
    public String getDescription() {
        return "SQL List Columns[" + getTablePattern() + ", " + getColumnPattern() + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (tablePattern != null) {
                builder.field("table_pattern", tablePattern);
            }
            if (columnPattern != null) {
                builder.field("column_pattern", columnPattern);
            }
        }
        return builder.endObject();
    }

    public static SqlListColumnsRequest fromXContent(XContentParser parser, Mode mode) {
        return PARSER.apply(parser, mode);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        SqlListColumnsRequest that = (SqlListColumnsRequest) o;
        return Objects.equals(tablePattern, that.tablePattern) &&
                Objects.equals(columnPattern, that.columnPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tablePattern, columnPattern);
    }
}