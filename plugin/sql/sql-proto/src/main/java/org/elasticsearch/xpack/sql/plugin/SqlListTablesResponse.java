/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response to perform an sql query
 */
public class SqlListTablesResponse extends ActionResponse implements ToXContentObject {
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SqlListTablesResponse, Void> PARSER =
            new ConstructingObjectParser<>("sql_list_tables", true,
                    objects -> new SqlListTablesResponse((List<String>) objects[0]));

    public static final ParseField TABLES = new ParseField("tables");

    static {
        PARSER.declareStringArray(optionalConstructorArg(), TABLES);
    }

    private List<String> tables;

    public SqlListTablesResponse() {
    }

    public SqlListTablesResponse(List<String> tables) {
        this.tables = tables;
    }

    /**
     * The key that must be sent back to SQL to access the next page of
     * results. If equal to "" then there is no next page.
     */
    public List<String> getTables() {
        return tables;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tables = in.readList(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringList(tables);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startArray("tables");
            {
                for (String table : tables) {
                    builder.value(table);
                }
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    public static SqlListTablesResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlListTablesResponse that = (SqlListTablesResponse) o;
        return Objects.equals(tables, that.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tables);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
