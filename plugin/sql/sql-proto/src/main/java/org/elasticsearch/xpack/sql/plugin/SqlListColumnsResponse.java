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
public class SqlListColumnsResponse extends ActionResponse implements ToXContentObject {
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SqlListColumnsResponse, Void> PARSER = new ConstructingObjectParser<>("sql", true,
            objects -> new SqlListColumnsResponse((List<MetaColumnInfo>) objects[0]));

    public static final ParseField COLUMNS = new ParseField("columns");

    static {
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> MetaColumnInfo.fromXContent(p), COLUMNS);
    }

    private List<MetaColumnInfo> columns;

    public SqlListColumnsResponse() {
    }

    public SqlListColumnsResponse(List<MetaColumnInfo> columns) {
        this.columns = columns;
    }

    /**
     * The key that must be sent back to SQL to access the next page of
     * results. If equal to "" then there is no next page.
     */
    public List<MetaColumnInfo> getColumns() {
        return columns;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        columns = in.readList(MetaColumnInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(columns);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startArray("columns");
            {
                for (MetaColumnInfo column : columns) {
                    column.toXContent(builder, params);
                }
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    public static SqlListColumnsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlListColumnsResponse that = (SqlListColumnsResponse) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
