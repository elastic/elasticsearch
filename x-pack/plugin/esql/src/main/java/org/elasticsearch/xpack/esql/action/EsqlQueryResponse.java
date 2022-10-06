/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class EsqlQueryResponse extends ActionResponse implements ToXContentObject {

    private final List<ColumnInfo> columns;
    private final List<List<Object>> values;

    private static final InstantiatingObjectParser<EsqlQueryResponse, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<EsqlQueryResponse, Void> parser = InstantiatingObjectParser.builder(
            "esql/query_response",
            true,
            EsqlQueryResponse.class
        );
        parser.declareObjectArray(constructorArg(), (p, c) -> ColumnInfo.fromXContent(p), new ParseField("columns"));
        parser.declareField(constructorArg(), (p, c) -> p.list(), new ParseField("values"), ObjectParser.ValueType.OBJECT_ARRAY);
        PARSER = parser.build();
    }

    public EsqlQueryResponse(StreamInput in) throws IOException {
        super(in);
        int colCount = in.readVInt();

        List<ColumnInfo> columns = new ArrayList<>(colCount);
        for (int r = 0; r < colCount; r++) {
            columns.add(new ColumnInfo(in.readString(), in.readString()));
        }
        this.columns = unmodifiableList(columns);

        List<List<Object>> values = new ArrayList<>(colCount);

        int rowCount = in.readVInt();
        for (int r = 0; r < rowCount; r++) {
            List<Object> row = new ArrayList<>(colCount);
            for (int c = 0; c < colCount; c++) {
                row.add(in.readGenericValue());
            }
            values.add(unmodifiableList(row));
        }

        this.values = unmodifiableList(values);
    }

    public EsqlQueryResponse(List<ColumnInfo> columns, List<List<Object>> values) {
        this.columns = columns;
        this.values = values;
    }

    public List<ColumnInfo> columns() {
        return columns;
    }

    public List<List<Object>> values() {
        return values;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("columns");
        for (ColumnInfo col : columns) {
            col.toXContent(builder, params);
        }
        builder.endArray();
        builder.startArray("values");
        for (List<Object> rows : values) {
            builder.startArray();
            for (Object value : rows) {
                builder.value(value);
            }
            builder.endArray();
        }
        builder.endArray();
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(columns.size());

        for (ColumnInfo column : columns) {
            out.writeString(column.name());
            out.writeString(column.type());
        }

        out.writeVInt(values.size());
        for (List<Object> row : values) {
            for (Object value : row) {
                out.writeGenericValue(value);
            }
        }
    }

    public static EsqlQueryResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EsqlQueryResponse that = (EsqlQueryResponse) o;
        return Objects.equals(columns, that.columns) && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, values);
    }
}
