/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class DataStreamAlias extends AbstractDiffable<DataStreamAlias> implements ToXContentObject {

    public static final ParseField DATA_STREAMS_FIELD = new ParseField("data_streams");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStreamAlias, String> PARSER = new ConstructingObjectParser<>(
        "data_stream_alias",
        false,
        (args, name) -> new DataStreamAlias(name, (List<String>) args[0])
    );

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), DATA_STREAMS_FIELD);
    }

    private final String name;
    private final List<String> dataStreams;

    public DataStreamAlias(String name, List<String> dataStreams) {
        this.name = Objects.requireNonNull(name);
        this.dataStreams = List.copyOf(dataStreams);
    }

    public DataStreamAlias(StreamInput in) throws IOException {
        this.name = in.readString();
        this.dataStreams = in.readStringList();
    }

    public String getName() {
        return name;
    }

    public List<String> getDataStreams() {
        return dataStreams;
    }

    public static Diff<DataStreamAlias> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(DataStreamAlias::new, in);
    }

    public static DataStreamAlias fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token");
        }
        String name = parser.currentName();
        DataStreamAlias alias = PARSER.parse(parser, name);
        return alias;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(DATA_STREAMS_FIELD.getPreferredName(), dataStreams);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringCollection(dataStreams);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStreamAlias that = (DataStreamAlias) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(dataStreams, that.dataStreams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataStreams);
    }
}
