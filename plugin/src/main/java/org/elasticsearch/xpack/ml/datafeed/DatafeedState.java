/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.util.Locale;

public enum DatafeedState implements Task.Status {

    STARTED, STOPPED;

    public static final String NAME = "DatafeedState";

    public static DatafeedState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static DatafeedState fromStream(StreamInput in) throws IOException {
        int ordinal = in.readVInt();
        if (ordinal < 0 || ordinal >= values().length) {
            throw new IOException("Unknown public enum DatafeedState ordinal [" + ordinal + "]");
        }
        return values()[ordinal];
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(ordinal());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.value(this.toString().toLowerCase(Locale.ROOT));
        return builder;
    }

    public static DatafeedState fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
            throw new ElasticsearchParseException("Unexpected token {}", parser.currentToken());
        }
        return fromString(parser.text());
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
