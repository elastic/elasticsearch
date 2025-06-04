/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

public abstract class TestProjectCustomMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
    private final String data;

    protected TestProjectCustomMetadata(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestProjectCustomMetadata that = (TestProjectCustomMetadata) o;

        if (data.equals(that.data) == false) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    public static <T extends TestProjectCustomMetadata> T readFrom(Function<String, T> supplier, StreamInput in) throws IOException {
        return supplier.apply(in.readString());
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(String name, StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, name, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getData());
    }

    @SuppressWarnings("unchecked")
    public static <T extends Metadata.ProjectCustom> T fromXContent(Function<String, T> supplier, XContentParser parser)
        throws IOException {
        XContentParser.Token token;
        String data = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if ("data".equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                        throw new ElasticsearchParseException("failed to parse snapshottable metadata, invalid data type");
                    }
                    data = parser.text();
                } else {
                    throw new ElasticsearchParseException("failed to parse snapshottable metadata, unknown field [{}]", currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("failed to parse snapshottable metadata");
            }
        }
        if (data == null) {
            throw new ElasticsearchParseException("failed to parse snapshottable metadata, data not found");
        }
        return supplier.apply(data);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.single((builder, params) -> builder.field("data", getData()));
    }

    @Override
    public String toString() {
        return "[" + getWriteableName() + "][" + data + "]";
    }
}
