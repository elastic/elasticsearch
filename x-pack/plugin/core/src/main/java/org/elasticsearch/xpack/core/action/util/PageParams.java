/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.*;

import java.io.IOException;
import java.util.Objects;

/**
 * Helper class collecting options for pagination in a search
 */
public class PageParams implements ToXContentObject, Writeable {

    public static final ParseField PAGE = new ParseField("page");
    public static final ParseField FROM = new ParseField("from");
    public static final ParseField SIZE = new ParseField("size");

    public static final int DEFAULT_FROM = 0;
    public static final int DEFAULT_SIZE = 100;

    public static final ConstructingObjectParser<PageParams, Void> PARSER = new ConstructingObjectParser<>(
        PAGE.getPreferredName(),
        a -> new PageParams(a[0] == null ? DEFAULT_FROM : (int) a[0], a[1] == null ? DEFAULT_SIZE : (int) a[1])
    );

    static {
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), FROM);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), SIZE);
    }

    private final int from;
    private final int size;

    public static PageParams defaultParams() {
        return new PageParams(DEFAULT_FROM, DEFAULT_SIZE);
    }

    public PageParams(StreamInput in) throws IOException {
        this(in.readVInt(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeVInt(size);
    }

    public PageParams() {
        this.from = DEFAULT_FROM;
        this.size = DEFAULT_SIZE;
    }

    public PageParams(int from, int size) {
        if (from < 0) {
            throw new IllegalArgumentException("Parameter [" + FROM.getPreferredName() + "] cannot be < 0");
        }
        if (size < 0) {
            throw new IllegalArgumentException("Parameter [" + PageParams.SIZE.getPreferredName() + "] cannot be < 0");
        }
        this.from = from;
        this.size = size;
    }

    public int getFrom() {
        return from;
    }

    public int getSize() {
        return size;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FROM.getPreferredName(), from);
        builder.field(SIZE.getPreferredName(), size);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, size);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PageParams other = (PageParams) obj;
        return Objects.equals(from, other.from) && Objects.equals(size, other.size);
    }
        public static PageParams fromXContent( XContentParser contentParser) {
        return PARSER.apply(contentParser,null);
    }
}
