/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class InnerChunkBuilder extends InnerHitBuilder {
    private static final ObjectParser<InnerChunkBuilder, Void> PARSER = new ObjectParser<>("inner_chunks", InnerChunkBuilder::new);

    static {
        PARSER.declareInt(InnerChunkBuilder::setFrom, SearchSourceBuilder.FROM_FIELD);
        PARSER.declareInt(InnerChunkBuilder::setSize, SearchSourceBuilder.SIZE_FIELD);
    }

    public InnerChunkBuilder() {
        super("chunks");
    }

    public InnerChunkBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public static InnerChunkBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new InnerChunkBuilder(), null);
    }
}
