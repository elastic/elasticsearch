/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.results;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record InferenceProcessStats(long memoryRss) implements ToXContentObject {

    private static final ParseField MEMORY_RSS = new ParseField("memory_rss");

    public static final ConstructingObjectParser<InferenceProcessStats, Void> PARSER = new ConstructingObjectParser<>(
        "inference_process_stats",
        a -> new InferenceProcessStats((long) a[0])
    );

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MEMORY_RSS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MEMORY_RSS.getPreferredName(), memoryRss);
        builder.endObject();
        return builder;
    }
}
