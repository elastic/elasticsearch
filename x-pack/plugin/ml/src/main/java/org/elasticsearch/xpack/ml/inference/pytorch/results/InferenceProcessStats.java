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

/**
 * Process-level statistics emitted by the native {@code pytorch_inference} process.
 *
 * @param memoryRss    the current resident set size in bytes
 * @param memoryMaxRss the peak (OS high-water mark) resident set size in bytes; {@code 0} when the native process did
 *                     not report it (e.g. an older ml-cpp that only sent {@code memory_rss})
 */
public record InferenceProcessStats(long memoryRss, long memoryMaxRss) implements ToXContentObject {

    private static final ParseField MEMORY_RSS = new ParseField("memory_rss");
    private static final ParseField MEMORY_MAX_RSS = new ParseField("memory_max_rss");

    public static final ConstructingObjectParser<InferenceProcessStats, Void> PARSER = new ConstructingObjectParser<>(
        "inference_process_stats",
        a -> new InferenceProcessStats((long) a[0], a[1] == null ? 0L : (long) a[1])
    );

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MEMORY_RSS);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MEMORY_MAX_RSS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MEMORY_RSS.getPreferredName(), memoryRss);
        builder.field(MEMORY_MAX_RSS.getPreferredName(), memoryMaxRss);
        builder.endObject();
        return builder;
    }
}
