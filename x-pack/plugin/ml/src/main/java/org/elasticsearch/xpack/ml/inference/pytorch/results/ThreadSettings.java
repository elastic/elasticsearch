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
import java.util.Objects;

public record ThreadSettings(int inferenceThreads, int modelThreads) implements ToXContentObject {

    private static final ParseField INFERENCE_THREADS = new ParseField("inference_threads");
    private static final ParseField MODEL_THREADS = new ParseField("model_threads");

    public static ConstructingObjectParser<ThreadSettings, Void> PARSER = new ConstructingObjectParser<>(
        "thread_settings",
        a -> new ThreadSettings((int) a[0], (int) a[1])
    );

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), INFERENCE_THREADS);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MODEL_THREADS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INFERENCE_THREADS.getPreferredName(), inferenceThreads);
        builder.field(MODEL_THREADS.getPreferredName(), modelThreads);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ThreadSettings that = (ThreadSettings) o;
        return inferenceThreads == that.inferenceThreads && modelThreads == that.modelThreads;
    }

    @Override
    public int hashCode() {
        return Objects.hash(inferenceThreads, modelThreads);
    }
}
