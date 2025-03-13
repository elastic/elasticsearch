/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.results;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * The top level object capturing output from the pytorch process.
 */
public record PyTorchResult(
    String requestId,
    Boolean isCacheHit,
    Long timeMs,
    @Nullable PyTorchInferenceResult inferenceResult,
    @Nullable ThreadSettings threadSettings,
    @Nullable AckResult ackResult,
    @Nullable ErrorResult errorResult
) implements ToXContentObject {

    private static final ParseField REQUEST_ID = new ParseField("request_id");
    private static final ParseField CACHE_HIT = new ParseField("cache_hit");
    private static final ParseField TIME_MS = new ParseField("time_ms");

    private static final ParseField RESULT = new ParseField("result");
    private static final ParseField THREAD_SETTINGS = new ParseField("thread_settings");
    private static final ParseField ACK = new ParseField("ack");

    public static final ConstructingObjectParser<PyTorchResult, Void> PARSER = new ConstructingObjectParser<>(
        "pytorch_result",
        a -> new PyTorchResult(
            (String) a[0],
            (Boolean) a[1],
            (Long) a[2],
            (PyTorchInferenceResult) a[3],
            (ThreadSettings) a[4],
            (AckResult) a[5],
            (ErrorResult) a[6]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REQUEST_ID);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), CACHE_HIT);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TIME_MS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), PyTorchInferenceResult.PARSER, RESULT);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ThreadSettings.PARSER, THREAD_SETTINGS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), AckResult.PARSER, ACK);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ErrorResult.PARSER, ErrorResult.ERROR);
    }

    public boolean isError() {
        return errorResult != null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (requestId != null) {
            builder.field(REQUEST_ID.getPreferredName(), requestId);
        }
        if (isCacheHit != null) {
            builder.field(CACHE_HIT.getPreferredName(), isCacheHit);
        }
        if (timeMs != null) {
            builder.field(TIME_MS.getPreferredName(), timeMs);
        }
        if (inferenceResult != null) {
            builder.field(RESULT.getPreferredName(), inferenceResult);
        }
        if (threadSettings != null) {
            builder.field(THREAD_SETTINGS.getPreferredName(), threadSettings);
        }
        if (ackResult != null) {
            builder.field(ACK.getPreferredName(), ackResult);
        }
        if (errorResult != null) {
            builder.field(ErrorResult.ERROR.getPreferredName(), errorResult);
        }

        builder.endObject();
        return builder;
    }
}
