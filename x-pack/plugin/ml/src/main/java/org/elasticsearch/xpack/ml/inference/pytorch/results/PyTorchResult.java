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
    @Nullable PyTorchInferenceResult inferenceResult,
    @Nullable ThreadSettings threadSettings,
    @Nullable ErrorResult errorResult
) implements ToXContentObject {

    static final ParseField REQUEST_ID = new ParseField("request_id");

    private static final ParseField RESULT = new ParseField("result");
    private static final ParseField THREAD_SETTINGS = new ParseField("thread_settings");

    public static ConstructingObjectParser<PyTorchResult, Void> PARSER = new ConstructingObjectParser<>(
        "pytorch_result",
        a -> new PyTorchResult((PyTorchInferenceResult) a[0], (ThreadSettings) a[1], (ErrorResult) a[2])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), PyTorchInferenceResult.PARSER, RESULT);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ThreadSettings.PARSER, THREAD_SETTINGS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ErrorResult.PARSER, ErrorResult.ERROR);
    }

    public boolean isError() {
        return errorResult != null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inferenceResult != null) {
            builder.field(RESULT.getPreferredName(), inferenceResult);
        }
        if (threadSettings != null) {
            builder.field(THREAD_SETTINGS.getPreferredName(), threadSettings);
        }
        if (errorResult != null) {
            builder.field(ErrorResult.ERROR.getPreferredName(), errorResult);
        }

        builder.endObject();
        return builder;
    }
}
