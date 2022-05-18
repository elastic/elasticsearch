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

public record ErrorResult(String requestId, String error) implements ToXContentObject {

    public static final ParseField ERROR = new ParseField("error");

    public static ConstructingObjectParser<ErrorResult, Void> PARSER = new ConstructingObjectParser<>(
        "error",
        a -> new ErrorResult((String) a[0], (String) a[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PyTorchResult.REQUEST_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ERROR);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (requestId != null) {
            builder.field(PyTorchResult.REQUEST_ID.getPreferredName(), requestId);
        }
        builder.field(ERROR.getPreferredName(), error);
        builder.endObject();
        return builder;
    }
}
