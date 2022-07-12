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
import java.util.Optional;

public class ErrorResult implements ToXContentObject {

    public static final ParseField ERROR = new ParseField("error");

    public static ConstructingObjectParser<ErrorResult, Void> PARSER = new ConstructingObjectParser<>(
        "error",
        a -> new ErrorResult((String) a[0], (String) a[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PyTorchResult.REQUEST_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ERROR);
    }

    private final String requestId;
    private final String error;

    ErrorResult(String requestId, String error) {
        this.requestId = requestId;
        this.error = error;
    }

    public ErrorResult(String error) {
        this.requestId = null;
        this.error = error;
    }

    public String error() {
        return error;
    }

    Optional<String> requestId() {
        return Optional.ofNullable(requestId);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorResult that = (ErrorResult) o;
        return Objects.equals(requestId, that.requestId) && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, error);
    }
}
