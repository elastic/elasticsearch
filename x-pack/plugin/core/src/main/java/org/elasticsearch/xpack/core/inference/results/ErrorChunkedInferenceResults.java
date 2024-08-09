/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class ErrorChunkedInferenceResults implements ChunkedInferenceServiceResults {

    public static final String NAME = "error_chunked";

    private final Exception exception;

    public ErrorChunkedInferenceResults(Exception exception) {
        this.exception = Objects.requireNonNull(exception);
    }

    public ErrorChunkedInferenceResults(StreamInput in) throws IOException {
        this.exception = in.readException();
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeException(exception);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ErrorChunkedInferenceResults that = (ErrorChunkedInferenceResults) object;
        // Just compare the message for serialization test purposes
        return Objects.equals(exception.getMessage(), that.exception.getMessage());
    }

    @Override
    public int hashCode() {
        // Just compare the message for serialization test purposes
        return Objects.hash(exception.getMessage());
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return null;
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        return null;
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> asMap = new LinkedHashMap<>();
        asMap.put(NAME, exception.getMessage());
        return asMap;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.field(NAME, exception.getMessage());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Iterator<Chunk> chunksAsMatchedTextAndByteReference(XContent xcontent) {
        return Stream.of(exception).map(e -> new Chunk(e.getMessage(), BytesArray.EMPTY)).iterator();
    }
}
