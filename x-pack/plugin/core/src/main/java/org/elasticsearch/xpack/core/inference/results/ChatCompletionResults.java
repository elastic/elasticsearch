/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Writes a chat completion result in the following json format:
 * {
 *    "completion": [
 *      {
 *          "result": "some result 1"
 *      },
 *      {
 *          "result": "some result 2"
 *      }
 *    ]
 * }
 *
 */
public record ChatCompletionResults(List<Result> results) implements InferenceServiceResults {

    public static final String NAME = "chat_completion_service_results";
    public static final String COMPLETION = TaskType.COMPLETION.name().toLowerCase(Locale.ROOT);

    public ChatCompletionResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(Result::new));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(COMPLETION);
        for (Result result : results) {
            result.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(results);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        throw new UnsupportedOperationException();
    }

    public List<Result> getResults() {
        return results;
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(COMPLETION, results.stream().map(Result::asMap).collect(Collectors.toList()));

        return map;
    }

    public record Result(String content) implements Writeable, ToXContentObject {

        public static final String RESULT = "result";

        public Result(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(content);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RESULT, content);
            builder.endObject();

            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        public Map<String, Object> asMap() {
            return Map.of(RESULT, content);
        }
    }

}
