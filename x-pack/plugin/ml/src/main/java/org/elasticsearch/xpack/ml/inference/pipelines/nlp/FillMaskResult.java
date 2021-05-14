/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FillMaskResult implements InferenceResults {

    public static class Result implements ToXContentObject, Writeable {

        private static final ParseField TOKEN = new ParseField("token");
        private static final ParseField SCORE = new ParseField("score");
        private static final ParseField SEQUENCE = new ParseField("sequence");

        private final String token;
        private final double score;
        private final String sequence;

        public Result(String token, double score, String sequence) {
            this.token = Objects.requireNonNull(token);
            this.score = score;
            this.sequence = Objects.requireNonNull(sequence);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TOKEN.getPreferredName(), token);
            builder.field(SCORE.getPreferredName(), score);
            builder.field(SEQUENCE.getPreferredName(), sequence);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(token);
            out.writeDouble(score);
            out.writeString(sequence);
        }
    }

    private static final String NAME = "fill_mask_result";

    private final List<Result> results;

    public FillMaskResult(List<Result> results) {
        this.results = results;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (Result result : results) {
            result.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(results);
    }

    @Override
    public Map<String, Object> asMap() {
        return null;
    }

    @Override
    public Object predictedValue() {
        return null;
    }
}
