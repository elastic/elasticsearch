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

import java.io.IOException;
import java.util.Objects;

public class EntityGroup implements ToXContentObject, Writeable {

    private static final ParseField LABEL = new ParseField("label");
    private static final ParseField SCORE = new ParseField("score");
    private static final ParseField WORD = new ParseField("word");

    private final NerProcessor.Entity label;
    private final double score;
    private final String word;

    public EntityGroup(NerProcessor.Entity label, double score, String word) {
        this.label = label;
        this.score = score;
        this.word = Objects.requireNonNull(word);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(LABEL.getPreferredName(), label);
        builder.field(SCORE.getPreferredName(), score);
        builder.field(WORD.getPreferredName(), word);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        label.writeTo(out);
        out.writeDouble(score);
        out.writeString(word);
    }

    public NerProcessor.Entity getLabel() {
        return label;
    }

    public double getScore() {
        return score;
    }

    public String getWord() {
        return word;
    }
}
