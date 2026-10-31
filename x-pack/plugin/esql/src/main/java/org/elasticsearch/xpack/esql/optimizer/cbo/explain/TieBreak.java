/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Why an alternative whose score tied with the chosen plan still lost, so ties are not mistaken for arbitrary choices.
 */
public record TieBreak(TieBreakRule rule, double epsilonPercent) implements Writeable, ToXContentObject {

    public TieBreak {
        Objects.requireNonNull(rule, "rule");
    }

    public TieBreak(StreamInput in) throws IOException {
        this(in.readEnum(TieBreakRule.class), in.readDouble());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(rule);
        out.writeDouble(epsilonPercent);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("rule", rule.displayName());
        ExplainXContent.numberField(b, "epsilon_percent", epsilonPercent);
        return b.endObject();
    }
}
