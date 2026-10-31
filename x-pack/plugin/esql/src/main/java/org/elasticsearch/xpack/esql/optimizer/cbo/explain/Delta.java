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

/**
 * How much worse an alternative's plan scored than the chosen plan, so a reader can see by how far each alternative lost.
 * A negative delta means the alternative would have been cheaper, which is expected only for alternatives rejected by a fit check.
 */
public record Delta(double score, double percent) implements Writeable, ToXContentObject {

    public Delta(StreamInput in) throws IOException {
        this(in.readDouble(), in.readDouble());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(score);
        out.writeDouble(percent);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        ExplainXContent.numberField(b, "score", score);
        ExplainXContent.numberField(b, "percent", Double.isFinite(percent) ? Math.round(percent * 100) / 100.0 : percent);
        return b.endObject();
    }
}
