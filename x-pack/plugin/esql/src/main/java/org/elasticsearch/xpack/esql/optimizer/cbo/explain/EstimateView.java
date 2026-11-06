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
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Estimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Provenance;

import java.io.IOException;
import java.util.Objects;

/**
 * The reportable part of a statistics {@link Estimate}, so a reader can judge how much a decision's inputs can be trusted.
 * The spread is left out because the upper bound and provenance already convey the uncertainty.
 */
public record EstimateView(double value, double upperBound, Provenance provenance) implements Writeable, ToXContentObject {

    public EstimateView {
        Objects.requireNonNull(provenance, "provenance");
    }

    public EstimateView(StreamInput in) throws IOException {
        this(in.readDouble(), in.readDouble(), in.readEnum(Provenance.class));
    }

    public static EstimateView of(Estimate e) {
        return new EstimateView(e.value(), e.upperBound(), e.provenance());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(value);
        out.writeDouble(upperBound);
        out.writeEnum(provenance);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        ExplainXContent.numberField(b, "value", value);
        ExplainXContent.numberField(b, "upper_bound", upperBound);
        b.field("provenance", ExplainXContent.lowercase(provenance));
        return b.endObject();
    }
}
