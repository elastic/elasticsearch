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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * One term of the cost attributed to a decision, so a reader can see which operator or exchange made an alternative expensive.
 * Components of a decision combine with {@link CostVector#plus} into its attributed cost.
 */
public record CostComponent(CostTerm term, @Nullable Side side, CostVector cost) implements Writeable, ToXContentObject {

    public CostComponent {
        Objects.requireNonNull(term, "term");
        Objects.requireNonNull(cost, "cost");
    }

    public CostComponent(StreamInput in) throws IOException {
        this(in.readEnum(CostTerm.class), in.readOptionalEnum(Side.class), new CostVector(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(term);
        out.writeOptionalEnum(side);
        cost.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("term", term.displayName());
        if (side != null) {
            b.field("side", side.displayName());
        }
        cost.toXContentObject(b, "cost");
        return b.endObject();
    }
}
