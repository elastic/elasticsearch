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
import java.util.List;
import java.util.Objects;

/**
 * The alternative a decision picked and the part of the chosen plan's cost attributed to it.
 * It carries no plan cost because the chosen plan's total is reported once, on the enclosing {@link RunReport}.
 */
public record ChosenReport(String name, String delivered, List<CostComponent> components, List<FitCheck> fitChecks)
    implements
        Writeable,
        ToXContentObject {

    public ChosenReport {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(delivered, "delivered");
        components = components == null ? List.of() : List.copyOf(components);
        fitChecks = fitChecks == null ? List.of() : List.copyOf(fitChecks);
    }

    public ChosenReport(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readString(),
            in.readCollectionAsImmutableList(CostComponent::new),
            in.readCollectionAsImmutableList(FitCheck::new)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(delivered);
        out.writeCollection(components);
        out.writeCollection(fitChecks);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("name", name);
        b.field("delivered", delivered);
        ExplainXContent.optionalArray(b, "components", components, params);
        ExplainXContent.optionalArray(b, "fit_checks", fitChecks, params);
        return b.endObject();
    }
}
