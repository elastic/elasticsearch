/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class Row extends LeafPlan implements PostAnalysisVerificationAware, TelemetryAware {

    private final List<Alias> fields;

    public Row(Source source, List<Alias> fields) {
        super(source);
        this.fields = fields;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public List<Alias> fields() {
        return fields;
    }

    @Override
    public List<Attribute> output() {
        return Expressions.asAttributes(fields);
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(fields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Row::new, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row constant = (Row) o;
        return Objects.equals(fields, constant.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        fields.forEach(a -> {
            if (DataType.isRepresentable(a.dataType()) == false) {
                failures.add(fail(a.child(), "cannot use [{}] directly in a row assignment", a.child().sourceText()));
            }
        });
    }
}
