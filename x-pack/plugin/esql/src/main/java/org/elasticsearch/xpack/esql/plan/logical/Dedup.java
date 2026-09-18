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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Removes duplicate rows from the result set (similar in spirit to SQL {@code DISTINCT}).
 * <p>
 * Implemented as a {@link SurrogateLogicalPlan} that rewrites to
 * {@code LIMIT 1 BY <child.output()>} during the optimizer's substitutions phase.
 * The node itself does not need to escape the coordinator, so it is not registered
 * with the {@code NamedWriteableRegistry}.
 */
public class Dedup extends UnaryPlan implements SurrogateLogicalPlan, TelemetryAware, PostAnalysisVerificationAware {

    public Dedup(Source source, LogicalPlan child) {
        super(source, child);
    }

    @Override
    public Dedup replaceChild(LogicalPlan newChild) {
        return new Dedup(source(), newChild);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<Dedup> info() {
        return NodeInfo.create(this, Dedup::new, child());
    }

    @Override
    public String telemetryLabel() {
        return "DEDUP";
    }

    @Override
    public LogicalPlan surrogate() {
        Literal one = new Literal(source(), 1, DataType.INTEGER);
        List<Expression> groupings = new ArrayList<>();
        for (Attribute attr : child().output()) {
            if (attr.dataType() == DataType.DOC_DATA_TYPE || attr instanceof UnsupportedAttribute) {
                continue;
            }
            groupings.add(attr);
        }
        if (groupings.isEmpty()) {
            return new Limit(source(), one, child());
        }
        return new LimitBy(source(), one, child(), groupings);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        for (Attribute attr : child().output()) {
            Aggregate.checkUnsupportedGroupingType(attr, failures);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }
}
