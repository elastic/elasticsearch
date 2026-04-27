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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Removes duplicate rows from the result set, like SQL {@code DISTINCT}.
 * <p>
 * Implemented as a {@link SurrogateLogicalPlan} that rewrites to
 * {@code LIMIT 1 BY <child.output()>} during the optimizer's substitutions phase.
 * The node itself does not need to escape the coordinator, so it is not registered
 * with the {@code NamedWriteableRegistry}.
 */
public class Distinct extends UnaryPlan implements SurrogateLogicalPlan, TelemetryAware, PostAnalysisVerificationAware {

    /**
     * Data types that the runtime {@code GroupKeyEncoder} (used by the {@code GroupedLimitOperator}
     * that implements {@code LIMIT BY}) cannot encode as group keys. Because DISTINCT is
     * implemented as {@code LIMIT 1 BY <every column>}, having any of these in the schema would
     * fail with an opaque {@code IllegalArgumentException} at execution time. We surface a
     * post-analysis error instead.
     * <p>
     * Public so generative tests can consult the same list and avoid producing queries that are
     * guaranteed to fail.
     */
    public static final Set<DataType> UNSUPPORTED_GROUPING_TYPES = EnumSet.of(
        DataType.AGGREGATE_METRIC_DOUBLE,
        DataType.EXPONENTIAL_HISTOGRAM,
        DataType.TDIGEST,
        DataType.DATE_RANGE,
        DataType.PARTIAL_AGG
    );

    public Distinct(Source source, LogicalPlan child) {
        super(source, child);
    }

    @Override
    public Distinct replaceChild(LogicalPlan newChild) {
        return new Distinct(source(), newChild);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<Distinct> info() {
        return NodeInfo.create(this, Distinct::new, child());
    }

    @Override
    public String telemetryLabel() {
        return "DISTINCT";
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
            if (UNSUPPORTED_GROUPING_TYPES.contains(attr.dataType())) {
                failures.add(
                    fail(this, "DISTINCT does not support fields of type [{}], found field [{}]", attr.dataType().typeName(), attr.name())
                );
            }
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
