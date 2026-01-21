/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;

public class MMR extends UnaryPlan implements TelemetryAware, ExecutesOn.Coordinator, PostAnalysisVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "MMR", MMR::new);
    public static final String LAMBDA_OPTION_NAME = "lambda";
    private static final List<String> VALID_MMR_OPTION_NAMES = List.of(LAMBDA_OPTION_NAME);

    private final Attribute diversifyField;
    private final Expression limit;
    private final Expression queryVector;
    private final Expression options;

    private Double lambdaValue;

    public MMR(
        Source source,
        LogicalPlan child,
        Attribute diversifyField,
        Expression limit,
        @Nullable Expression queryVector,
        @Nullable Expression options
    ) {
        super(source, child);
        this.diversifyField = diversifyField;
        this.limit = limit;
        this.queryVector = queryVector;
        this.options = options;
    }

    public MMR(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    public Attribute diversifyField() {
        return diversifyField;
    }

    public Expression limit() {
        return limit;
    }

    public Expression queryVector() {
        return queryVector;
    }

    public Expression options() {
        return options;
    }

    public Double lambdaValue() {
        return lambdaValue;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new MMR(source(), newChild, diversifyField, limit, queryVector, options);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, MMR::new, child(), diversifyField, limit, queryVector, options);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean expressionsResolved() {
        return diversifyField.resolved();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(this.diversifyField);
        out.writeNamedWriteable(limit);
        out.writeOptionalNamedWriteable(queryVector);
        out.writeOptionalNamedWriteable(options);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        MMR mmr = (MMR) o;
        return Objects.equals(this.diversifyField, mmr.diversifyField)
            && Objects.equals(this.limit, mmr.limit)
            && Objects.equals(this.queryVector, mmr.queryVector)
            && Objects.equals(this.options, mmr.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), diversifyField, limit, queryVector, options);
    }

    public List<String> validOptionNames() {
        return VALID_MMR_OPTION_NAMES;
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (diversifyField.dataType() != DataType.DENSE_VECTOR) {
            failures.add(fail(this, "MMR diversify field must be a dense vector field"));
        }

        // ensure LIMIT value is integer
        if (limit instanceof Literal litLimit && litLimit.dataType() == INTEGER) {
            int limitValue = (Integer) litLimit.value();
            if (limitValue < 1) {
                failures.add(fail(this, "MMR limit must be an positive integer"));
            }
        } else {
            failures.add(fail(this, "MMR limit must be an positive integer"));
        }

        // ensure query_vector, if given, is resolved to a DENSE_VECTOR type
        if (queryVector != null) {
            if (queryVector.resolved() && queryVector.dataType() != DataType.DENSE_VECTOR) {
                failures.add(fail(this, "MMR query vector must be resolved to a dense vector type"));
            }
        }

        // ensure lambda, if given, is between 0.0 and 1.0
        postAnalysisOptionsVerification(failures);
    }

    private void postAnalysisOptionsVerification(Failures failures) {
        if (options == null) {
            return;
        }

        if ((options instanceof MapExpression) == false) {
            failures.add(fail(this, "MMR options must be a map expression"));
            return;
        }

        Map<String, Expression> optionsMap = ((MapExpression) options).keyFoldedMap();

        Expression lambdaValueExpression = optionsMap.remove(MMR.LAMBDA_OPTION_NAME);
        if (lambdaValueExpression != null) {
            if (lambdaValueExpression instanceof Literal litLambdaValue) {
                this.lambdaValue = (Double) litLambdaValue.value();

                if (this.lambdaValue == null || this.lambdaValue < 0.0 || this.lambdaValue > 1.0) {
                    failures.add(fail(this, "MMR lambda value must be a number between 0.0 and 1.0"));
                }
            } else {
                failures.add(fail(this, "MMR lambda value must be a number between 0.0 and 1.0"));
            }
        }

        if (optionsMap.isEmpty() == false) {
            failures.add(
                fail(
                    this,
                    "Invalid option [{}] in <MMR>, expected one of [{}]",
                    optionsMap.keySet().stream().findAny().get(),
                    this.validOptionNames()
                )
            );
        }
    }
}
