/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.planner.PlannerUtils.hasLimitedInput;

/**
 * Logical plan for the MMR command.
 * MMR performs result diversification on incoming results using maximum marginal relevance.
 * The input is a set of limited rows, where at least one field is a dense vector to use for vector comparison.
 * The output is a reduced set of results, in the same order as the input, but "diversified" to be results that are semantically
 * diverse from each other within the input set.
 */
public class MMR extends UnaryPlan
    implements
        TelemetryAware,
        ExecutesOn.Coordinator,
        PostAnalysisVerificationAware,
        PostOptimizationPlanVerificationAware {

    public static final String LAMBDA_OPTION_NAME = "lambda";
    public static final float DEFAULT_LAMBDA = 0.5f;

    private final Attribute diversifyField;
    private final Expression limit;
    private final Expression queryVector;
    private final MapExpression options;

    public MMR(
        Source source,
        LogicalPlan child,
        Attribute diversifyField,
        Expression limit,
        @Nullable Expression queryVector,
        @Nullable MapExpression options
    ) {
        super(source, child);
        this.diversifyField = diversifyField;
        this.limit = limit;
        this.queryVector = queryVector;
        this.options = options;
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

    public MapExpression options() {
        return options;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new MMR(source(), newChild, diversifyField, limit, queryVector, options);
    }

    @Override
    public boolean expressionsResolved() {
        return diversifyField.resolved();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, MMR::new, child(), diversifyField, limit, queryVector, options);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        MMR mmr = (MMR) o;
        return Objects.equals(this.child(), mmr.child())
            && Objects.equals(this.diversifyField, mmr.diversifyField)
            && Objects.equals(this.limit, mmr.limit)
            && Objects.equals(this.queryVector, mmr.queryVector)
            && Objects.equals(this.options, mmr.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), diversifyField, limit, queryVector, options);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (false == hasLimitedInput(this)) {
            failures.add(fail(this, "MMR can only be used on a limited number of rows. Consider adding a LIMIT before MMR."));
        }

        if (diversifyField.dataType() != DataType.DENSE_VECTOR) {
            failures.add(fail(this, "MMR diversify field must be a dense vector field"));
        }

        if (queryVector != null && queryVector.dataType() != DENSE_VECTOR) {
            failures.add(
                fail(
                    this,
                    "MMR query vector must be a DENSE_VECTOR, found [{}] of type [{}]",
                    queryVector.source().text(),
                    queryVector.dataType().name()
                )
            );
        }
        // ensure the limit is present and a positive integer
        postAnalysisLimitVerification(failures);
        // ensure lambda, if given, is between 0.0 and 1.0
        postAnalysisOptionsVerification(failures);
    }

    private void postAnalysisOptionsVerification(Failures failures) {
        if (options == null) {
            return;
        }
        options.keyFoldedMap().forEach((key, value) -> {
            if (key.equals(LAMBDA_OPTION_NAME)) {
                if ((value instanceof Literal) == false) {
                    failures.add(fail(this, "expected " + key + " to be a literal, got [" + value.sourceText() + "]"));
                }
                if (value.dataType().isNumeric() == false) {
                    failures.add(fail(this, "expected " + key + " to be numeric, got [" + value.sourceText() + "]"));
                    return;
                }
                Number numericValue = (Number) value.fold(FoldContext.small());
                if (numericValue != null && (numericValue.floatValue() < 0.0f || numericValue.floatValue() > 1.0f)) {
                    failures.add(fail(this, "MMR lambda value must be a number between 0.0 and 1.0, got [" + value.sourceText() + "]"));
                }
            } else {
                failures.add(fail(this, "Invalid option [" + key + "] in [" + this.sourceText() + "]"));
            }
        });
    }

    private void postAnalysisLimitVerification(Failures failures) {
        if (limit instanceof Literal == false) {
            failures.add(fail(this, "MMR limit is not a constant literal, got [" + limit.sourceText() + "]"));
            return;
        }

        Literal limitLiteral = (Literal) limit;
        if (limitLiteral.dataType() != INTEGER) {
            failures.add(fail(this, "MMR limit is not an integer, got [" + limitLiteral.sourceText() + "]"));
            return;
        }

        int limitValue = ((Integer) limitLiteral.value()).intValue();

        if (limitValue <= 0) {
            failures.add(fail(this, "MMR limit must be a positive integer, got [" + limitValue + "]"));
        }
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postOptimizationPlanVerification() {
        return (plan, failures) -> {
            if (plan instanceof MMR mmr) {
                Expression queryVector = mmr.queryVector();
                if (queryVector != null && false == queryVector instanceof Literal) {
                    failures.add(fail(mmr, "MMR query vector must be a constant, found [{}]", queryVector.source().text()));
                }
            }
        };
    }
}
