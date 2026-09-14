/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.Signature;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPostOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPreOptimizationValidation;

/**
 * ES|QL aggregate analogue of PromQL {@code bottomk(k, v)}: collects the {@code k} lowest values per group.
 * <p>
 * This is a thin surrogate over {@link Top} with a fixed {@code "ASC"} order. Unlike PromQL {@code bottomk},
 * which preserves full series identity as separate rows via {@code TopNBy}, this follows ES|QL aggregate
 * output shape: one row per group carrying a multivalue array. The row-preserving ES|QL equivalent remains
 * {@code SORT value ASC | LIMIT k BY group}.
 */
public class BottomK extends AggregateFunction implements SurrogateExpression, PostOptimizationVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "BottomK", BottomK::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(BottomK.class).binary(BottomK::new).name("bottomk");

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = { "boolean", "double", "integer", "long", "date", "ip", "keyword" },
        signatures = { @Signature(params = { "boolean|ip|date|double|integer|long|STRING", "integer" }, returnType = "$0.noText") },
        briefSummary = "Collects the bottom values for a field, including repeated values.",
        description = "Collects the `k` lowest values for a field, including repeated values. "
            + "Shorthand for `TOP(field, k, \"ASC\")`, and the ES|QL aggregate analogue of PromQL `bottomk(k, v)`.",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "stats_topk", tag = "bottomk")
    )
    public BottomK(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text" },
            description = "The field to collect the bottom values for."
        ) Expression field,
        @Param(
            name = "k",
            type = { "integer" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "The maximum number of values to collect."
        ) Expression limit
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, limit);
    }

    public BottomK(Source source, Expression field, Expression filter, Expression window, Expression limit) {
        super(source, field, filter, window, List.of(limit));
    }

    private BottomK(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public BottomK withFilter(Expression filter) {
        return new BottomK(source(), field(), filter, window(), limitField());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression limitField() {
        return parameters().get(0);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        var typeResolution = isType(
            field(),
            dt -> dt == DataType.BOOLEAN
                || dt == DataType.DATETIME
                || dt == DataType.IP
                || DataType.isString(dt)
                || (dt.isNumeric() && dt != DataType.UNSIGNED_LONG),
            sourceText(),
            FIRST,
            "boolean",
            "date",
            "ip",
            "string",
            "numeric except unsigned_long or counter types"
        ).and(isNotNull(limitField(), sourceText(), SECOND))
            .and(isType(limitField(), dt -> dt == DataType.INTEGER, sourceText(), SECOND, "integer"));
        if (typeResolution.unresolved()) {
            return typeResolution;
        }
        return Foldables.resolveTypeLimit(limitField(), sourceText(), forPreOptimizationValidation(limitField()));
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        Foldables.resolveTypeLimit(limitField(), sourceText(), forPostOptimizationValidation(limitField(), failures));
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected NodeInfo<BottomK> info() {
        return NodeInfo.create(this, BottomK::new, field(), filter(), window(), limitField());
    }

    @Override
    public BottomK replaceChildren(List<Expression> newChildren) {
        return new BottomK(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public Expression surrogate() {
        var s = source();
        if (field().dataType() == DataType.NULL) {
            return new Literal(s, null, DataType.NULL);
        }
        return new Top(s, field(), filter(), window(), limitField(), Literal.keyword(s, "ASC"), null);
    }
}
