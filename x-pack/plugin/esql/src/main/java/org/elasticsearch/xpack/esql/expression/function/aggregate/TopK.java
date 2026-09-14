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
 * ES|QL aggregate analogue of PromQL {@code topk(k, v)}: collects the {@code k} highest values per group.
 * <p>
 * This is a thin surrogate over {@link Top} with a fixed {@code "DESC"} order. Unlike PromQL {@code topk},
 * which preserves full series identity as separate rows via {@code TopNBy}, this follows ES|QL aggregate
 * output shape: one row per group carrying a multivalue array. The row-preserving ES|QL equivalent remains
 * {@code SORT value DESC | LIMIT k BY group}.
 */
public class TopK extends AggregateFunction implements SurrogateExpression, PostOptimizationVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "TopK", TopK::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TopK.class).binary(TopK::new).name("topk");

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = { "boolean", "double", "integer", "long", "date", "ip", "keyword" },
        signatures = { @Signature(params = { "boolean|ip|date|double|integer|long|STRING", "integer" }, returnType = "$0.noText") },
        briefSummary = "Collects the top values for a field, including repeated values.",
        description = "Collects the `k` highest values for a field, including repeated values. "
            + "Shorthand for `TOP(field, k, \"DESC\")`, and the ES|QL aggregate analogue of PromQL `topk(k, v)`.",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "stats_topk", tag = "topk")
    )
    public TopK(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text" },
            description = "The field to collect the top values for."
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

    public TopK(Source source, Expression field, Expression filter, Expression window, Expression limit) {
        super(source, field, filter, window, List.of(limit));
    }

    private TopK(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public TopK withFilter(Expression filter) {
        return new TopK(source(), field(), filter, window(), limitField());
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
    protected NodeInfo<TopK> info() {
        return NodeInfo.create(this, TopK::new, field(), filter(), window(), limitField());
    }

    @Override
    public TopK replaceChildren(List<Expression> newChildren) {
        return new TopK(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public Expression surrogate() {
        var s = source();
        if (field().dataType() == DataType.NULL) {
            return new Literal(s, null, DataType.NULL);
        }
        return new Top(s, field(), filter(), window(), limitField(), Literal.keyword(s, "DESC"), null);
    }
}
