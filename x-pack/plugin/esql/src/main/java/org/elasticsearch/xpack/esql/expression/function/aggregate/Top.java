/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopIpAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPostOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPreOptimizationValidation;

public class Top extends AggregateFunction
    implements
        OptionalArgument,
        ToAggregator,
        SurrogateExpression,
        PostOptimizationVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Top", Top::new);

    private static final String ORDER_ASC = "ASC";
    private static final String ORDER_DESC = "DESC";

    @FunctionInfo(
        returnType = { "boolean", "double", "integer", "long", "date", "ip", "keyword" },
        description = "Collects the top values for a field. Includes repeated values.",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "stats_top", tag = "top")
    )
    public Top(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text" },
            description = "The field to collect the top values for."
        ) Expression field,
        @Param(name = "limit", type = { "integer" }, description = "The maximum number of values to collect.") Expression limit,
        @Param(
            optional = true,
            name = "order",
            type = { "keyword" },
            description = "The order to calculate the top values. Either `asc` or `desc`, and defaults to `asc` if omitted."
        ) Expression order
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, limit, order == null ? Literal.keyword(source, ORDER_ASC) : order);
    }

    public Top(Source source, Expression field, Expression filter, Expression window, Expression limit, Expression order) {
        super(source, field, filter, window, asList(limit, order));
    }

    private Top(StreamInput in) throws IOException {
        super(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public Top withFilter(Expression filter) {
        return new Top(source(), field(), filter, window(), limitField(), orderField());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression limitField() {
        return parameters().get(0);
    }

    Expression orderField() {
        return parameters().get(1);
    }

    private Integer limitValue() {
        return Foldables.limitValue(limitField(), sourceText());
    }

    private boolean orderValue() {
        if (orderField() instanceof Literal literal) {
            String order = BytesRefs.toString(literal.value());
            if (ORDER_ASC.equalsIgnoreCase(order) || ORDER_DESC.equalsIgnoreCase(order)) {
                return order.equalsIgnoreCase(ORDER_ASC);
            }
        }
        throw new EsqlIllegalArgumentException("Order value must be a literal, found: " + orderField());
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
            .and(isType(limitField(), dt -> dt == DataType.INTEGER, sourceText(), SECOND, "integer"))
            .and(isNotNull(orderField(), sourceText(), THIRD))
            .and(isString(orderField(), sourceText(), THIRD));

        if (typeResolution.unresolved()) {
            return typeResolution;
        }

        TypeResolution result = resolveTypeLimit();
        if (result.equals(TypeResolution.TYPE_RESOLVED) == false) {
            return result;
        }
        result = resolveTypeOrder(forPreOptimizationValidation(orderField()));
        if (result.equals(TypeResolution.TYPE_RESOLVED) == false) {
            return result;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * We check that the limit is not null and that if it is a literal, it is a positive integer
     * During postOptimizationVerification folding is already done, so we also verify that it is definitively a literal
     */
    private TypeResolution resolveTypeLimit() {
        return Foldables.resolveTypeLimit(limitField(), sourceText(), forPreOptimizationValidation(limitField()));
    }

    /**
     * We check that the order is not null and that if it is a literal, it is one of the two valid values: "asc" or "desc".
     * During postOptimizationVerification folding is already done, so we also verify that it is definitively a literal
     */
    private Expression.TypeResolution resolveTypeOrder(TypeResolutionValidator validator) {
        Expression order = orderField();
        if (order == null) {
            validator.invalid(new TypeResolution(format(null, "Order must be a valid string in [{}], found [{}]", sourceText(), order)));
        } else if (order instanceof Literal literal) {
            if (literal.value() == null) {
                validator.invalid(
                    new TypeResolution(
                        format(
                            null,
                            "Invalid order value in [{}], expected [{}, {}] but got [{}]",
                            sourceText(),
                            ORDER_ASC,
                            ORDER_DESC,
                            order
                        )
                    )
                );
            } else {
                String value = BytesRefs.toString(literal.value());
                if (value == null || value.equalsIgnoreCase(ORDER_ASC) == false && value.equalsIgnoreCase(ORDER_DESC) == false) {
                    validator.invalid(
                        new TypeResolution(
                            format(
                                null,
                                "Invalid order value in [{}], expected [{}, {}] but got [{}]",
                                sourceText(),
                                ORDER_ASC,
                                ORDER_DESC,
                                order
                            )
                        )
                    );
                }
            }
        } else {
            // it is expected that the expression is a literal after folding
            // we fail if it is not a literal
            validator.invalidIfPostValidation(fail(order, "Order must be a valid string in [{}], found [{}]", sourceText(), order));
        }
        return validator.getResolvedType();
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        postOptimizationVerificationLimit(failures);
        postOptimizationVerificationOrder(failures);
    }

    private void postOptimizationVerificationLimit(Failures failures) {
        Foldables.resolveTypeLimit(limitField(), sourceText(), forPostOptimizationValidation(limitField(), failures));
    }

    private void postOptimizationVerificationOrder(Failures failures) {
        resolveTypeOrder(forPostOptimizationValidation(orderField(), failures));
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected NodeInfo<Top> info() {
        return NodeInfo.create(this, Top::new, field(), filter(), window(), limitField(), orderField());
    }

    @Override
    public Top replaceChildren(List<Expression> newChildren) {
        return new Top(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3), newChildren.get(4));
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (type == DataType.LONG || type == DataType.DATETIME) {
            return new TopLongAggregatorFunctionSupplier(limitValue(), orderValue());
        }
        if (type == DataType.INTEGER) {
            return new TopIntAggregatorFunctionSupplier(limitValue(), orderValue());
        }
        if (type == DataType.DOUBLE) {
            return new TopDoubleAggregatorFunctionSupplier(limitValue(), orderValue());
        }
        if (type == DataType.BOOLEAN) {
            return new TopBooleanAggregatorFunctionSupplier(limitValue(), orderValue());
        }
        if (type == DataType.IP) {
            return new TopIpAggregatorFunctionSupplier(limitValue(), orderValue());
        }
        if (DataType.isString(type)) {
            return new TopBytesRefAggregatorFunctionSupplier(limitValue(), orderValue());
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    @Override
    public Expression surrogate() {
        var s = source();
        if (orderField() instanceof Literal && limitField() instanceof Literal && limitValue() == 1) {
            if (orderValue()) {
                return new Min(s, field(), filter(), window());
            } else {
                return new Max(s, field(), filter(), window());
            }
        }
        return null;
    }
}
