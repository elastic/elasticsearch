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
import org.elasticsearch.compute.aggregation.TopDoubleDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopDoubleFloatAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopDoubleIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopDoubleLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopFloatDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopFloatFloatAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopFloatIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopFloatLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopIntDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopIntFloatAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopIntIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopIntLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopIpAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopLongDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopLongFloatAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopLongIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.TopLongLongAggregatorFunctionSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
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
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPostOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPreOptimizationValidation;

public class Top extends AggregateFunction
    implements
        TwoOptionalArguments,
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
        ) Expression order,
        @Param(
            optional = true,
            name = "outputField",
            type = { "double", "integer", "long", "date" },
            description = "The extra field that, if present, will be the output of the TOP call instead of `field`."
                + "{applies_to}`stack: ga 9.3`"
        ) Expression outputField
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, limit, order == null ? Literal.keyword(source, ORDER_ASC) : order, outputField);
    }

    public Top(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        Expression limit,
        Expression order,
        @Nullable Expression outputField
    ) {
        super(source, field, filter, window, outputField != null ? asList(limit, order, outputField) : asList(limit, order));
    }

    private Top(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Top withFilter(Expression filter) {
        return new Top(source(), field(), filter, window(), limitField(), orderField(), outputField());
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

    @Nullable
    Expression outputField() {
        return parameters().size() > 2 ? parameters().get(2) : null;
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
        if (outputField() != null) {
            typeResolution = typeResolution.and(
                isType(
                    outputField(),
                    dt -> dt == DataType.DATETIME || (dt.isNumeric() && dt != DataType.UNSIGNED_LONG),
                    sourceText(),
                    FOURTH,
                    "date",
                    "numeric except unsigned_long or counter types"
                )
            )
                .and(
                    isType(
                        field(),
                        dt -> dt == DataType.DATETIME || (dt.isNumeric() && dt != DataType.UNSIGNED_LONG),
                        "when fourth argument is set, ",
                        sourceText(),
                        FIRST,
                        false,
                        "date",
                        "numeric except unsigned_long or counter types"
                    )
                );
        }

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
        return outputField() == null ? field().dataType().noText() : outputField().dataType().noText();
    }

    @Override
    protected NodeInfo<Top> info() {
        return NodeInfo.create(this, Top::new, field(), filter(), window(), limitField(), orderField(), outputField());
    }

    @Override
    public Top replaceChildren(List<Expression> newChildren) {
        return new Top(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.get(4),
            newChildren.size() > 5 ? newChildren.get(5) : null
        );
    }

    private static final Map<DataType, BiFunction<Integer, Boolean, AggregatorFunctionSupplier>> SUPPLIERS = Map.ofEntries(
        Map.entry(DataType.LONG, TopLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATETIME, TopLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.INTEGER, TopIntAggregatorFunctionSupplier::new),
        Map.entry(DataType.DOUBLE, TopDoubleAggregatorFunctionSupplier::new),
        Map.entry(DataType.BOOLEAN, TopBooleanAggregatorFunctionSupplier::new),
        Map.entry(DataType.IP, TopIpAggregatorFunctionSupplier::new),
        Map.entry(DataType.KEYWORD, TopBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.TEXT, TopBytesRefAggregatorFunctionSupplier::new)
    );

    private static final Map<Tuple<DataType, DataType>, BiFunction<Integer, Boolean, AggregatorFunctionSupplier>> SUPPLIERS_WITH_EXTRA = Map
        .ofEntries(
            Map.entry(Tuple.tuple(DataType.LONG, DataType.DATETIME), TopLongLongAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.LONG, DataType.INTEGER), TopLongIntAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.LONG, DataType.LONG), TopLongLongAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.LONG, DataType.FLOAT), TopLongFloatAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.LONG, DataType.DOUBLE), TopLongDoubleAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.DATETIME, DataType.DATETIME), TopLongLongAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.DATETIME, DataType.INTEGER), TopLongIntAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.DATETIME, DataType.LONG), TopLongLongAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.DATETIME, DataType.FLOAT), TopLongFloatAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.DATETIME, DataType.DOUBLE), TopLongDoubleAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.INTEGER, DataType.DATETIME), TopIntLongAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.INTEGER, DataType.INTEGER), TopIntIntAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.INTEGER, DataType.LONG), TopIntLongAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.INTEGER, DataType.FLOAT), TopIntFloatAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.INTEGER, DataType.DOUBLE), TopIntDoubleAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.FLOAT, DataType.DATETIME), TopFloatLongAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.FLOAT, DataType.INTEGER), TopFloatIntAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.FLOAT, DataType.LONG), TopFloatLongAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.FLOAT, DataType.FLOAT), TopFloatFloatAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.FLOAT, DataType.DOUBLE), TopFloatDoubleAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.DOUBLE, DataType.DATETIME), TopDoubleLongAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.DOUBLE, DataType.INTEGER), TopDoubleIntAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.DOUBLE, DataType.LONG), TopDoubleLongAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.DOUBLE, DataType.FLOAT), TopDoubleFloatAggregatorFunctionSupplier::new),
            Map.entry(Tuple.tuple(DataType.DOUBLE, DataType.DOUBLE), TopDoubleDoubleAggregatorFunctionSupplier::new)
        );

    @Override
    public AggregatorFunctionSupplier supplier() {
        DataType fieldType = field().dataType();
        BiFunction<Integer, Boolean, AggregatorFunctionSupplier> supplierCtor;
        if (outputField() == null) {
            supplierCtor = SUPPLIERS.get(fieldType);
            if (supplierCtor == null) {
                throw EsqlIllegalArgumentException.illegalDataType(fieldType);
            }
        } else {
            DataType outputFieldType = outputField().dataType();
            supplierCtor = SUPPLIERS_WITH_EXTRA.get(Tuple.tuple(fieldType, outputFieldType));
            if (supplierCtor == null) {
                throw EsqlIllegalArgumentException.illegalDataTypeCombination(fieldType, outputFieldType);
            }
        }
        return supplierCtor.apply(limitValue(), orderValue());
    }

    @Override
    public Expression surrogate() {
        var s = source();
        // If the `outputField` is specified but its value is the same as `field` then we do not need to handle `outputField` separately.
        if (outputField() != null && field().semanticEquals(outputField())) {
            return new Top(s, field(), limitField(), orderField(), null);
        }
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
