/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nulls;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;

/**
 * Function returning the first non-null value. {@code COALESCE} runs as though
 * it were lazily evaluating each position in each incoming {@link Block}.
 */
public class Coalesce extends EsqlScalarFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Coalesce", Coalesce::new);

    private DataType dataType;

    @FunctionInfo(
        returnType = {
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date_nanos",
            "date",
            "geo_point",
            "geo_shape",
            "integer",
            "ip",
            "keyword",
            "long",
            "version" },
        description = "Returns the first of its arguments that is not null. If all arguments are null, it returns `null`.",
        examples = { @Example(file = "null", tag = "coalesce") }
    )
    public Coalesce(
        Source source,
        @Param(
            name = "first",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date_nanos",
                "date",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" },
            description = "Expression to evaluate."
        ) Expression first,
        @Param(
            name = "rest",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date_nanos",
                "date",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" },
            description = "Other expression to evaluate.",
            optional = true
        ) List<Expression> rest
    ) {
        super(source, Stream.concat(Stream.of(first), rest.stream()).toList());
    }

    private Coalesce(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeNamedWriteableCollection(children().subList(1, children().size()));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        for (int position = 0; position < children().size(); position++) {
            if (dataType == null || dataType == NULL) {
                dataType = children().get(position).dataType().noText();
                continue;
            }
            TypeResolution resolution = TypeResolutions.isType(
                children().get(position),
                t -> t.noText() == dataType,
                sourceText(),
                TypeResolutions.ParamOrdinal.fromIndex(position),
                dataType.typeName()
            );
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Nullability nullable() {
        // If any of the children aren’t nullable then this isn’t .
        for (Expression c : children()) {
            if (c.nullable() == Nullability.FALSE) {
                return Nullability.FALSE;
            }
        }
        /*
         * Otherwise let’s call this one "unknown". If we returned TRUE here
         * an optimizer rule would replace this with null if any of our children
         * fold to null. We don’t want that at all.
         */
        return Nullability.UNKNOWN;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Coalesce(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Coalesce::new, children().get(0), children().subList(1, children().size()));
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (dataType()) {
            case BOOLEAN -> CoalesceBooleanEvaluator.toEvaluator(toEvaluator, children());
            case DOUBLE, COUNTER_DOUBLE -> CoalesceDoubleEvaluator.toEvaluator(toEvaluator, children());
            case INTEGER, COUNTER_INTEGER -> CoalesceIntEvaluator.toEvaluator(toEvaluator, children());
            case LONG, DATE_NANOS, DATETIME, COUNTER_LONG, UNSIGNED_LONG -> CoalesceLongEvaluator.toEvaluator(toEvaluator, children());
            case KEYWORD, TEXT, CARTESIAN_POINT, CARTESIAN_SHAPE, GEO_POINT, GEO_SHAPE, IP, VERSION -> CoalesceBytesRefEvaluator
                .toEvaluator(toEvaluator, children());
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            case UNSUPPORTED, SHORT, BYTE, DATE_PERIOD, OBJECT, DOC_DATA_TYPE, SOURCE, TIME_DURATION, FLOAT, HALF_FLOAT, TSID_DATA_TYPE,
                SCALED_FLOAT, PARTIAL_AGG, AGGREGATE_METRIC_DOUBLE, DENSE_VECTOR -> throw new UnsupportedOperationException(
                    dataType() + " can’t be coalesced"
                );
        };
    }
}
