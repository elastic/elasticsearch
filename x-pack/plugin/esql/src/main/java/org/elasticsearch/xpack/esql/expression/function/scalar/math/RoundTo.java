/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Foldables;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.commonType;

/**
 * Round down to one of a list of values.
 */
public class RoundTo extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "RoundTo", RoundTo::new);

    private final Expression field;
    private final List<Expression> points;

    private DataType resultType;

    @FunctionInfo(
        returnType = { "double", "integer", "long", "date", "date_nanos" },
        description = """
            Rounds down to one of a list of fixed points.""",
        examples = @Example(file = "math", tag = "round_to"),
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.COMING, version = "8.19.0/9.1.0") }
    )
    public RoundTo(
        Source source,
        @Param(
            name = "field",
            type = { "double", "integer", "long", "date", "date_nanos" },
            description = "The numeric value to round. If `null`, the function returns `null`."
        ) Expression field,
        @Param(
            name = "points",
            type = { "double", "integer", "long", "date", "date_nanos" },
            description = "Remaining rounding points. Must be constants."
        ) List<Expression> points
    ) {
        super(source, Iterators.toList(Iterators.concat(Iterators.single(field), points.iterator())));
        this.field = field;
        this.points = points;
    }

    private RoundTo(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteableCollection(points);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        int index = 1;
        for (Expression f : points) {
            if (f.dataType() == DataType.NULL) {
                continue;
            }
            TypeResolution resolution = isFoldable(f, sourceText(), TypeResolutions.ParamOrdinal.fromIndex(index));
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        DataType dataType = dataType();
        if (dataType == null || SIGNATURES.containsKey(dataType) == false) {
            return new TypeResolution(format(null, "all arguments must be numeric, date, or data_nanos"));
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        if (resultType != null) {
            return resultType;
        }
        resultType = field.dataType();
        for (Expression f : points) {
            if (resultType == DataType.UNSIGNED_LONG || resultType == null || f.dataType() == UNSIGNED_LONG) {
                return null;
            }
            resultType = commonType(resultType, f.dataType());
        }
        return resultType;
    }

    @Override
    public boolean foldable() {
        for (Expression c : children()) {
            if (c.foldable() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new RoundTo(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RoundTo::new, field(), points());
    }

    public Expression field() {
        return field;
    }

    public List<Expression> points() {
        return points;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        DataType dataType = dataType();
        Build build = SIGNATURES.get(dataType);
        if (build == null) {
            throw new IllegalStateException("unsupported type");
        }

        ExpressionEvaluator.Factory field = toEvaluator.apply(field());
        field = Cast.cast(source(), field().dataType(), dataType, field);
        List<Object> points = Iterators.toList(Iterators.map(points().iterator(), p -> Foldables.valueOf(toEvaluator.foldCtx(), p)));
        return build.build(source(), field, points);
    }

    interface Build {
        ExpressionEvaluator.Factory build(Source source, ExpressionEvaluator.Factory field, List<Object> points);
    }

    private static final Map<DataType, Build> SIGNATURES = Map.ofEntries(
        Map.entry(DATETIME, RoundToLong.BUILD),
        Map.entry(DATE_NANOS, RoundToLong.BUILD),
        Map.entry(INTEGER, RoundToInt.BUILD),
        Map.entry(LONG, RoundToLong.BUILD),
        Map.entry(DOUBLE, RoundToDouble.BUILD)
    );
}
