/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.Rounding.RoundingConvention.DOWN;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.ROUND_TO_BLOCK_LOADER;
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
public class RoundTo extends EsqlScalarFunction implements BlockLoaderExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "RoundTo", RoundTo::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(RoundTo.class).unaryVariadic(RoundTo::new).name("round_to");
    public static final TransportVersion ESQL_ROUND_TO_CONVENTION = TransportVersion.fromName("esql_round_to_convention");

    private final Expression field;
    private final List<Expression> points;
    private final Rounding.RoundingConvention convention;

    private DataType resultType;

    @FunctionInfo(
        returnType = { "double", "integer", "long", "date", "date_nanos" },
        briefSummary = "Rounds down to one of a list of fixed points.",
        description = """
            Rounds down to one of a list of fixed points.""",
        examples = @Example(file = "math", tag = "round_to"),
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.1.0") }
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
        this(source, field, points, null);
    }

    public RoundTo(Source source, Expression field, List<Expression> points, Rounding.RoundingConvention convention) {
        super(source, Iterators.toList(Iterators.concat(Iterators.single(field), points.iterator())));
        this.field = field;
        this.points = points;
        this.convention = (convention != null) ? convention : DOWN;
    }

    private RoundTo(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.getTransportVersion().supports(ESQL_ROUND_TO_CONVENTION) ? in.readEnum(Rounding.RoundingConvention.class) : null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteableCollection(points);
        TransportVersion transportVersion = out.getTransportVersion();
        if (transportVersion.supports(ESQL_ROUND_TO_CONVENTION)) {
            out.writeEnum(convention);
        } else if (convention != DOWN) {
            throw new EsqlIllegalArgumentException(
                "rounding convention is not supported in peer node version [{}]. Upgrade to version [{}] or newer.",
                transportVersion,
                ESQL_ROUND_TO_CONVENTION
            );
        }
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
        if ((dataType == LONG || dataType == DATETIME || dataType == DATE_NANOS || dataType == INTEGER || dataType == DOUBLE) == false) {
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
        return new RoundTo(source(), newChildren.getFirst(), newChildren.subList(1, newChildren.size()), convention);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RoundTo::new, field(), points(), convention);
    }

    public Expression field() {
        return field;
    }

    public List<Expression> points() {
        return points;
    }

    public Rounding.RoundingConvention roundingConvention() {
        return convention;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        DataType dataType = dataType();
        ExpressionEvaluator.Factory fieldEval = Cast.cast(source(), field().dataType(), dataType, toEvaluator.apply(field()));
        if (dataType == LONG || dataType == DATETIME || dataType == DATE_NANOS) {
            long[] cc = new long[points.size()];
            for (int i = 0; i < points.size(); i++) {
                var p = (Number) Foldables.valueOf(toEvaluator.foldCtx(), points.get(i));
                if (p == null) {
                    return null;
                }
                cc[i] = DataTypeConverter.safeToLong(p);
            }
            Arrays.sort(cc);
            return RoundToLong.BUILD.build(source(), fieldEval, cc, convention);
        } else if (dataType == INTEGER) {
            int[] cc = new int[points.size()];
            for (int i = 0; i < points.size(); i++) {
                var p = (Number) Foldables.valueOf(toEvaluator.foldCtx(), points.get(i));
                if (p == null) {
                    return null;
                }
                cc[i] = p.intValue();
            }
            Arrays.sort(cc);
            return RoundToInt.BUILD.build(source(), fieldEval, cc, convention);
        } else if (dataType == DOUBLE) {
            double[] cc = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                var p = (Number) Foldables.valueOf(toEvaluator.foldCtx(), points.get(i));
                if (p == null) {
                    return null;
                }
                cc[i] = p.doubleValue();
            }
            Arrays.sort(cc);
            return RoundToDouble.BUILD.build(source(), fieldEval, cc, convention);
        }
        throw new IllegalStateException("unsupported type: " + dataType);
    }

    @Override
    public PushedBlockLoaderExpression tryPushToFieldLoading(SearchStats stats) {
        if (ROUND_TO_BLOCK_LOADER.isEnabled() == false) {
            return null;
        }
        if (field instanceof FieldAttribute f) {
            DataType dt = dataType();
            if (dt != LONG && dt != DATETIME && dt != DATE_NANOS) {
                return null;
            }
            long[] pts = new long[points.size()];
            for (int i = 0; i < points.size(); i++) {
                if (points.get(i) instanceof Literal lit) {
                    pts[i] = DataTypeConverter.safeToLong((Number) lit.value());
                } else {
                    return null;
                }
            }
            Arrays.sort(pts);
            return new PushedBlockLoaderExpression(f, new BlockLoaderFunctionConfig.RoundToLongs(pts));
        }
        return null;
    }
}
