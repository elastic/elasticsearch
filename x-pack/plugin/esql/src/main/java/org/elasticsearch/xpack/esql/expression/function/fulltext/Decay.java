/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.script.ScoreScriptUtils;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.*;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.*;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialPoint;

/**
 * Decay a numeric, spatial or date type value based on the distance of it to an origin.
 *
 * This function uses the same {@link ScoreScriptUtils} implementations as Painless scripts,
 * ensuring consistent decay calculations across ES|QL and script contexts. The decay
 * functions support linear, exponential, and gaussian decay types for:
 * - Numeric types (int, long, double)
 * - Spatial types (geo_point, cartesian_point)
 * - Temporal types (datetime, date_nanos)
 */
public class Decay extends EsqlScalarFunction implements OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Decay", Decay::new);

    private static final String NUMERIC_DATE_OR_SPATIAL_POINT = "numeric, date or spatial point";

    private static final Double DEFAULT_NUMERIC_OFFSET = 0.0;
    private static final BytesRef DEFAULT_GEO_POINT_OFFSET = new BytesRef("0m");
    private static final Double DEFAULT_CARTESIAN_POINT_OFFSET = 0.0;
    private static final Long DEFAULT_TEMPORAL_OFFSET = 0L;
    private static final Double DEFAULT_DECAY = 0.5;
    private static final String DEFAULT_FUNCTION = "linear";

    private final Expression origin;
    private final Expression value;
    private final Expression scale;
    private final Expression offset;
    private final Expression decay;
    private final Expression type;

    @FunctionInfo(
        returnType = "double",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) },
        description = "Decay a numeric, spatial or date type value based on the distance of it to an origin.",
        examples = { @Example(file = "decay", tag = "decay") }
    )
    public Decay(
        Source source,
        @Param(
            name = "value",
            type = { "double", "integer", "long", "date", "date_nanos", "geo_point", "cartesian_point" },
            description = "Value to calculate decayed value for."
        ) Expression value,
        @Param(
            name = "origin",
            type = { "double", "integer", "long", "date", "date_nanos", "geo_point", "cartesian_point" },
            description = "Central point from which the distances are calculated."
        ) Expression origin,
        @Param(
            name = "scale",
            type = { "double", "integer", "long", "time_duration", "keyword", "text" },
            description = "Distance from the origin where the function returns the decay value."
        ) Expression scale,
        @Param(
            name = "offset",
            type = { "double", "integer", "long", "time_duration", "keyword", "text" },
            description = "Distance from the origin where no decay occurs.",
            optional = true
        ) Expression offset,
        @Param(
            name = "decay",
            type = { "double" },
            description = "Multiplier value returned at the scale distance from the origin.",
            optional = true
        ) Expression decay,
        @Param(
            name = "type",
            type = { "keyword" },
            description = "Function to use: linear, exponential or gaussian decay.",
            optional = true
        ) Expression type
    ) {
        super(source, Arrays.asList(value, origin, scale, offset, decay, type));
        this.value = value;
        this.origin = origin;
        this.scale = scale;
        this.offset = offset;
        this.decay = decay;
        this.type = type;
    }

    private Decay(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(value);
        out.writeNamedWriteable(origin);
        out.writeNamedWriteable(scale);
        out.writeOptionalNamedWriteable(offset);
        out.writeOptionalNamedWriteable(decay);
        out.writeOptionalNamedWriteable(type);
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

        TypeResolution valueResolution = isNotNull(value, sourceText(), FIRST).and(
            isType(value, dt -> dt.isNumeric() || dt.isDate() || isSpatialPoint(dt), sourceText(), FIRST, NUMERIC_DATE_OR_SPATIAL_POINT)
        );
        if (valueResolution.unresolved()) {
            return valueResolution;
        }

        DataType valueDataType = value.dataType();
        boolean isGeoPoint = isGeoPoint(valueDataType);
        boolean isCartesianPoint = isCartesianPoint(valueDataType);

        // Spatial decay
        if (isGeoPoint || isCartesianPoint) {
            TypeResolution originResolution = isNotNull(origin, sourceText(), SECOND).and(
                isType(origin, DataType::isSpatialPoint, sourceText(), SECOND, "spatial point")
            );
            if (originResolution.unresolved()) {
                return originResolution;
            }

            TypeResolution scaleResolution = isNotNull(scale, sourceText(), THIRD);
            if (scaleResolution.unresolved()) {
                return scaleResolution;
            }

            if (isGeoPoint) {
                // Geo points: scale should be a distance unit string (e.g. "100km")
                scaleResolution = isNotNull(scale, sourceText(), THIRD).and(
                    isType(scale, DataType::isString, sourceText(), THIRD, "keyword or text")
                );
            } else {
                // Cartesian points: scale should be numeric (e.g. 100.0)
                scaleResolution = isNotNull(scale, sourceText(), THIRD).and(
                    isType(scale, DataType::isNumeric, sourceText(), THIRD, "numeric")
                );
            }

            if (scaleResolution.unresolved()) {
                return scaleResolution;
            }
        }
        // Temporal decay
        else if (isMillisOrNanos(valueDataType)) {
            TypeResolution originResolution = isNotNull(origin, sourceText(), SECOND).and(
                isType(origin, DataType::isMillisOrNanos, sourceText(), SECOND, "datetime or date_nanos")
            );
            if (originResolution.unresolved()) {
                return originResolution;
            }

            TypeResolution scaleResolution = isNotNull(scale, sourceText(), THIRD).and(
                isType(scale, DataType::isTimeDuration, sourceText(), THIRD, "time_duration")
            );
            if (scaleResolution.unresolved()) {
                return scaleResolution;
            }
        }
        // Numeric decay
        else {
            TypeResolution originResolution = isNotNull(origin, sourceText(), SECOND).and(isNumeric(origin, sourceText(), SECOND));
            if (originResolution.unresolved()) {
                return originResolution;
            }

            TypeResolution scaleResolution = isNotNull(scale, sourceText(), THIRD).and(isNumeric(scale, sourceText(), THIRD));
            if (scaleResolution.unresolved()) {
                return scaleResolution;
            }
        }

        TypeResolutions.ParamOrdinal paramOrdinal = FOURTH;

        if (offset != null) {
            TypeResolution resolution = isType(
                offset,
                dt -> dt.isNumeric() || isTemporalAmount(dt) || isString(dt),
                sourceText(),
                paramOrdinal,
                "numeric, temporal, or string"
            );
            if (resolution.unresolved()) {
                return resolution;
            }
            paramOrdinal = FIFTH;
        }

        if (decay != null) {
            TypeResolution resolution = isNumeric(decay, sourceText(), paramOrdinal);
            if (resolution.unresolved()) {
                return resolution;
            }
            paramOrdinal = SIXTH;
        }

        if (type != null) {
            TypeResolution resolution = TypeResolutions.isString(type, sourceText(), paramOrdinal);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Decay(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.get(4),
            newChildren.get(5)
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(
            this,
            Decay::new,
            children().get(0),
            children().get(1),
            children().get(2),
            children().get(3),
            children().get(4),
            children().get(5)
        );
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        EvalOperator.ExpressionEvaluator.Factory valueFactory = toEvaluator.apply(value);
        EvalOperator.ExpressionEvaluator.Factory originFactory = toEvaluator.apply(origin);

        // Handle temporal scale conversion - fold temporal amounts to milliseconds
        EvalOperator.ExpressionEvaluator.Factory scaleFactory;
        if (isTimeDuration(scale.dataType())) {
            scaleFactory = getTemporalScaleAsMillis(toEvaluator);
        } else {
            scaleFactory = toEvaluator.apply(scale);
        }

        // Handle temporal offset conversion - fold temporal amounts to milliseconds
        EvalOperator.ExpressionEvaluator.Factory offsetFactory;
        if (offset != null) {
            if (isTimeDuration(offset.dataType())) {
                offsetFactory = getTemporalOffsetAsMillis(toEvaluator);
            } else {
                offsetFactory = toEvaluator.apply(offset);
            }
        } else {
            offsetFactory = getDefaultOffset();
        }

        EvalOperator.ExpressionEvaluator.Factory decayFactory = decay != null
            ? toEvaluator.apply(decay)
            : EvalOperator.DoubleFactory(DEFAULT_DECAY);

        EvalOperator.ExpressionEvaluator.Factory typeFactory = type != null
            ? toEvaluator.apply(type)
            : EvalOperator.BytesRefFactory(new BytesRef(DEFAULT_FUNCTION));

        return switch (value.dataType()) {
            case INTEGER -> new DecayIntEvaluator.Factory(
                source(),
                valueFactory,
                originFactory,
                scaleFactory,
                offsetFactory,
                decayFactory,
                typeFactory
            );
            case DOUBLE -> new DecayDoubleEvaluator.Factory(
                source(),
                valueFactory,
                originFactory,
                scaleFactory,
                offsetFactory,
                decayFactory,
                typeFactory
            );
            case LONG -> new DecayLongEvaluator.Factory(
                source(),
                valueFactory,
                originFactory,
                scaleFactory,
                offsetFactory,
                decayFactory,
                typeFactory
            );
            case GEO_POINT -> new DecayGeoPointEvaluator.Factory(
                source(),
                valueFactory,
                originFactory,
                scaleFactory,
                offsetFactory,
                decayFactory,
                typeFactory
            );
            case CARTESIAN_POINT -> new DecayCartesianPointEvaluator.Factory(
                source(),
                valueFactory,
                originFactory,
                scaleFactory,
                offsetFactory,
                decayFactory,
                typeFactory
            );
            case DATETIME -> new DecayDatetimeEvaluator.Factory(
                source(),
                valueFactory,
                originFactory,
                scaleFactory,
                offsetFactory,
                decayFactory,
                typeFactory
            );
            case DATE_NANOS -> new DecayDateNanosEvaluator.Factory(
                source(),
                valueFactory,
                originFactory,
                scaleFactory,
                offsetFactory,
                decayFactory,
                typeFactory
            );
            default -> throw new UnsupportedOperationException("Unsupported data type: " + value.dataType());
        };
    }

    @Evaluator(extraName = "Int")
    static double process(int value, int origin, int scale, int offset, double decay, BytesRef functionType) {
        return switch (functionType.utf8ToString()) {
            case "exp" -> new ScoreScriptUtils.DecayNumericExp(origin, scale, offset, decay).decayNumericExp(value);
            case "gauss" -> new ScoreScriptUtils.DecayNumericGauss(origin, scale, offset, decay).decayNumericGauss(value);
            default -> new ScoreScriptUtils.DecayNumericLinear(origin, scale, offset, decay).decayNumericLinear(value);
        };
    }

    @Evaluator(extraName = "Double")
    static double process(double value, double origin, double scale, double offset, double decay, BytesRef functionType) {
        return switch (functionType.utf8ToString()) {
            case "exp" -> new ScoreScriptUtils.DecayNumericExp(origin, scale, offset, decay).decayNumericExp(value);
            case "gauss" -> new ScoreScriptUtils.DecayNumericGauss(origin, scale, offset, decay).decayNumericGauss(value);
            default -> new ScoreScriptUtils.DecayNumericLinear(origin, scale, offset, decay).decayNumericLinear(value);
        };
    }

    @Evaluator(extraName = "Long")
    static double process(long value, long origin, long scale, long offset, double decay, BytesRef functionType) {
        return switch (functionType.utf8ToString()) {
            case "exp" -> new ScoreScriptUtils.DecayNumericExp(origin, scale, offset, decay).decayNumericExp(value);
            case "gauss" -> new ScoreScriptUtils.DecayNumericGauss(origin, scale, offset, decay).decayNumericGauss(value);
            default -> new ScoreScriptUtils.DecayNumericLinear(origin, scale, offset, decay).decayNumericLinear(value);
        };
    }

    @Evaluator(extraName = "GeoPoint")
    static double process(BytesRef value, BytesRef origin, BytesRef scale, BytesRef offset, double decay, BytesRef functionType) {
        Point valuePoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(value);
        GeoPoint valueGeoPoint = new GeoPoint(valuePoint.getY(), valuePoint.getX());

        Point originPoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(origin);
        GeoPoint originGeoPoint = new GeoPoint(originPoint.getY(), originPoint.getX());

        String originStr = originGeoPoint.getX() + "," + originGeoPoint.getY();
        String scaleStr = scale.utf8ToString();
        String offsetStr = offset.utf8ToString();

        return switch (functionType.utf8ToString()) {
            case "exp" -> new ScoreScriptUtils.DecayGeoExp(originStr, scaleStr, offsetStr, decay).decayGeoExp(valueGeoPoint);
            case "gauss" -> new ScoreScriptUtils.DecayGeoGauss(originStr, scaleStr, offsetStr, decay).decayGeoGauss(valueGeoPoint);
            default -> new ScoreScriptUtils.DecayGeoLinear(originStr, scaleStr, offsetStr, decay).decayGeoLinear(valueGeoPoint);
        };
    }

    @Evaluator(extraName = "CartesianPoint")
    static double processCartesianPoint(BytesRef value, BytesRef origin, double scale, double offset, double decay, BytesRef functionType) {
        Point valuePoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(value);
        Point originPoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(origin);

        // Euclidean distance
        double dx = valuePoint.getX() - originPoint.getX();
        double dy = valuePoint.getY() - originPoint.getY();
        double distance = Math.sqrt(dx * dx + dy * dy);

        distance = Math.max(0.0, distance - offset);

        return switch (functionType.utf8ToString()) {
            case "exp" -> {
                double scaling = Math.log(decay) / scale;
                yield Math.exp(scaling * distance);
            }
            case "gauss" -> {
                double sigmaSquared = -Math.pow(scale, 2.0) / (2.0 * Math.log(decay));
                yield Math.exp(-Math.pow(distance, 2.0) / (2.0 * sigmaSquared));
            }
            // linear
            default -> {
                double scaling = scale / (1.0 - decay);
                yield Math.max(0.0, (scaling - distance) / scaling);
            }
        };
    }

    @Evaluator(extraName = "Datetime", warnExceptions = { InvalidArgumentException.class, IllegalArgumentException.class })
    static double processDatetime(long value, long origin, long scale, long offset, double decay, BytesRef functionType) {
        ZonedDateTime dateTime = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);

        Duration scaleAmount = Duration.ofMillis(scale);
        Duration offsetAmount = Duration.ofMillis(offset);

        return switch (functionType.utf8ToString()) {
            case "exp" -> decayDateExp(origin, scaleAmount, offsetAmount, decay, dateTime);
            case "gauss" -> decayDateGauss(origin, scaleAmount, offsetAmount, decay, dateTime);
            default -> decayDateLinear(origin, scaleAmount, offsetAmount, decay, dateTime);
        };
    }

    @Evaluator(extraName = "DateNanos", warnExceptions = { InvalidArgumentException.class, IllegalArgumentException.class })
    static double processDateNanos(long value, long origin, long scale, long offset, double decay, BytesRef functionType) {
        long millis = DateUtils.toMilliSeconds(value);
        // TODO: UTC correct?
        ZonedDateTime dateTime = Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC);
        long originMillis = DateUtils.toMilliSeconds(origin);

        // TODO: nanosecond precision?
        Duration scaleAmount = Duration.ofMillis(scale);
        Duration offsetAmount = Duration.ofMillis(offset);

        return switch (functionType.utf8ToString()) {
            case "exp" -> decayDateExp(originMillis, scaleAmount, offsetAmount, decay, dateTime);
            case "gauss" -> decayDateGauss(originMillis, scaleAmount, offsetAmount, decay, dateTime);
            default -> decayDateLinear(originMillis, scaleAmount, offsetAmount, decay, dateTime);
        };
    }

    private static double decayDateLinear(long origin, Duration scale, Duration offset, double decay, ZonedDateTime docValueDate) {
        long docValue = docValueDate.toInstant().toEpochMilli();
        long offsetMillis = offset.toMillis();
        long scaleMillis = scale.toMillis();
        double scaling = scaleMillis / (1.0 - decay);

        long diff = (docValue >= origin) ? (docValue - origin) : (origin - docValue);
        long distance = Math.max(0, diff - offsetMillis);
        return Math.max(0.0, (scaling - distance) / scaling);
    }

    private static double decayDateExp(long origin, Duration scale, Duration offset, double decay, ZonedDateTime docValueDate) {
        long docValue = docValueDate.toInstant().toEpochMilli();
        long offsetMillis = offset.toMillis();
        long scaleMillis = scale.toMillis();
        double scaling = Math.log(decay) / scaleMillis;

        long diff = (docValue >= origin) ? (docValue - origin) : (origin - docValue);
        long distance = Math.max(0, diff - offsetMillis);
        return Math.exp(scaling * distance);
    }

    private static double decayDateGauss(long origin, Duration scale, Duration offset, double decay, ZonedDateTime docValueDate) {
        long docValue = docValueDate.toInstant().toEpochMilli();
        long offsetMillis = offset.toMillis();
        long scaleMillis = scale.toMillis();
        double scaling = 0.5 * Math.pow(scaleMillis, 2.0) / Math.log(decay);

        long diff = (docValue >= origin) ? (docValue - origin) : (origin - docValue);
        long distance = Math.max(0, diff - offsetMillis);
        return Math.exp(0.5 * Math.pow(distance, 2.0) / scaling);
    }

    private EvalOperator.ExpressionEvaluator.Factory getTemporalScaleAsMillis(ToEvaluator toEvaluator) {
        EvalOperator.ExpressionEvaluator.Factory scaleFactory;
        if (scale.foldable() == false) {
            throw new IllegalArgumentException(
                "Function [" + sourceText() + "] has non-constant temporal scale [" + scale.sourceText() + "]."
            );
        }
        Object foldedScale = scale.fold(toEvaluator.foldCtx());
        long scaleMillis = ((Duration) foldedScale).toMillis();
        scaleFactory = EvalOperator.LongFactory(scaleMillis);
        return scaleFactory;
    }

    private EvalOperator.ExpressionEvaluator.Factory getTemporalOffsetAsMillis(ToEvaluator toEvaluator) {
        EvalOperator.ExpressionEvaluator.Factory offsetFactory;
        if (offset.foldable() == false) {
            throw new IllegalArgumentException(
                "Function [" + sourceText() + "] has non-constant temporal offset [" + offset.sourceText() + "]."
            );
        }
        Object foldedOffset = offset.fold(toEvaluator.foldCtx());
        long offsetMillis = ((Duration) foldedOffset).toMillis();
        offsetFactory = EvalOperator.LongFactory(offsetMillis);
        return offsetFactory;
    }

    private EvalOperator.ExpressionEvaluator.Factory getDefaultOffset() {
        EvalOperator.ExpressionEvaluator.Factory offsetFactory;
        offsetFactory = switch (value.dataType()) {
            case INTEGER, LONG, DOUBLE -> EvalOperator.DoubleFactory(DEFAULT_NUMERIC_OFFSET);
            case GEO_POINT -> EvalOperator.BytesRefFactory(DEFAULT_GEO_POINT_OFFSET);
            case CARTESIAN_POINT -> EvalOperator.DoubleFactory(DEFAULT_CARTESIAN_POINT_OFFSET);
            case DATETIME, DATE_NANOS -> EvalOperator.LongFactory(DEFAULT_TEMPORAL_OFFSET);
            default -> throw new UnsupportedOperationException("Unsupported data type: " + value.dataType());
        };
        return offsetFactory;
    }
}
