/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.score;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.script.ScoreScriptUtils;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isCartesianPoint;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateNanos;
import static org.elasticsearch.xpack.esql.core.type.DataType.isGeoPoint;
import static org.elasticsearch.xpack.esql.core.type.DataType.isMillisOrNanos;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialPoint;
import static org.elasticsearch.xpack.esql.core.type.DataType.isTimeDuration;

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
public class Decay extends EsqlScalarFunction implements OptionalArgument, PostAnalysisPlanVerificationAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Decay", Decay::new);

    public static final String ORIGIN = "origin";
    public static final String SCALE = "scale";
    public static final String OFFSET = "offset";
    public static final String DECAY = "decay";
    public static final String TYPE = "type";

    private static final Map<String, Collection<DataType>> ALLOWED_OPTIONS = Map.of(
        OFFSET,
        Set.of(TIME_DURATION, INTEGER, LONG, DOUBLE, KEYWORD, TEXT),
        DECAY,
        Set.of(DOUBLE),
        TYPE,
        Set.of(KEYWORD)
    );

    // Default offsets
    private static final Integer DEFAULT_INTEGER_OFFSET = 0;
    private static final Long DEFAULT_LONG_OFFSET = 0L;
    private static final Double DEFAULT_DOUBLE_OFFSET = 0.0;
    private static final BytesRef DEFAULT_GEO_POINT_OFFSET = new BytesRef("0m");
    private static final Double DEFAULT_CARTESIAN_POINT_OFFSET = 0.0;
    private static final Long DEFAULT_TEMPORAL_OFFSET = 0L;

    private static final Double DEFAULT_DECAY = 0.5;

    private static final BytesRef DEFAULT_FUNCTION = new BytesRef("linear");

    private final Expression origin;
    private final Expression value;
    private final Expression scale;
    private final Expression options;

    private final Map<String, Object> resolvedOptions;

    @FunctionInfo(
        returnType = "double",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") },
        description = "Calculates a relevance score that decays based on the distance of a numeric, spatial or date type value from a target origin, using configurable decay functions.",
        // TODO: detailed description
        examples = { @Example(file = "decay", tag = "decay") }
    )
    public Decay(
        Source source,
        @Param(
            name = "value",
            type = { "double", "integer", "long", "date", "date_nanos", "geo_point", "cartesian_point" },
            description = "The input value to apply decay scoring to."
        ) Expression value,
        @Param(
            name = ORIGIN,
            type = { "double", "integer", "long", "date", "date_nanos", "geo_point", "cartesian_point" },
            description = "Central point from which the distances are calculated."
        ) Expression origin,
        @Param(
            name = SCALE,
            type = { "double", "integer", "long", "time_duration", "keyword", "text" },
            description = "Distance from the origin where the function returns the decay value."
        ) Expression scale,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = OFFSET,
                    type = { "double", "integer", "long", "time_duration", "keyword", "text" },
                    description = "Distance from the origin where no decay occurs."
                ),
                @MapParam.MapParamEntry(
                    name = DECAY,
                    type = { "double" },
                    description = "Multiplier value returned at the scale distance from the origin."
                ),
                @MapParam.MapParamEntry(
                    name = TYPE,
                    type = { "keyword" },
                    description = "Decay function to use: linear, exponential or gaussian."
                ) },
            optional = true
        ) Expression options
    ) {
        super(source, options != null ? List.of(value, origin, scale, options) : List.of(value, origin, scale));
        this.value = value;
        this.origin = origin;
        this.scale = scale;
        this.options = options;
        this.resolvedOptions = new HashMap<>();
    }

    private Decay(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(value);
        out.writeNamedWriteable(origin);
        out.writeNamedWriteable(scale);
        out.writeOptionalNamedWriteable(options);
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
            isType(value, dt -> dt.isNumeric() || dt.isDate() || isSpatialPoint(dt), sourceText(), FIRST, "numeric, date or spatial point")
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
                scaleResolution = isType(scale, DataType::isString, sourceText(), THIRD, "keyword or text");
            } else {
                // Cartesian points: scale should be numeric (e.g. 100.0)
                scaleResolution = isType(scale, DataType::isNumeric, sourceText(), THIRD, "numeric");
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

        return Options.resolveWithMultipleDataTypesAllowed(options, source(), FOURTH, ALLOWED_OPTIONS);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Decay(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), options != null ? newChildren.get(3) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Decay::new, children().get(0), children().get(1), children().get(2), children().get(3));
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        DataType valueDataType = value.dataType();
        Options.populateMapWithExpressionsMultipleDataTypesAllowed(
            (MapExpression) options,
            resolvedOptions,
            source(),
            FOURTH,
            ALLOWED_OPTIONS
        );

        EvalOperator.ExpressionEvaluator.Factory valueFactory = toEvaluator.apply(value);

        Expression offsetExpr = (Expression) resolvedOptions.get(OFFSET);
        Expression decayExpr = (Expression) resolvedOptions.get(DECAY);
        Expression typeExpr = (Expression) resolvedOptions.get(TYPE);

        FoldContext foldCtx = toEvaluator.foldCtx();

        // Constants
        Object originFolded = origin.fold(foldCtx);
        Object scaleFolded = getScale(foldCtx, valueDataType);
        Object offsetFolded = getOffset(foldCtx, valueDataType, offsetExpr);
        Double decayFolded = decayExpr != null ? (Double) decayExpr.fold(foldCtx) : DEFAULT_DECAY;
        BytesRef typeFolded = typeExpr != null ? (BytesRef) typeExpr.fold(foldCtx) : DEFAULT_FUNCTION;

        return switch (valueDataType) {
            case INTEGER -> new DecayIntEvaluator.Factory(
                source(),
                valueFactory,
                (Integer) originFolded,
                (Integer) scaleFolded,
                (Integer) offsetFolded,
                decayFolded,
                typeFolded
            );
            case DOUBLE -> new DecayDoubleEvaluator.Factory(
                source(),
                valueFactory,
                (Double) originFolded,
                (Double) scaleFolded,
                (Double) offsetFolded,
                decayFolded,
                typeFolded
            );
            case LONG -> new DecayLongEvaluator.Factory(
                source(),
                valueFactory,
                (Long) originFolded,
                (Long) scaleFolded,
                (Long) offsetFolded,
                decayFolded,
                typeFolded
            );
            case GEO_POINT -> new DecayGeoPointEvaluator.Factory(
                source(),
                valueFactory,
                (BytesRef) originFolded,
                (BytesRef) scaleFolded,
                (BytesRef) offsetFolded,
                decayFolded,
                typeFolded
            );
            case CARTESIAN_POINT -> new DecayCartesianPointEvaluator.Factory(
                source(),
                valueFactory,
                (BytesRef) originFolded,
                (Double) scaleFolded,
                (Double) offsetFolded,
                decayFolded,
                typeFolded
            );
            case DATETIME -> new DecayDatetimeEvaluator.Factory(
                source(),
                valueFactory,
                (Long) originFolded,
                (Long) scaleFolded,
                (Long) offsetFolded,
                decayFolded,
                typeFolded
            );
            case DATE_NANOS -> new DecayDateNanosEvaluator.Factory(
                source(),
                valueFactory,
                (Long) originFolded,
                (Long) scaleFolded,
                (Long) offsetFolded,
                decayFolded,
                typeFolded
            );
            default -> throw new UnsupportedOperationException("Unsupported data typeExpr: " + valueDataType);
        };
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (LogicalPlan plan, Failures failures) -> {
            Expression offset = (Expression) resolvedOptions.get(OFFSET);
            Expression decay = (Expression) resolvedOptions.get(DECAY);
            Expression type = (Expression) resolvedOptions.get(TYPE);

            Map<String, Expression> constantExpressions = new HashMap<>();
            constantExpressions.put(ORIGIN, origin);
            constantExpressions.put(SCALE, scale);
            constantExpressions.put(OFFSET, offset);
            constantExpressions.put(DECAY, decay);
            constantExpressions.put(TYPE, type);

            // Verify parameter except "value" is constant
            constantExpressions.forEach((exprName, expr) -> {
                if (Objects.nonNull(expr) && expr.foldable() == false) {
                    failures.add(fail(expr, "Function [{}] has non-constant value [{}] [{}].", sourceText(), exprName, expr.sourceText()));
                }
            });
        };
    }

    @Evaluator(extraName = "Int")
    static double process(
        int value,
        @Fixed int origin,
        @Fixed int scale,
        @Fixed int offset,
        @Fixed double decay,
        @Fixed BytesRef functionType
    ) {
        return switch (functionType.utf8ToString()) {
            case "exp" -> new ScoreScriptUtils.DecayNumericExp(origin, scale, offset, decay).decayNumericExp(value);
            case "gauss" -> new ScoreScriptUtils.DecayNumericGauss(origin, scale, offset, decay).decayNumericGauss(value);
            default -> new ScoreScriptUtils.DecayNumericLinear(origin, scale, offset, decay).decayNumericLinear(value);
        };
    }

    @Evaluator(extraName = "Double")
    static double process(
        double value,
        @Fixed double origin,
        @Fixed double scale,
        @Fixed double offset,
        @Fixed double decay,
        @Fixed BytesRef functionType
    ) {
        return switch (functionType.utf8ToString()) {
            case "exp" -> new ScoreScriptUtils.DecayNumericExp(origin, scale, offset, decay).decayNumericExp(value);
            case "gauss" -> new ScoreScriptUtils.DecayNumericGauss(origin, scale, offset, decay).decayNumericGauss(value);
            default -> new ScoreScriptUtils.DecayNumericLinear(origin, scale, offset, decay).decayNumericLinear(value);
        };
    }

    @Evaluator(extraName = "Long")
    static double process(
        long value,
        @Fixed long origin,
        @Fixed long scale,
        @Fixed long offset,
        @Fixed double decay,
        @Fixed BytesRef functionType
    ) {
        return switch (functionType.utf8ToString()) {
            case "exp" -> new ScoreScriptUtils.DecayNumericExp(origin, scale, offset, decay).decayNumericExp(value);
            case "gauss" -> new ScoreScriptUtils.DecayNumericGauss(origin, scale, offset, decay).decayNumericGauss(value);
            default -> new ScoreScriptUtils.DecayNumericLinear(origin, scale, offset, decay).decayNumericLinear(value);
        };
    }

    @Evaluator(extraName = "GeoPoint")
    static double process(
        BytesRef value,
        @Fixed BytesRef origin,
        @Fixed BytesRef scale,
        @Fixed BytesRef offset,
        @Fixed double decay,
        @Fixed BytesRef functionType
    ) {
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
    static double processCartesianPoint(
        BytesRef value,
        @Fixed BytesRef origin,
        @Fixed double scale,
        @Fixed double offset,
        @Fixed double decay,
        @Fixed BytesRef functionType
    ) {
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
    static double processDatetime(
        long value,
        @Fixed long origin,
        @Fixed long scale,
        @Fixed long offset,
        @Fixed double decay,
        @Fixed BytesRef functionType
    ) {
        return switch (functionType.utf8ToString()) {
            case "exp" -> decayDateExp(origin, scale, offset, decay, value);
            case "gauss" -> decayDateGauss(origin, scale, offset, decay, value);
            default -> decayDateLinear(origin, scale, offset, decay, value);
        };
    }

    @Evaluator(extraName = "DateNanos", warnExceptions = { InvalidArgumentException.class, IllegalArgumentException.class })
    static double processDateNanos(
        long value,
        @Fixed long origin,
        @Fixed long scale,
        @Fixed long offset,
        @Fixed double decay,
        @Fixed BytesRef functionType
    ) {
        return switch (functionType.utf8ToString()) {
            case "exp" -> decayDateExp(origin, scale, offset, decay, value);
            case "gauss" -> decayDateGauss(origin, scale, offset, decay, value);
            default -> decayDateLinear(origin, scale, offset, decay, value);
        };
    }

    private static double decayDateLinear(long origin, long scale, long offset, double decay, long value) {
        double scaling = scale / (1.0 - decay);

        long diff = (value >= origin) ? (value - origin) : (origin - value);
        long distance = Math.max(0, diff - offset);
        return Math.max(0.0, (scaling - distance) / scaling);
    }

    private static double decayDateExp(long origin, long scale, long offset, double decay, long value) {
        double scaling = Math.log(decay) / scale;

        long diff = (value >= origin) ? (value - origin) : (origin - value);
        long distance = Math.max(0, diff - offset);
        return Math.exp(scaling * distance);
    }

    private static double decayDateGauss(long origin, long scale, long offset, double decay, long value) {
        double scaling = 0.5 * Math.pow(scale, 2.0) / Math.log(decay);

        long diff = (value >= origin) ? (value - origin) : (origin - value);
        long distance = Math.max(0, diff - offset);
        return Math.exp(0.5 * Math.pow(distance, 2.0) / scaling);
    }

    private Object getOffset(FoldContext foldCtx, DataType valueDataType, Expression offset) {
        if (offset == null) {
            return getDefaultOffset(valueDataType);
        }

        if (isTimeDuration(offset.dataType()) == false) {
            return offset.fold(foldCtx);
        }

        if (isDateNanos(valueDataType)) {
            return getTemporalOffsetAsNanos(foldCtx, offset);
        }

        return getTemporalOffsetAsMillis(foldCtx, offset);
    }

    private Object getScale(FoldContext foldCtx, DataType valueDataType) {
        if (isTimeDuration(scale.dataType()) == false) {
            return scale.fold(foldCtx);
        }

        if (isDateNanos(valueDataType)) {
            return getTemporalScaleAsNanos(foldCtx);
        }

        return getTemporalScaleAsMillis(foldCtx);
    }

    private Long getTemporalScaleAsMillis(FoldContext foldCtx) {
        Object foldedScale = scale.fold(foldCtx);
        return ((Duration) foldedScale).toMillis();
    }

    private Long getTemporalScaleAsNanos(FoldContext foldCtx) {
        Object foldedScale = scale.fold(foldCtx);
        Duration scaleDuration = (Duration) foldedScale;
        return scaleDuration.toNanos();
    }

    private Long getTemporalOffsetAsMillis(FoldContext foldCtx, Expression offset) {
        Object foldedOffset = offset.fold(foldCtx);
        return ((Duration) foldedOffset).toMillis();
    }

    private Long getTemporalOffsetAsNanos(FoldContext foldCtx, Expression offset) {
        Object foldedOffset = offset.fold(foldCtx);
        Duration offsetDuration = (Duration) foldedOffset;
        return offsetDuration.toNanos();
    }

    private Object getDefaultOffset(DataType valueDataType) {
        return switch (valueDataType) {
            case INTEGER -> DEFAULT_INTEGER_OFFSET;
            case LONG -> DEFAULT_LONG_OFFSET;
            case DOUBLE -> DEFAULT_DOUBLE_OFFSET;
            case GEO_POINT -> DEFAULT_GEO_POINT_OFFSET;
            case CARTESIAN_POINT -> DEFAULT_CARTESIAN_POINT_OFFSET;
            case DATETIME, DATE_NANOS -> DEFAULT_TEMPORAL_OFFSET;
            default -> throw new UnsupportedOperationException("Unsupported data type: " + valueDataType);
        };
    }
}
