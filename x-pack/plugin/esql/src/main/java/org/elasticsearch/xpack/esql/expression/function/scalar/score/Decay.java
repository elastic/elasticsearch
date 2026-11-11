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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.script.ScoreScriptUtils;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.Signature;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateNanos;
import static org.elasticsearch.xpack.esql.core.type.DataType.isGeoPoint;
import static org.elasticsearch.xpack.esql.core.type.DataType.isMillisOrNanos;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialPoint;

/**
 * Decay a numeric, spatial or date type value based on the distance of it to an origin.
 *
 * This function uses the same {@link ScoreScriptUtils} implementations as Painless scripts,
 * ensuring consistent decay calculations across ES|QL and script contexts. The decay
 * functions support linear, exponential, and gaussian decay types for:
 * - Numeric types (int, long, double, unsigned_long)
 * - Spatial types (geo_point, cartesian_point)
 * - Temporal types (datetime, date_nanos)
 */
public class Decay extends EsqlScalarFunction implements OptionalArgument, PostOptimizationVerificationAware, AnyNullIsNull {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Decay", Decay::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Decay.class)
        .quaternary(Decay::new)
        .capabilities("ga_fixes")
        .name("decay");

    public static final String ORIGIN = "origin";
    public static final String SCALE = "scale";
    public static final String OFFSET = "offset";
    public static final String DECAY = "decay";
    public static final String TYPE = "type";

    private static final Map<String, Collection<DataType>> ALLOWED_OPTIONS = Map.of(
        OFFSET,
        Set.of(TIME_DURATION, INTEGER, LONG, DOUBLE, KEYWORD, TEXT, UNSIGNED_LONG),
        DECAY,
        Set.of(DOUBLE),
        TYPE,
        Set.of(KEYWORD)
    );

    private static final BytesRef DEFAULT_GEO_POINT_OFFSET = new BytesRef("0m");

    private static final double DEFAULT_DECAY = 0.5;

    private static final BytesRef DEFAULT_FUNCTION = new BytesRef("linear");

    private final Expression origin;
    private final Expression value;
    private final Expression scale;
    private final Expression options;

    private final Map<String, Object> resolvedOptions;

    @FunctionInfo(
        returnType = "double",
        signatures = {
            @Signature(params = { "integer", "integer", "integer", "named_parameters" }, returnType = "double"),
            @Signature(params = { "long", "long", "long", "named_parameters" }, returnType = "double"),
            @Signature(params = { "unsigned_long", "unsigned_long", "unsigned_long", "named_parameters" }, returnType = "double"),
            @Signature(params = { "double", "double", "double", "named_parameters" }, returnType = "double"),
            @Signature(params = { "geo_point", "geo_point", "STRING", "named_parameters" }, returnType = "double"),
            @Signature(params = { "cartesian_point", "cartesian_point", "double", "named_parameters" }, returnType = "double"),
            @Signature(params = { "date", "date", "time_duration", "named_parameters" }, returnType = "double"),
            @Signature(params = { "date_nanos", "date_nanos", "time_duration", "named_parameters" }, returnType = "double") },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") },
        briefSummary = "Calculates a relevance score that decays with distance from a target origin.",
        description = "Calculates a relevance score that decays based on the distance of a numeric, spatial or date type value "
            + "from a target origin, using configurable decay functions.",
        detailedDescription = """
            `DECAY` calculates a score between 0 and 1 based on how far a field value is from a specified origin point (called distance).
            The distance can be a numeric distance, spatial distance or temporal distance depending on the specific data type.

            `DECAY` can use <<esql-function-named-params,function named parameters>> to specify additional `options`
            for the decay function.

            For spatial queries, scale and offset for geo points use distance units (e.g., "10km", "5mi"),
            while cartesian points use numeric values. For date queries, scale and offset use time_duration values.
            For numeric queries you also use numeric values.

            :::{tip}
            Learn more about using [ES|QL for search use cases](docs-content://solutions/search/esql-for-search.md).
            :::
            """,
        examples = { @Example(file = "decay", tag = "decay") }
    )
    public Decay(
        Source source,
        @Param(
            name = "value",
            type = { "double", "integer", "long", "unsigned_long", "date", "date_nanos", "geo_point", "cartesian_point" },
            description = "The input value to apply decay scoring to."
        ) Expression value,
        @Param(
            name = ORIGIN,
            type = { "double", "integer", "long", "unsigned_long", "date", "date_nanos", "geo_point", "cartesian_point" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Central point from which the distances are calculated."
        ) Expression origin,
        @Param(
            name = SCALE,
            type = { "double", "integer", "long", "unsigned_long", "time_duration", "keyword", "text" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Distance from the origin where the function returns the decay value."
        ) Expression scale,
        @MapParam(
            name = "options",
            description = "(Optional) Additional options such as `decay`, `offset` and `type`.",
            params = {
                @MapParam.MapParamEntry(
                    name = OFFSET,
                    type = { "double", "integer", "long", "unsigned_long", "time_duration", "keyword", "text" },
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
                    description = "Decay function to use: linear, exp or gauss. Defaults to linear."
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

    Expression value() {
        return value;
    }

    Expression origin() {
        return origin;
    }

    Expression scale() {
        return scale;
    }

    Expression options() {
        return options;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return validateValue().and(() -> Options.resolveWithMultipleDataTypesAllowed(options, source(), FOURTH, ALLOWED_OPTIONS))
            .and(() -> validateOriginScaleAndOffset(value.dataType()))
            .and(this::validateTypeOption);
    }

    private TypeResolution validateValue() {
        return isNotNull(value, sourceText(), FIRST).and(
            isType(value, dt -> dt.isNumeric() || dt.isDate() || isSpatialPoint(dt), sourceText(), FIRST, "numeric, date or spatial point")
        );
    }

    private TypeResolution validateOriginScaleAndOffset(DataType valueType) {
        if (isSpatialPoint(valueType)) {
            boolean isGeoPoint = isGeoPoint(valueType);

            return validateOriginScaleAndOffset(
                dt -> dt == valueType,
                valueType.typeName(),
                isGeoPoint ? DataType::isString : DataType::isNumeric,
                isGeoPoint ? "keyword or text" : "numeric",
                isGeoPoint ? DataType::isString : DataType::isNumeric,
                isGeoPoint ? "keyword or text" : "numeric"
            );
        } else if (isMillisOrNanos(valueType)) {
            return validateOriginScaleAndOffset(
                DataType::isMillisOrNanos,
                "datetime or date_nanos",
                DataType::isTimeDuration,
                "time_duration",
                DataType::isTimeDuration,
                "time_duration"
            );
        } else {
            return validateOriginScaleAndOffset(
                DataType::isNumeric,
                "numeric",
                DataType::isNumeric,
                "numeric",
                DataType::isNumeric,
                "numeric"
            );
        }
    }

    private TypeResolution validateOriginScaleAndOffset(
        Predicate<DataType> originPredicate,
        String originDesc,
        Predicate<DataType> scalePredicate,
        String scaleDesc,
        Predicate<DataType> offsetPredicate,
        String offsetDesc
    ) {
        if (options != null) {
            Expression offset = ((MapExpression) options).keyFoldedMap().get(OFFSET);
            if (offset != null && offset.dataType() != NULL && offsetPredicate.test((offset).dataType()) == false) {
                return new TypeResolution(
                    format(null, "{} option has invalid type, expected [{}], found [{}]", OFFSET, offsetDesc, offset.dataType().typeName())
                );
            }
        }

        return isNotNull(origin, sourceText(), SECOND).and(isType(origin, originPredicate, sourceText(), SECOND, originDesc))
            .and(isNotNull(scale, sourceText(), THIRD))
            .and(isType(scale, scalePredicate, sourceText(), THIRD, scaleDesc));
    }

    private TypeResolution validateTypeOption() {
        if (options == null) {
            return TypeResolution.TYPE_RESOLVED;
        }

        Expression decayType = ((MapExpression) options).keyFoldedMap().get(TYPE);

        if (decayType == null || decayType.dataType() == NULL) {
            return TypeResolution.TYPE_RESOLVED;
        }

        if (decayType.dataType() != KEYWORD) {
            return new TypeResolution(
                format(
                    null,
                    "{} option has invalid type, expected [{}], found [{}]",
                    TYPE,
                    KEYWORD.typeName(),
                    decayType.dataType().typeName()
                )
            );
        }

        String decayTypeName = BytesRefs.toString(decayType.fold(FoldContext.small())).toLowerCase(Locale.ROOT);

        if (DecayFunction.BY_NAME.containsKey(decayTypeName) == false) {
            return new TypeResolution(
                format(
                    null,
                    "{} option has invalid value, expected one of [gauss, linear, exp], found [{}]",
                    TYPE,
                    decayType.source().text()
                )
            );
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Decay(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), options != null ? newChildren.get(3) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Decay::new, value, origin, scale, options);
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return evaluatorFactory(toEvaluator.apply(value), toEvaluator.foldCtx());
    }

    private ExpressionEvaluator.Factory evaluatorFactory(ExpressionEvaluator.Factory valueFactory, FoldContext foldCtx) {
        DataType valueDataType = value.dataType();
        Options.populateMapWithExpressionsMultipleDataTypesAllowed(
            (MapExpression) options,
            resolvedOptions,
            source(),
            FOURTH,
            ALLOWED_OPTIONS
        );

        Expression offsetExpr = (Expression) resolvedOptions.get(OFFSET);
        Expression decayExpr = (Expression) resolvedOptions.get(DECAY);
        Expression typeExpr = (Expression) resolvedOptions.get(TYPE);

        // Constants
        Object originFolded = origin.fold(foldCtx);
        Object scaleFolded = scale.fold(foldCtx);
        Object offsetFolded = offsetExpr != null ? offsetExpr.fold(foldCtx) : null;
        double decayFolded = decayExpr != null ? ((Number) decayExpr.fold(foldCtx)).doubleValue() : DEFAULT_DECAY;
        if ((decayFolded > 0 && decayFolded < 1) == false) {
            throw new InvalidArgumentException("[{}] option must be in the range (0, 1), found [{}]", DECAY, decayFolded);
        }
        DecayFunction decayFunction = DecayFunction.fromBytesRef(typeExpr != null ? (BytesRef) typeExpr.fold(foldCtx) : DEFAULT_FUNCTION);

        return switch (valueDataType) {
            case INTEGER -> new DecayIntEvaluator.Factory(
                source(),
                valueFactory,
                toDoubleParam(originFolded, origin.dataType()),
                numericScale(scaleFolded),
                numericOffset(offsetExpr, offsetFolded),
                decayFolded,
                decayFunction
            );
            case DOUBLE -> new DecayDoubleEvaluator.Factory(
                source(),
                valueFactory,
                toDoubleParam(originFolded, origin.dataType()),
                numericScale(scaleFolded),
                numericOffset(offsetExpr, offsetFolded),
                decayFolded,
                decayFunction
            );
            case LONG -> new DecayLongEvaluator.Factory(
                source(),
                valueFactory,
                toDoubleParam(originFolded, origin.dataType()),
                numericScale(scaleFolded),
                numericOffset(offsetExpr, offsetFolded),
                decayFolded,
                decayFunction
            );
            case UNSIGNED_LONG -> new DecayUnsignedLongEvaluator.Factory(
                source(),
                valueFactory,
                toDoubleParam(originFolded, origin.dataType()),
                numericScale(scaleFolded),
                numericOffset(offsetExpr, offsetFolded),
                decayFolded,
                decayFunction
            );
            case GEO_POINT -> {
                BytesRef geoScale = (BytesRef) scaleFolded;
                BytesRef geoOffset = offsetExpr != null ? (BytesRef) offsetFolded : DEFAULT_GEO_POINT_OFFSET;
                checkScale(parseDistance(SCALE, geoScale), geoScale.utf8ToString());
                checkOffset(parseDistance(OFFSET, geoOffset), geoOffset.utf8ToString());
                yield new DecayGeoPointEvaluator.Factory(
                    source(),
                    valueFactory,
                    (BytesRef) originFolded,
                    geoScale,
                    geoOffset,
                    decayFolded,
                    decayFunction
                );
            }
            case CARTESIAN_POINT -> new DecayCartesianPointEvaluator.Factory(
                source(),
                valueFactory,
                (BytesRef) originFolded,
                numericScale(scaleFolded),
                numericOffset(offsetExpr, offsetFolded),
                decayFolded,
                decayFunction
            );
            case DATETIME -> new DecayDatetimeEvaluator.Factory(
                source(),
                valueFactory,
                temporalOrigin(originFolded, valueDataType),
                temporalScale(scaleFolded, valueDataType),
                temporalOffset(offsetFolded, valueDataType),
                decayFolded,
                decayFunction
            );
            case DATE_NANOS -> new DecayDateNanosEvaluator.Factory(
                source(),
                valueFactory,
                temporalOrigin(originFolded, valueDataType),
                temporalScale(scaleFolded, valueDataType),
                temporalOffset(offsetFolded, valueDataType),
                decayFolded,
                decayFunction
            );
            default -> throw new UnsupportedOperationException("Unsupported data type: " + valueDataType);
        };
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        // Verify that "origin" and "scale" are literal values
        Map.of(ORIGIN, origin, SCALE, scale).forEach((exprName, expr) -> {
            if ((expr instanceof Literal) == false) {
                failures.add(fail(expr, "Function [{}] has non-literal value [{}].", sourceText(), exprName));
            }
        });
        if (origin instanceof Literal && scale instanceof Literal) {
            // Data nodes only check the constants when they build the evaluator, which never happens for a target without
            // shards, so check them here on the coordinator too. The value is not needed for that.
            try {
                evaluatorFactory(null, FoldContext.small());
            } catch (InvalidArgumentException e) {
                failures.add(fail(this, "{}", e.getMessage()));
            }
        }
    }

    @Evaluator(extraName = "Int")
    static double process(
        int value,
        @Fixed double origin,
        @Fixed double scale,
        @Fixed double offset,
        @Fixed double decay,
        @Fixed DecayFunction decayFunction
    ) {
        return decayFunction.numericDecay(value, origin, scale, offset, decay);
    }

    @Evaluator(extraName = "Double")
    static double process(
        double value,
        @Fixed double origin,
        @Fixed double scale,
        @Fixed double offset,
        @Fixed double decay,
        @Fixed DecayFunction decayFunction
    ) {
        return decayFunction.numericDecay(value, origin, scale, offset, decay);
    }

    @Evaluator(extraName = "Long")
    static double process(
        long value,
        @Fixed double origin,
        @Fixed double scale,
        @Fixed double offset,
        @Fixed double decay,
        @Fixed DecayFunction decayFunction
    ) {
        return decayFunction.numericDecay(value, origin, scale, offset, decay);
    }

    @Evaluator(extraName = "UnsignedLong")
    static double processUnsignedLong(
        long value,
        @Fixed double origin,
        @Fixed double scale,
        @Fixed double offset,
        @Fixed double decay,
        @Fixed DecayFunction decayFunction
    ) {
        // unsigned_long values are stored as longs offset by Long.MIN_VALUE; decode to their
        // true magnitude as a double before feeding the score-script math, which operates in double space.
        return decayFunction.numericDecay(NumericUtils.unsignedLongToDouble(value), origin, scale, offset, decay);
    }

    @Evaluator(extraName = "GeoPoint")
    static double process(
        BytesRef value,
        @Fixed BytesRef origin,
        @Fixed BytesRef scale,
        @Fixed BytesRef offset,
        @Fixed double decay,
        @Fixed DecayFunction decayFunction
    ) {
        Point valuePoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(value);
        GeoPoint valueGeoPoint = new GeoPoint(valuePoint.getY(), valuePoint.getX());

        Point originPoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(origin);
        GeoPoint originGeoPoint = new GeoPoint(originPoint.getY(), originPoint.getX());

        // GeoUtils.parseGeoPoint expects coordinate strings in "lat,lon" order; use getY (lat) then getX (lon).
        String originStr = originGeoPoint.getY() + "," + originGeoPoint.getX();
        String scaleStr = scale.utf8ToString();
        String offsetStr = offset.utf8ToString();

        return decayFunction.geoPointDecay(valueGeoPoint, originStr, scaleStr, offsetStr, decay);
    }

    @Evaluator(extraName = "CartesianPoint")
    static double processCartesianPoint(
        BytesRef value,
        @Fixed BytesRef origin,
        @Fixed double scale,
        @Fixed double offset,
        @Fixed double decay,
        @Fixed DecayFunction decayFunction
    ) {
        Point valuePoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(value);
        Point originPoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(origin);

        // Euclidean distance
        double dx = valuePoint.getX() - originPoint.getX();
        double dy = valuePoint.getY() - originPoint.getY();
        double distance = Math.sqrt(dx * dx + dy * dy);

        distance = Math.max(0.0, distance - offset);

        return decayFunction.cartesianDecay(distance, scale, offset, decay);
    }

    @Evaluator(extraName = "Datetime", warnExceptions = { InvalidArgumentException.class, IllegalArgumentException.class })
    static double processDatetime(
        long value,
        @Fixed long origin,
        @Fixed long scale,
        @Fixed long offset,
        @Fixed double decay,
        @Fixed DecayFunction decayFunction
    ) {
        return decayFunction.temporalDecay(value, origin, scale, offset, decay);
    }

    @Evaluator(extraName = "DateNanos", warnExceptions = { InvalidArgumentException.class, IllegalArgumentException.class })
    static double processDateNanos(
        long value,
        @Fixed long origin,
        @Fixed long scale,
        @Fixed long offset,
        @Fixed double decay,
        @Fixed DecayFunction decayFunction
    ) {
        return decayFunction.temporalDecay(value, origin, scale, offset, decay);

    }

    public enum DecayFunction {
        LINEAR("linear") {
            @Override
            public double numericDecay(double value, double origin, double scale, double offset, double decay) {
                return new ScoreScriptUtils.DecayNumericLinear(origin, scale, offset, decay).decayNumericLinear(value);
            }

            @Override
            public double geoPointDecay(GeoPoint value, String origin, String scale, String offset, double decay) {
                return new ScoreScriptUtils.DecayGeoLinear(origin, scale, offset, decay).decayGeoLinear(value);
            }

            @Override
            public double cartesianDecay(double distance, double scale, double offset, double decay) {
                double scaling = scale / (1.0 - decay);
                return Math.max(0.0, (scaling - distance) / scaling);
            }

            @Override
            public double temporalDecay(long value, long origin, long scale, long offset, double decay) {
                return decayDateLinear(origin, scale, offset, decay, value);
            }
        },

        EXPONENTIAL("exp") {
            @Override
            public double numericDecay(double value, double origin, double scale, double offset, double decay) {
                return new ScoreScriptUtils.DecayNumericExp(origin, scale, offset, decay).decayNumericExp(value);
            }

            @Override
            public double geoPointDecay(GeoPoint value, String origin, String scale, String offset, double decay) {
                return new ScoreScriptUtils.DecayGeoExp(origin, scale, offset, decay).decayGeoExp(value);
            }

            @Override
            public double cartesianDecay(double distance, double scale, double offset, double decay) {
                double scaling = Math.log(decay) / scale;
                return Math.exp(scaling * distance);
            }

            @Override
            public double temporalDecay(long value, long origin, long scale, long offset, double decay) {
                return decayDateExp(origin, scale, offset, decay, value);
            }
        },

        GAUSSIAN("gauss") {
            @Override
            public double numericDecay(double value, double origin, double scale, double offset, double decay) {
                return new ScoreScriptUtils.DecayNumericGauss(origin, scale, offset, decay).decayNumericGauss(value);
            }

            @Override
            public double geoPointDecay(GeoPoint value, String origin, String scale, String offset, double decay) {
                return new ScoreScriptUtils.DecayGeoGauss(origin, scale, offset, decay).decayGeoGauss(value);
            }

            @Override
            public double cartesianDecay(double distance, double scale, double offset, double decay) {
                double sigmaSquared = -Math.pow(scale, 2.0) / (2.0 * Math.log(decay));
                return Math.exp(-Math.pow(distance, 2.0) / (2.0 * sigmaSquared));
            }

            @Override
            public double temporalDecay(long value, long origin, long scale, long offset, double decay) {
                return decayDateGauss(origin, scale, offset, decay, value);
            }
        };

        private final String functionName;
        private static final Map<String, DecayFunction> BY_NAME = Arrays.stream(values())
            .collect(Collectors.toMap(df -> df.functionName, df -> df));

        DecayFunction(String functionName) {
            this.functionName = functionName;
        }

        public abstract double numericDecay(double value, double origin, double scale, double offset, double decay);

        public abstract double geoPointDecay(GeoPoint value, String origin, String scale, String offset, double decay);

        public abstract double cartesianDecay(double distance, double scale, double offset, double decay);

        public abstract double temporalDecay(long value, long origin, long scale, long offset, double decay);

        public static DecayFunction fromBytesRef(BytesRef functionType) {
            String name = functionType.utf8ToString().toLowerCase(Locale.ROOT);
            DecayFunction decayFunction = BY_NAME.get(name);
            if (decayFunction == null) {
                throw new InvalidArgumentException("unknown decay type [{}], expected one of [gauss, linear, exp]", name);
            }
            return decayFunction;
        }
    }

    private static double decayDateLinear(long origin, long scale, long offset, double decay, long value) {
        double scaling = scale / (1.0 - decay);
        double distance = temporalDistance(value, origin, offset);
        return Math.max(0.0, (scaling - distance) / scaling);
    }

    private static double decayDateExp(long origin, long scale, long offset, double decay, long value) {
        double scaling = Math.log(decay) / scale;
        double distance = temporalDistance(value, origin, offset);
        return Math.exp(scaling * distance);
    }

    private static double decayDateGauss(long origin, long scale, long offset, double decay, long value) {
        double scaling = 0.5 * Math.pow(scale, 2.0) / Math.log(decay);
        double distance = temporalDistance(value, origin, offset);
        return Math.exp(0.5 * Math.pow(distance, 2.0) / scaling);
    }

    private static double temporalDistance(long value, long origin, long offset) {
        // as java.lang.Math#abs(long) is a forbidden API, have to use this comparison instead
        long diff = (value >= origin) ? (value - origin) : (origin - value);
        if (diff >= 0) {
            return Math.max(0, diff - offset);
        }
        // the subtraction overflowed, so diff holds the exact distance as an unsigned long above Long.MAX_VALUE;
        // offset is below it, so subtracting in unsigned arithmetic is exact
        return NumericUtils.unsignedLongToDouble(NumericUtils.asLongUnsigned(diff - offset));
    }

    private double numericScale(Object folded) {
        double scaleValue = toDoubleParam(folded, scale.dataType());
        checkScale(scaleValue, scaleValue);
        return scaleValue;
    }

    private static double numericOffset(Expression offset, Object folded) {
        if (offset == null) {
            return 0.0;
        }
        double offsetValue = toDoubleParam(folded, offset.dataType());
        checkOffset(offsetValue, offsetValue);
        return offsetValue;
    }

    private static double toDoubleParam(Object folded, DataType type) {
        if (type == UNSIGNED_LONG) {
            return NumericUtils.unsignedLongToDouble(((Number) folded).longValue());
        }
        return ((Number) folded).doubleValue();
    }

    private static double parseDistance(String name, BytesRef distance) {
        String distanceString = distance.utf8ToString();
        try {
            return DistanceUnit.DEFAULT.parse(distanceString, DistanceUnit.DEFAULT);
        } catch (IllegalArgumentException e) {
            throw new InvalidArgumentException(e, "[{}] is not a valid distance: [{}]", name, distanceString);
        }
    }

    private long temporalOrigin(Object folded, DataType valueDataType) {
        long originValue = (Long) folded;
        if (origin.dataType() == valueDataType) {
            return originValue;
        }
        try {
            return isDateNanos(valueDataType) ? DateUtils.toNanoSeconds(originValue) : DateUtils.toMilliSeconds(originValue);
        } catch (IllegalArgumentException e) {
            throw new InvalidArgumentException(e, "[{}] cannot be converted to [{}]: {}", ORIGIN, valueDataType.typeName(), e.getMessage());
        }
    }

    private static long temporalScale(Object folded, DataType valueDataType) {
        long scaleValue = toTemporalUnits(SCALE, (Duration) folded, valueDataType);
        checkScale(scaleValue, scaleValue + temporalUnit(valueDataType));
        return scaleValue;
    }

    private static long temporalOffset(Object folded, DataType valueDataType) {
        if (folded == null) {
            return 0L;
        }
        long offsetValue = toTemporalUnits(OFFSET, (Duration) folded, valueDataType);
        checkOffset(offsetValue, offsetValue + temporalUnit(valueDataType));
        return offsetValue;
    }

    private static long toTemporalUnits(String name, Duration duration, DataType valueDataType) {
        try {
            return isDateNanos(valueDataType) ? duration.toNanos() : duration.toMillis();
        } catch (ArithmeticException e) {
            throw new InvalidArgumentException(e, "[{}] is too large for a [{}] value: [{}]", name, valueDataType.typeName(), duration);
        }
    }

    private static String temporalUnit(DataType valueDataType) {
        return isDateNanos(valueDataType) ? "ns" : "ms";
    }

    private static void checkScale(double scale, Object shown) {
        if ((scale > 0 && Double.isFinite(scale)) == false) {
            throw new InvalidArgumentException("[{}] must be a finite number greater than 0, found [{}]", SCALE, shown);
        }
    }

    private static void checkOffset(double offset, Object shown) {
        if ((offset >= 0 && Double.isFinite(offset)) == false) {
            throw new InvalidArgumentException("[{}] option must be a finite number greater than or equal to 0, found [{}]", OFFSET, shown);
        }
    }
}
