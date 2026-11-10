/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.score;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
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
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayScorers.CartesianScorer;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayScorers.GeoScorer;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayScorers.NumericScorer;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayScorers.TemporalScorer;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.time.Duration;
import java.time.Period;
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
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
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
 * This function uses the same formulas as the {@link ScoreScriptUtils} classes Painless scripts use,
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
        .capabilities("ga_fixes", "date_period")
        .name("decay");

    public static final String ORIGIN = "origin";
    public static final String SCALE = "scale";
    public static final String OFFSET = "offset";
    public static final String DECAY = "decay";
    public static final String TYPE = "type";

    private static final Map<String, Collection<DataType>> ALLOWED_OPTIONS = Map.of(
        OFFSET,
        Set.of(TIME_DURATION, DATE_PERIOD, INTEGER, LONG, DOUBLE, KEYWORD, TEXT, UNSIGNED_LONG),
        DECAY,
        Set.of(DOUBLE),
        TYPE,
        Set.of(KEYWORD)
    );

    private static final String DEFAULT_GEO_POINT_OFFSET = "0m";

    private static final double DEFAULT_DECAY = 0.5;

    private static final BytesRef DEFAULT_FUNCTION = new BytesRef("linear");

    private final Expression origin;
    private final Expression value;
    private final Expression scale;
    private final Expression options;

    private final Map<String, Object> resolvedOptions;

    @FunctionInfo(
        returnType = "double",
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.7.0") },
        briefSummary = "Calculates a relevance score that decays with distance from a target origin.",
        description = "Calculates a relevance score that decays based on the distance of a numeric, spatial or date type value "
            + "from a target origin, using configurable decay functions.",
        detailedDescription = """
            `DECAY` calculates a score between 0 and 1 based on how far a value is from an origin (called distance).
            The distance is numeric, spatial or temporal depending on the type of the value.
            Up to `offset` from the origin the score is 1. At `offset` plus `scale` the score equals `decay`,
            and further away it keeps decreasing towards 0 along the `linear`, `exp` or `gauss` curve.

            `DECAY` uses <<esql-function-named-params,function named parameters>> for the optional `type` (defaults to `linear`),
            `decay` (defaults to `0.5`) and `offset` (defaults to `0`). `origin` and `scale` must be constants.
            `scale` must be greater than 0, `decay` must be greater than 0 and less than 1, and `offset` must not be negative.
            Any other value is an error.

            The unit of `scale` and `offset` depends on the type of the value:

            * numeric values and `cartesian_point` use numbers.
            * `geo_point` uses distance strings such as `"200km"` or `"5mi"`. The origin is a point such as `TO_GEOPOINT("POINT(lon lat)")`.
            * `date` and `date_nanos` use time durations such as `12 hours`, or periods of days and weeks such as `7 days`.
              Months and years have no fixed length and are rejected.

            A `null` value scores `null`. A multivalued value scores `null` and adds a warning.
            To score the nearest of several values, `MV_EXPAND` the field and take the `MAX` of `DECAY` for each document.

            To boost relevance, combine `DECAY` with `_score`, for example `_score * DECAY(...)`, `_score + DECAY(...)`
            or `GREATEST(_score, DECAY(...))`. Use `COALESCE(DECAY(...), 0.5)` to give documents without the field a fixed score.

            Compared to the decay functions of the `function_score` query, a missing value scores `null` instead of 1,
            there is no `multi_value_mode`, and `origin` has no default of `now`.

            :::{tip}
            Learn more about using [ES|QL for search use cases](docs-content://solutions/search/esql-for-search.md).
            :::
            """,
        examples = {
            @Example(file = "decay", tag = "decay"),
            @Example(
                description = "Boost the relevance score of recent books, giving books without a year half the score:",
                file = "decay",
                tag = "decay-score"
            ) }
    )
    public Decay(
        Source source,
        @Param(
            name = "value",
            type = { "double", "integer", "long", "unsigned_long", "date", "date_nanos", "geo_point", "cartesian_point" },
            description = "The value to score. A `null` value scores `null`, and a multivalued value scores `null` with a warning."
        ) Expression value,
        @Param(
            name = ORIGIN,
            type = { "double", "integer", "long", "unsigned_long", "date", "date_nanos", "geo_point", "cartesian_point" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Central point from which the distances are calculated. Must be a constant."
        ) Expression origin,
        @Param(
            name = SCALE,
            type = { "double", "integer", "long", "unsigned_long", "time_duration", "date_period", "keyword", "text" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Distance beyond `offset` at which the score equals `decay`. Must be a constant greater than 0."
        ) Expression scale,
        @MapParam(
            name = "options",
            description = "(Optional) Additional options such as `decay`, `offset` and `type`.",
            params = {
                @MapParam.MapParamEntry(
                    name = OFFSET,
                    type = { "double", "integer", "long", "unsigned_long", "time_duration", "date_period", "keyword", "text" },
                    description = "Distance from the origin within which the score is 1. Must not be negative. Defaults to 0."
                ),
                @MapParam.MapParamEntry(
                    name = DECAY,
                    type = { "double" },
                    description = "Score at `scale` beyond `offset`. Must be greater than 0 and less than 1. Defaults to 0.5."
                ),
                @MapParam.MapParamEntry(
                    name = TYPE,
                    type = { "keyword" },
                    description = "Decay function to use: `linear`, `exp`, or `gauss`. Defaults to `linear`."
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
                DataType::isTemporalAmount,
                "time_duration or date_period",
                DataType::isTemporalAmount,
                "time_duration or date_period"
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
                numericScorer(decayFunction, originFolded, scaleFolded, offsetExpr, offsetFolded, decayFolded)
            );
            case DOUBLE -> new DecayDoubleEvaluator.Factory(
                source(),
                valueFactory,
                numericScorer(decayFunction, originFolded, scaleFolded, offsetExpr, offsetFolded, decayFolded)
            );
            case LONG -> new DecayLongEvaluator.Factory(
                source(),
                valueFactory,
                numericScorer(decayFunction, originFolded, scaleFolded, offsetExpr, offsetFolded, decayFolded)
            );
            case UNSIGNED_LONG -> new DecayUnsignedLongEvaluator.Factory(
                source(),
                valueFactory,
                numericScorer(decayFunction, originFolded, scaleFolded, offsetExpr, offsetFolded, decayFolded)
            );
            case GEO_POINT -> {
                String scaleString = BytesRefs.toString(scaleFolded);
                String offsetString = offsetExpr != null ? BytesRefs.toString(offsetFolded) : DEFAULT_GEO_POINT_OFFSET;
                double geoScale = parseDistance(SCALE, scaleString);
                checkScale(geoScale, scaleString);
                double geoOffset = parseDistance(OFFSET, offsetString);
                checkOffset(geoOffset, offsetString);
                yield new DecayGeoPointEvaluator.Factory(
                    source(),
                    valueFactory,
                    GeoScorer.of(decayFunction, (BytesRef) originFolded, geoScale, geoOffset, decayFolded)
                );
            }
            case CARTESIAN_POINT -> new DecayCartesianPointEvaluator.Factory(
                source(),
                valueFactory,
                CartesianScorer.of(
                    decayFunction,
                    (BytesRef) originFolded,
                    numericScale(scaleFolded),
                    numericOffset(offsetExpr, offsetFolded),
                    decayFolded
                )
            );
            case DATETIME -> new DecayDatetimeEvaluator.Factory(
                source(),
                valueFactory,
                temporalScorer(decayFunction, originFolded, scaleFolded, offsetFolded, decayFolded, valueDataType)
            );
            case DATE_NANOS -> new DecayDateNanosEvaluator.Factory(
                source(),
                valueFactory,
                temporalScorer(decayFunction, originFolded, scaleFolded, offsetFolded, decayFolded, valueDataType)
            );
            default -> throw new UnsupportedOperationException("Unsupported data type: " + valueDataType);
        };
    }

    private NumericScorer numericScorer(
        DecayFunction decayFunction,
        Object originFolded,
        Object scaleFolded,
        Expression offsetExpr,
        Object offsetFolded,
        double decay
    ) {
        return NumericScorer.of(
            decayFunction,
            toDoubleParam(originFolded, origin.dataType()),
            numericScale(scaleFolded),
            numericOffset(offsetExpr, offsetFolded),
            decay
        );
    }

    private TemporalScorer temporalScorer(
        DecayFunction decayFunction,
        Object originFolded,
        Object scaleFolded,
        Object offsetFolded,
        double decay,
        DataType valueDataType
    ) {
        return TemporalScorer.of(
            decayFunction,
            temporalOrigin(originFolded, valueDataType),
            temporalScale(scaleFolded, valueDataType),
            temporalOffset(offsetFolded, valueDataType),
            decay
        );
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
    static double process(int value, @Fixed NumericScorer scorer) {
        return scorer.score(value);
    }

    @Evaluator(extraName = "Double")
    static double process(double value, @Fixed NumericScorer scorer) {
        return scorer.score(value);
    }

    @Evaluator(extraName = "Long")
    static double process(long value, @Fixed NumericScorer scorer) {
        return scorer.score(value);
    }

    @Evaluator(extraName = "UnsignedLong")
    static double processUnsignedLong(long value, @Fixed NumericScorer scorer) {
        // unsigned_long values are stored as longs offset by Long.MIN_VALUE; decode to their
        // true magnitude as a double before feeding the score-script math, which operates in double space.
        return scorer.score(NumericUtils.unsignedLongToDouble(value));
    }

    @Evaluator(extraName = "GeoPoint")
    static double process(BytesRef value, @Fixed GeoScorer scorer) {
        return scorer.score(value);
    }

    @Evaluator(extraName = "CartesianPoint")
    static double processCartesianPoint(BytesRef value, @Fixed CartesianScorer scorer) {
        return scorer.score(value);
    }

    @Evaluator(extraName = "Datetime")
    static double processDatetime(long value, @Fixed TemporalScorer scorer) {
        return scorer.score(value);
    }

    @Evaluator(extraName = "DateNanos")
    static double processDateNanos(long value, @Fixed TemporalScorer scorer) {
        return scorer.score(value);
    }

    public enum DecayFunction {
        LINEAR("linear"),
        EXPONENTIAL("exp"),
        GAUSSIAN("gauss");

        private final String functionName;
        private static final Map<String, DecayFunction> BY_NAME = Arrays.stream(values())
            .collect(Collectors.toMap(df -> df.functionName, df -> df));

        DecayFunction(String functionName) {
            this.functionName = functionName;
        }

        public static DecayFunction fromBytesRef(BytesRef functionType) {
            String name = functionType.utf8ToString().toLowerCase(Locale.ROOT);
            DecayFunction decayFunction = BY_NAME.get(name);
            if (decayFunction == null) {
                throw new InvalidArgumentException("unknown decay type [{}], expected one of [gauss, linear, exp]", name);
            }
            return decayFunction;
        }

        /**
         * The constant {@link #score} needs, derived once from the scale and the decay.
         */
        double scaling(double scale, double decay) {
            return switch (this) {
                case LINEAR -> scale / (1.0 - decay);
                case EXPONENTIAL -> Math.log(decay) / scale;
                case GAUSSIAN -> 0.5 * Math.pow(scale, 2.0) / Math.log(decay);
            };
        }

        /**
         * The score at {@code distance} past the offset, given the {@link #scaling} for this query.
         */
        double score(double scaling, double distance) {
            return switch (this) {
                case LINEAR -> Math.max(0.0, (scaling - distance) / scaling);
                case EXPONENTIAL -> Math.exp(scaling * distance);
                case GAUSSIAN -> Math.exp(0.5 * Math.pow(distance, 2.0) / scaling);
            };
        }
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

    private static double parseDistance(String name, String distance) {
        try {
            return DistanceUnit.DEFAULT.parse(distance, DistanceUnit.DEFAULT);
        } catch (IllegalArgumentException e) {
            throw new InvalidArgumentException(e, "[{}] is not a valid distance: [{}]", name, distance);
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
        long scaleValue = toTemporalUnits(SCALE, toDuration(SCALE, folded), valueDataType);
        checkScale(scaleValue, scaleValue + temporalUnit(valueDataType));
        return scaleValue;
    }

    private static long temporalOffset(Object folded, DataType valueDataType) {
        if (folded == null) {
            return 0L;
        }
        long offsetValue = toTemporalUnits(OFFSET, toDuration(OFFSET, folded), valueDataType);
        checkOffset(offsetValue, offsetValue + temporalUnit(valueDataType));
        return offsetValue;
    }

    /**
     * Months and years have no fixed length, so only periods made of days (weeks are days) are accepted.
     */
    private static Duration toDuration(String name, Object folded) {
        if (folded instanceof Period period) {
            if (period.getYears() != 0 || period.getMonths() != 0) {
                throw new InvalidArgumentException(
                    "[{}] must not contain months or years, use days or a time duration: [{}]",
                    name,
                    period
                );
            }
            return Duration.ofDays(period.getDays());
        }
        return (Duration) folded;
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
