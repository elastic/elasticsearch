/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll;
import org.elasticsearch.xpack.esql.core.querydsl.query.NotQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.Foldables.valueOf;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.HOUR_MINUTE_SECOND;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.commonType;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateWithTypeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;

public abstract class EsqlBinaryComparison extends BinaryComparison
    implements
        EvaluatorMapper,
        TranslationAware.SingleValueTranslationAware {

    private static final Logger logger = LogManager.getLogger(EsqlBinaryComparison.class);

    private final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap;

    private final BinaryComparisonOperation functionType;
    private final EsqlArithmeticOperation.BinaryEvaluator nanosToMillisEvaluator;
    private final EsqlArithmeticOperation.BinaryEvaluator millisToNanosEvaluator;

    @FunctionalInterface
    public interface BinaryOperatorConstructor {
        EsqlBinaryComparison apply(Source source, Expression lhs, Expression rhs);
    }

    public enum BinaryComparisonOperation implements Writeable {

        EQ(0, "==", org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.EQ, Equals::new),
        // id 1 reserved for NullEquals
        NEQ(
            2,
            "!=",
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.NEQ,
            NotEquals::new
        ),
        GT(
            3,
            ">",
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.GT,
            GreaterThan::new
        ),
        GTE(
            4,
            ">=",
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.GTE,
            GreaterThanOrEqual::new
        ),
        LT(5, "<", org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.LT, LessThan::new),
        LTE(
            6,
            "<=",
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.LTE,
            LessThanOrEqual::new
        );

        private final int id;
        private final String symbol;
        // Temporary mapping to the old enum, to satisfy the superclass constructor signature.
        private final org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation shim;
        private final BinaryOperatorConstructor constructor;

        BinaryComparisonOperation(
            int id,
            String symbol,
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation shim,
            BinaryOperatorConstructor constructor
        ) {
            this.id = id;
            this.symbol = symbol;
            this.shim = shim;
            this.constructor = constructor;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(id);
        }

        public static BinaryComparisonOperation readFromStream(StreamInput in) throws IOException {
            int id = in.readVInt();
            for (BinaryComparisonOperation op : values()) {
                if (op.id == id) {
                    return op;
                }
            }
            throw new IOException("No BinaryComparisonOperation found for id [" + id + "]");
        }

        public String symbol() {
            return symbol;
        }

        public EsqlBinaryComparison buildNewInstance(Source source, Expression lhs, Expression rhs) {
            return constructor.apply(source, lhs, rhs);
        }
    }

    protected EsqlBinaryComparison(
        Source source,
        Expression left,
        Expression right,
        BinaryComparisonOperation operation,
        Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap,
        EsqlArithmeticOperation.BinaryEvaluator nanosToMillisEvaluator,
        EsqlArithmeticOperation.BinaryEvaluator millisToNanosEvaluator
    ) {
        this(source, left, right, operation, null, evaluatorMap, nanosToMillisEvaluator, millisToNanosEvaluator);
    }

    protected EsqlBinaryComparison(
        Source source,
        Expression left,
        Expression right,
        BinaryComparisonOperation operation,
        // TODO: We are definitely not doing the right thing with this zoneId
        ZoneId zoneId,
        Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap,
        EsqlArithmeticOperation.BinaryEvaluator nanosToMillisEvaluator,
        EsqlArithmeticOperation.BinaryEvaluator millisToNanosEvaluator
    ) {
        super(source, left, right, operation.shim, zoneId);
        this.evaluatorMap = evaluatorMap;
        this.functionType = operation;
        this.nanosToMillisEvaluator = nanosToMillisEvaluator;
        this.millisToNanosEvaluator = millisToNanosEvaluator;
    }

    public static EsqlBinaryComparison readFrom(StreamInput in) throws IOException {
        // TODO this uses a constructor on the operation *and* a name which is confusing. It only needs one. Everything else uses a name.
        var source = Source.readFrom((PlanStreamInput) in);
        EsqlBinaryComparison.BinaryComparisonOperation operation = EsqlBinaryComparison.BinaryComparisonOperation.readFromStream(in);
        var left = in.readNamedWriteable(Expression.class);
        var right = in.readNamedWriteable(Expression.class);
        // TODO: Remove zoneId entirely
        var zoneId = in.readOptionalZoneId();
        return operation.buildNewInstance(source, left, right);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        functionType.writeTo(out);
        out.writeNamedWriteable(left());
        out.writeNamedWriteable(right());
        out.writeOptionalZoneId(zoneId());
    }

    public BinaryComparisonOperation getFunctionType() {
        return functionType;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        EvalOperator.ExpressionEvaluator.Factory lhs;
        EvalOperator.ExpressionEvaluator.Factory rhs;

        // Special cases for mixed nanosecond and millisecond comparisions
        if (left().dataType() == DataType.DATE_NANOS && right().dataType() == DataType.DATETIME) {
            lhs = toEvaluator.apply(left());
            rhs = toEvaluator.apply(right());
            return nanosToMillisEvaluator.apply(source(), lhs, rhs);
        }

        if (left().dataType() == DataType.DATETIME && right().dataType() == DataType.DATE_NANOS) {
            lhs = toEvaluator.apply(left());
            rhs = toEvaluator.apply(right());
            return millisToNanosEvaluator.apply(source(), lhs, rhs);
        }

        // Our type is always boolean, so figure out the evaluator type from the inputs
        DataType commonType = commonType(left().dataType(), right().dataType());
        if (commonType.isNumeric()) {
            lhs = Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left()));
            rhs = Cast.cast(source(), right().dataType(), commonType, toEvaluator.apply(right()));
        } else {
            lhs = toEvaluator.apply(left());
            rhs = toEvaluator.apply(right());
        }

        if (evaluatorMap.containsKey(commonType) == false) {
            throw new EsqlIllegalArgumentException("Unsupported type " + left().dataType());
        }
        return evaluatorMap.get(commonType).apply(source(), lhs, rhs);
    }

    @Override
    public Boolean fold(FoldContext ctx) {
        return (Boolean) EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution typeResolution = super.resolveType();
        if (typeResolution.unresolved()) {
            return typeResolution;
        }

        return checkCompatibility();
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, TypeResolutions.ParamOrdinal paramOrdinal) {
        return TypeResolutions.isType(
            e,
            evaluatorMap::containsKey,
            sourceText(),
            paramOrdinal,
            evaluatorMap.keySet().stream().map(DataType::typeName).sorted().toArray(String[]::new)
        );
    }

    /**
     * Check if the two input types are compatible for this operation.
     * NOTE: this method should be consistent with
     * {@link org.elasticsearch.xpack.esql.analysis.Verifier#validateBinaryComparison(BinaryComparison)}
     *
     * @return TypeResolution.TYPE_RESOLVED iff the types are compatible.  Otherwise, an appropriate type resolution error.
     */
    protected TypeResolution checkCompatibility() {
        DataType leftType = left().dataType();
        DataType rightType = right().dataType();

        // Unsigned long is only interoperable with other unsigned longs
        if ((rightType == UNSIGNED_LONG && (false == (leftType == UNSIGNED_LONG || leftType == DataType.NULL)))
            || (leftType == UNSIGNED_LONG && (false == (rightType == UNSIGNED_LONG || rightType == DataType.NULL)))) {
            return new TypeResolution(formatIncompatibleTypesMessage(left().dataType(), right().dataType(), sourceText()));
        }

        if ((leftType.isNumeric() && rightType.isNumeric())
            || (DataType.isString(leftType) && DataType.isString(rightType))
            || (leftType.isDate() && rightType.isDate()) // Millis and Nanos
            || leftType.equals(rightType)
            || DataType.isNull(leftType)
            || DataType.isNull(rightType)) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return new TypeResolution(formatIncompatibleTypesMessage(left().dataType(), right().dataType(), sourceText()));
    }

    public static String formatIncompatibleTypesMessage(DataType leftType, DataType rightType, String sourceText) {
        if (leftType.equals(UNSIGNED_LONG)) {
            return format(
                null,
                "first argument of [{}] is [unsigned_long] and second is [{}]. "
                    + "[unsigned_long] can only be operated on together with another [unsigned_long]",
                sourceText,
                rightType.typeName()
            );
        }
        if (rightType.equals(UNSIGNED_LONG)) {
            return format(
                null,
                "first argument of [{}] is [{}] and second is [unsigned_long]. "
                    + "[unsigned_long] can only be operated on together with another [unsigned_long]",
                sourceText,
                leftType.typeName()
            );
        }
        return format(
            null,
            "first argument of [{}] is [{}] so second argument must also be [{}] but was [{}]",
            sourceText,
            leftType.isNumeric() ? "numeric" : leftType.typeName(),
            leftType.isNumeric() ? "numeric" : leftType.typeName(),
            rightType.typeName()
        );
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        if (right().foldable()) {
            if (pushdownPredicates.isPushableFieldAttribute(left())) {
                return Translatable.YES;
            }
            if (LucenePushdownPredicates.isPushableMetadataAttribute(left())) {
                return this instanceof Equals || this instanceof NotEquals ? Translatable.YES : Translatable.NO;
            }
        }
        return Translatable.NO;
    }

    /**
     * This method is responsible for pushing the ES|QL Binary Comparison operators into Lucene.  It covers:
     *  <ul>
     *      <li>{@link Equals}</li>
     *      <li>{@link NotEquals}</li>
     *      <li>{@link GreaterThanOrEqual}</li>
     *      <li>{@link GreaterThan}</li>
     *      <li>{@link LessThanOrEqual}</li>
     *      <li>{@link LessThan}</li>
     *  </ul>
     *
     *  In general, we are able to push these down when one of the arguments is a constant (i.e. is foldable).  This class assumes
     *  that an earlier pass through the query has rearranged things so that the foldable value will be the right hand side
     *  input to the operation.
     */
    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        Check.isTrue(
            right().foldable(),
            "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [{}]",
            right().sourceLocation().getLineNumber(),
            right().sourceLocation().getColumnNumber(),
            Expressions.name(right()),
            symbol()
        );

        Query translated = translateOutOfRangeComparisons();
        return translated != null ? translated : translate(handler);
    }

    @Override
    public Expression singleValueField() {
        return left();
    }

    private Query translate(TranslatorHandler handler) {
        TypedAttribute attribute = LucenePushdownPredicates.checkIsPushableAttribute(left());
        String name = handler.nameOf(attribute);
        Object value = valueOf(FoldContext.small() /* TODO remove me */, right());
        String format = null;
        boolean isDateLiteralComparison = false;

        logger.trace(
            "Translating binary comparison with right: [{}<{}>], left: [{}<{}>], attribute: [{}<{}>]",
            right(),
            right().dataType(),
            left(),
            left().dataType(),
            attribute,
            attribute.dataType()
        );

        // TODO: This type coersion layer is copied directly from the QL counterpart code. It's probably not necessary or desireable
        // in the ESQL version. We should instead do the type conversions using our casting functions.
        // for a date constant comparison, we need to use a format for the date, to make sure that the format is the same
        // no matter the timezone provided by the user
        if (value instanceof ZonedDateTime || value instanceof OffsetTime) {
            DateFormatter formatter;
            if (value instanceof ZonedDateTime) {
                // NB: we check the data type of right here because value is the RHS value
                formatter = switch (right().dataType()) {
                    case DATETIME -> DEFAULT_DATE_TIME_FORMATTER;
                    case DATE_NANOS -> DEFAULT_DATE_NANOS_FORMATTER;
                    default -> throw new EsqlIllegalArgumentException("Found date value in non-date type comparison");
                };
                // RangeQueryBuilder accepts an Object as its parameter, but it will call .toString() on the ZonedDateTime instance
                // which can have a slightly different format depending on the ZoneId used to create the ZonedDateTime
                // Since RangeQueryBuilder can handle date as String as well, we'll format it as String and provide the format as well.
                value = formatter.format((ZonedDateTime) value);
            } else {
                formatter = HOUR_MINUTE_SECOND;
                value = formatter.format((OffsetTime) value);
            }
            format = formatter.pattern();
            isDateLiteralComparison = true;
        } else if (attribute.dataType() == IP && value instanceof BytesRef bytesRef) {
            value = ipToString(bytesRef);
        } else if (attribute.dataType() == VERSION) {
            // VersionStringFieldMapper#indexedValueForSearch() only accepts as input String or BytesRef with the String (i.e. not
            // encoded) representation of the version as it'll do the encoding itself.
            if (value instanceof BytesRef bytesRef) {
                value = versionToString(bytesRef);
            } else if (value instanceof Version version) {
                value = versionToString(version);
            }
        } else if (attribute.dataType() == UNSIGNED_LONG && value instanceof Long ul) {
            value = unsignedLongAsNumber(ul);
        }

        ZoneId zoneId = null;
        if (attribute.dataType() == DATETIME) {
            zoneId = zoneId();
            value = dateWithTypeToString((Long) value, right().dataType());
            format = DEFAULT_DATE_TIME_FORMATTER.pattern();
        } else if (attribute.dataType() == DATE_NANOS) {
            zoneId = zoneId();
            value = dateWithTypeToString((Long) value, right().dataType());
            format = DEFAULT_DATE_NANOS_FORMATTER.pattern();
        }
        if (this instanceof GreaterThan) {
            return new RangeQuery(source(), name, value, false, null, false, format, zoneId);
        }
        if (this instanceof GreaterThanOrEqual) {
            return new RangeQuery(source(), name, value, true, null, false, format, zoneId);
        }
        if (this instanceof LessThan) {
            return new RangeQuery(source(), name, null, false, value, false, format, zoneId);
        }
        if (this instanceof LessThanOrEqual) {
            return new RangeQuery(source(), name, null, false, value, true, format, zoneId);
        }
        if (this instanceof Equals || this instanceof NotEquals) {
            name = LucenePushdownPredicates.pushableAttributeName(attribute);

            Query query;
            if (isDateLiteralComparison) {
                // dates equality uses a range query because it's the one that has a "format" parameter
                query = new RangeQuery(source(), name, value, true, value, true, format, zoneId);
            } else {
                query = new TermQuery(source(), name, value);
            }
            if (this instanceof NotEquals) {
                query = new NotQuery(source(), query);
            }
            return query;
        }

        throw new QlIllegalArgumentException(
            "Don't know how to translate binary comparison [{}] in [{}]",
            right().nodeString(),
            toString()
        );
    }

    private Query translateOutOfRangeComparisons() {
        if ((left() instanceof FieldAttribute) == false || left().dataType().isNumeric() == false) {
            return null;
        }
        Object value = valueOf(FoldContext.small() /* TODO remove me */, right());

        // Comparisons with multi-values always return null in ESQL.
        if (value instanceof List<?>) {
            return new MatchAll(source()).negate(source());
        }

        DataType valueType = right().dataType();
        DataType attributeDataType = left().dataType();
        if (valueType == UNSIGNED_LONG && value instanceof Long ul) {
            value = unsignedLongAsNumber(ul);
        }
        Number num = (Number) value;
        if (isInRange(attributeDataType, valueType, num)) {
            return null;
        }

        if (Double.isNaN(((Number) value).doubleValue())) {
            return new MatchAll(source()).negate(source());
        }

        boolean matchAllOrNone;
        if (this instanceof GreaterThan || this instanceof GreaterThanOrEqual) {
            matchAllOrNone = (num.doubleValue() > 0) == false;
        } else if (this instanceof LessThan || this instanceof LessThanOrEqual) {
            matchAllOrNone = (num.doubleValue() > 0);
        } else if (this instanceof Equals) {
            matchAllOrNone = false;
        } else if (this instanceof NotEquals) {
            matchAllOrNone = true;
        } else {
            throw new QlIllegalArgumentException("Unknown binary comparison [{}]", toString());
        }

        return matchAllOrNone ? new MatchAll(source()) : new MatchAll(source()).negate(source());
    }

    private static final BigDecimal HALF_FLOAT_MAX = BigDecimal.valueOf(65504);
    private static final BigDecimal UNSIGNED_LONG_MAX = BigDecimal.valueOf(2).pow(64).subtract(BigDecimal.ONE);

    private static boolean isInRange(DataType numericFieldDataType, DataType valueDataType, Number value) {
        double doubleValue = value.doubleValue();
        if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
            return false;
        }

        BigDecimal decimalValue;
        if (value instanceof BigInteger bigIntValue) {
            // Unsigned longs may be represented as BigInteger.
            decimalValue = new BigDecimal(bigIntValue);
        } else {
            decimalValue = valueDataType.isRationalNumber() ? BigDecimal.valueOf(doubleValue) : BigDecimal.valueOf(value.longValue());
        }

        // Determine min/max for dataType. Use BigDecimals as doubles will have rounding errors for long/ulong.
        BigDecimal minValue;
        BigDecimal maxValue;
        if (numericFieldDataType == DataType.BYTE) {
            minValue = BigDecimal.valueOf(Byte.MIN_VALUE);
            maxValue = BigDecimal.valueOf(Byte.MAX_VALUE);
        } else if (numericFieldDataType == DataType.SHORT) {
            minValue = BigDecimal.valueOf(Short.MIN_VALUE);
            maxValue = BigDecimal.valueOf(Short.MAX_VALUE);
        } else if (numericFieldDataType == DataType.INTEGER) {
            minValue = BigDecimal.valueOf(Integer.MIN_VALUE);
            maxValue = BigDecimal.valueOf(Integer.MAX_VALUE);
        } else if (numericFieldDataType == DataType.LONG) {
            minValue = BigDecimal.valueOf(Long.MIN_VALUE);
            maxValue = BigDecimal.valueOf(Long.MAX_VALUE);
        } else if (numericFieldDataType == DataType.UNSIGNED_LONG) {
            minValue = BigDecimal.ZERO;
            maxValue = UNSIGNED_LONG_MAX;
        } else if (numericFieldDataType == DataType.HALF_FLOAT) {
            minValue = HALF_FLOAT_MAX.negate();
            maxValue = HALF_FLOAT_MAX;
        } else if (numericFieldDataType == DataType.FLOAT) {
            minValue = BigDecimal.valueOf(-Float.MAX_VALUE);
            maxValue = BigDecimal.valueOf(Float.MAX_VALUE);
        } else if (numericFieldDataType == DataType.DOUBLE || numericFieldDataType == DataType.SCALED_FLOAT) {
            // Scaled floats are represented as doubles in ESQL.
            minValue = BigDecimal.valueOf(-Double.MAX_VALUE);
            maxValue = BigDecimal.valueOf(Double.MAX_VALUE);
        } else {
            throw new QlIllegalArgumentException("Data type [{}] unsupported for numeric range check", numericFieldDataType);
        }

        return minValue.compareTo(decimalValue) <= 0 && maxValue.compareTo(decimalValue) >= 0;
    }
}
