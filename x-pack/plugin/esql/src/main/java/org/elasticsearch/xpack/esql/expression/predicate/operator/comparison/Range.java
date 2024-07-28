/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

// BETWEEN or range - is a mix of gt(e) AND lt(e)
public class Range extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Range", Range::new);

    private final Expression value, lower, upper;
    private final boolean includeLower, includeUpper;
    private final ZoneId zoneId;

    public Range(Source src, Expression value, Expression lower, boolean inclLower, Expression upper, boolean inclUpper, ZoneId zoneId) {
        super(src, asList(value, lower, upper));

        this.value = value;
        this.lower = lower;
        this.upper = upper;
        this.includeLower = inclLower;
        this.includeUpper = inclUpper;
        this.zoneId = zoneId;
    }

    private Range(StreamInput in) throws IOException {
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readBoolean(),
            in.readNamedWriteable(Expression.class),
            in.readBoolean(),
            in.readZoneId()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_ADD_RANGE)) {
            source().writeTo(out);
            out.writeNamedWriteable(value);
            out.writeNamedWriteable(lower);
            out.writeBoolean(includeLower);
            out.writeNamedWriteable(upper);
            out.writeBoolean(includeUpper);
            out.writeZoneId(zoneId);
        } else {
            if (includeUpper) {
                if (includeLower) {
                    new And(source(), new GreaterThanOrEqual(source(), value, lower), new LessThanOrEqual(source(), value, upper)).writeTo(
                        out
                    );
                } else {
                    new And(source(), new GreaterThan(source(), value, lower), new LessThanOrEqual(source(), value, upper)).writeTo(out);
                }
            } else {
                if (includeLower) {
                    new And(source(), new GreaterThanOrEqual(source(), value, lower), new LessThan(source(), value, upper)).writeTo(out);
                } else {
                    new And(source(), new GreaterThan(source(), value, lower), new LessThan(source(), value, upper)).writeTo(out);
                }
            }
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Range> info() {
        return NodeInfo.create(this, Range::new, value, lower, includeLower, upper, includeUpper, zoneId);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Range(source(), newChildren.get(0), newChildren.get(1), includeLower, newChildren.get(2), includeUpper, zoneId);
    }

    public Expression value() {
        return value;
    }

    public Expression lower() {
        return lower;
    }

    public Expression upper() {
        return upper;
    }

    public boolean includeLower() {
        return includeLower;
    }

    public boolean includeUpper() {
        return includeUpper;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public boolean foldable() {
        return lower.foldable() && upper.foldable() && value.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        DataType boundaryCommonType = EsqlDataTypeRegistry.INSTANCE.commonType(lower.dataType(), upper.dataType());
        DataType commonType = EsqlDataTypeRegistry.INSTANCE.commonType(value.dataType(), boundaryCommonType);
        EvalOperator.ExpressionEvaluator.Factory v;

        if (commonType.isNumeric()) {
            v = Cast.cast(source(), value().dataType(), commonType, toEvaluator.apply(value()));
        } else {
            v = toEvaluator.apply(value());
        }

        return switch (PlannerUtils.toElementType(commonType)) {
            case BYTES_REF -> new RangeKeywordsEvaluator.Factory(
                source(),
                v,
                (BytesRef) lower.fold(),
                includeLower,
                (BytesRef) upper.fold(),
                includeUpper
            );
            case DOUBLE -> new RangeDoublesEvaluator.Factory(
                source(),
                v,
                (Double) EsqlDataTypeConverter.convert(lower.fold(), DOUBLE),
                includeLower,
                (Double) EsqlDataTypeConverter.convert(upper.fold(), DOUBLE),
                includeUpper
            );
            case INT -> new RangeIntsEvaluator.Factory(
                source(),
                v,
                (Integer) EsqlDataTypeConverter.convert(lower.fold(), INTEGER),
                includeLower,
                (Integer) EsqlDataTypeConverter.convert(upper.fold(), INTEGER),
                includeUpper
            );
            case LONG -> new RangeLongsEvaluator.Factory(
                source(),
                v,
                (Long) EsqlDataTypeConverter.convert(lower.fold(), LONG),
                includeLower,
                (Long) EsqlDataTypeConverter.convert(upper.fold(), LONG),
                includeUpper
            );
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(value.dataType());
        };
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * Check whether the boundaries are invalid ( upper &lt; lower) or not.
     * If they are, the value does not have to be evaluated.
     */
    public static boolean areBoundariesInvalid(Object lowerValue, boolean includeLower, Object upperValue, boolean includeUpper) {
        Integer compare = BinaryComparison.compare(lowerValue, upperValue);
        // upper < lower OR upper == lower and the range doesn't contain any equals
        return compare != null && (compare > 0 || (compare == 0 && (includeLower == false || includeUpper == false)));
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeLower, includeUpper, value, lower, upper, zoneId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Range other = (Range) obj;
        return Objects.equals(includeLower, other.includeLower)
            && Objects.equals(includeUpper, other.includeUpper)
            && Objects.equals(value, other.value)
            && Objects.equals(lower, other.lower)
            && Objects.equals(upper, other.upper)
            && Objects.equals(zoneId, other.zoneId);
    }

    @Evaluator(extraName = "Ints")
    static boolean processInts(int field, @Fixed int lower, @Fixed boolean includeLower, @Fixed int upper, @Fixed boolean includeUpper) {
        if (areBoundariesInvalid(lower, includeLower, upper, includeUpper)) {
            return false;
        }
        if (includeLower) {
            if (includeUpper) {
                return field >= lower && field <= upper;
            } else {
                return field >= lower && field < upper;
            }
        } else {
            if (includeUpper) {
                return field > lower && field <= upper;
            } else {
                return field > lower && field < upper;
            }
        }
    }

    @Evaluator(extraName = "Longs")
    static boolean processLongs(
        long field,
        @Fixed long lower,
        @Fixed boolean includeLower,
        @Fixed long upper,
        @Fixed boolean includeUpper
    ) {
        if (areBoundariesInvalid(lower, includeLower, upper, includeUpper)) {
            return false;
        }
        if (includeLower) {
            if (includeUpper) {
                return field >= lower && field <= upper;
            } else {
                return field >= lower && field < upper;
            }
        } else {
            if (includeUpper) {
                return field > lower && field <= upper;
            } else {
                return field > lower && field < upper;
            }
        }
    }

    @Evaluator(extraName = "Doubles")
    static boolean processDoubles(
        double field,
        @Fixed double lower,
        @Fixed boolean includeLower,
        @Fixed double upper,
        @Fixed boolean includeUpper
    ) {
        if (areBoundariesInvalid(lower, includeLower, upper, includeUpper)) {
            return false;
        }
        if (includeLower) {
            if (includeUpper) {
                return field >= lower && field <= upper;
            } else {
                return field >= lower && field < upper;
            }
        } else {
            if (includeUpper) {
                return field > lower && field <= upper;
            } else {
                return field > lower && field < upper;
            }
        }
    }

    @Evaluator(extraName = "Keywords")
    static boolean processKeywords(
        BytesRef field,
        @Fixed BytesRef lower,
        @Fixed boolean includeLower,
        @Fixed BytesRef upper,
        @Fixed boolean includeUpper
    ) {
        if (areBoundariesInvalid(lower, includeLower, upper, includeUpper)) {
            return false;
        }
        Integer lowerCompare = BinaryComparison.compare(lower, field);
        Integer upperCompare = BinaryComparison.compare(field, upper);
        boolean lowerComparsion = lowerCompare == null ? false : (includeLower ? lowerCompare <= 0 : lowerCompare < 0);
        boolean upperComparsion = upperCompare == null ? false : (includeUpper ? upperCompare <= 0 : upperCompare < 0);
        return lowerComparsion && upperComparsion;
    }
}
