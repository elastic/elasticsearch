/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.expression.Foldables.valueOf;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.asDateTime;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateWithTypeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;

// BETWEEN or range - is a mix of gt(e) AND lt(e)
public class Range extends ScalarFunction implements TranslationAware.SingleValueTranslationAware {
    private static final Logger logger = LogManager.getLogger(Range.class);

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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
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

    /**
     * In case that the range is empty due to foldable, invalid bounds, but the bounds themselves are not yet folded, the optimizer will
     * need two passes to fold this.
     * That's because we shouldn't perform folding when trying to determine foldability.
     */
    @Override
    public boolean foldable() {
        // NB: this is likely dead code. See note in areBoundariesInvalid
        if (lower.foldable() && upper.foldable()) {
            if (value().foldable()) {
                return true;
            }

            // We cannot fold the bounds here; but if they're already literals, we can check if the range is always empty.
            if (lower() instanceof Literal l && upper() instanceof Literal u) {
                return areBoundariesInvalid(l.value(), u.value());
            }
        }
        return false;
    }

    @Override
    public Object fold(FoldContext ctx) {
        // NB: this is likely dead code. See note in areBoundariesInvalid
        Object lowerValue = lower.fold(ctx);
        Object upperValue = upper.fold(ctx);
        if (areBoundariesInvalid(lowerValue, upperValue)) {
            return Boolean.FALSE;
        }

        Object val = value.fold(ctx);
        Integer lowerCompare = BinaryComparison.compare(lower.fold(ctx), val);
        Integer upperCompare = BinaryComparison.compare(val, upper().fold(ctx));
        boolean lowerComparsion = lowerCompare == null ? false : (includeLower ? lowerCompare <= 0 : lowerCompare < 0);
        boolean upperComparsion = upperCompare == null ? false : (includeUpper ? upperCompare <= 0 : upperCompare < 0);
        return lowerComparsion && upperComparsion;
    }

    /**
     * Check whether the boundaries are invalid ( upper &lt; lower) or not.
     * If they are, the value does not have to be evaluated.
     */
    protected boolean areBoundariesInvalid(Object lowerValue, Object upperValue) {
        /*
        NB: I am reasonably sure this code is dead.  It can only be reached from foldable(), and as far as I can tell
        we never fold ranges. There's no ES|QL syntax for ranges, so they can never be created by the parser.  The
        PropagateEquals optimizer rule can in theory create ranges, but only from existing ranges.  The fact that this
        class is not serializable (note that writeTo throws UnsupportedOperationException) is a clear indicator that
        logical planning cannot output Range nodes.

        PushFiltersToSource can also create ranges, but that is a Physical optimizer rule. Folding happens in the
        Logical optimization layer, and should be done by the time we are calling PushFiltersToSource.

        That said, if somehow you have arrived here while debugging something, know that this method is likely
        completely broken for date_nanos, and possibly other types.
         */
        if (DataType.isDateTime(value.dataType()) || DataType.isDateTime(lower.dataType()) || DataType.isDateTime(upper.dataType())) {
            try {
                if (upperValue instanceof String upperString) {
                    upperValue = asDateTime(upperString);
                }
                if (lowerValue instanceof String lowerString) {
                    lowerValue = asDateTime(lowerString);
                }
            } catch (DateTimeException e) {
                // one of the patterns is not a normal date, it could be a date math expression
                // that has to be evaluated at lower level.
                return false;
            }
            // for all the other cases, normal BinaryComparison logic is sufficient
        }

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

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return pushdownPredicates.isPushableAttribute(value) && lower.foldable() && upper.foldable() ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return translate(handler);
    }

    private RangeQuery translate(TranslatorHandler handler) {
        Object l = valueOf(FoldContext.small() /* TODO remove me */, lower);
        Object u = valueOf(FoldContext.small() /* TODO remove me */, upper);
        String format = null;

        DataType dataType = value.dataType();
        logger.trace(
            "Translating Range into lucene query.  dataType is [{}] upper is [{}<{}>]  lower is [{}<{}>]",
            dataType,
            lower,
            lower.dataType(),
            upper,
            upper.dataType()
        );
        if (dataType == DataType.DATETIME) {
            l = dateWithTypeToString((Long) l, lower.dataType());
            u = dateWithTypeToString((Long) u, upper.dataType());
            format = DEFAULT_DATE_TIME_FORMATTER.pattern();
        }

        if (dataType == DATE_NANOS) {
            l = dateWithTypeToString((Long) l, lower.dataType());
            u = dateWithTypeToString((Long) u, upper.dataType());
            format = DEFAULT_DATE_NANOS_FORMATTER.pattern();
        }

        if (dataType == IP) {
            if (l instanceof BytesRef bytesRef) {
                l = ipToString(bytesRef);
            }
            if (u instanceof BytesRef bytesRef) {
                u = ipToString(bytesRef);
            }
        } else if (dataType == VERSION) {
            // VersionStringFieldMapper#indexedValueForSearch() only accepts as input String or BytesRef with the String (i.e. not
            // encoded) representation of the version as it'll do the encoding itself.
            if (l instanceof BytesRef bytesRef) {
                l = versionToString(bytesRef);
            } else if (l instanceof Version version) {
                l = versionToString(version);
            }
            if (u instanceof BytesRef bytesRef) {
                u = versionToString(bytesRef);
            } else if (u instanceof Version version) {
                u = versionToString(version);
            }
        } else if (dataType == UNSIGNED_LONG) {
            if (l instanceof Long ul) {
                l = unsignedLongAsNumber(ul);
            }
            if (u instanceof Long ul) {
                u = unsignedLongAsNumber(ul);
            }
        }
        logger.trace("Building range query with format string [{}]", format);
        return new RangeQuery(source(), handler.nameOf(value), l, includeLower(), u, includeUpper(), format, zoneId);
    }

    @Override
    public Expression singleValueField() {
        return value;
    }
}
