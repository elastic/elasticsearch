/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.pushdown.StringPrefixUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Holds validated ESQL filter expressions whose translation to Parquet {@link FilterPredicate}s
 * is deferred until read time when the per-file physical schema is available.
 * <p>
 * This two-level approach (validate at optimize time, translate at read time) follows Spark's
 * ParquetFilters design (SPARK-24716). It is necessary because DATETIME columns can have
 * different physical representations across Parquet files in the same glob:
 * <ul>
 *   <li>INT32 with DATE annotation (days since epoch)</li>
 *   <li>INT64 with TIMESTAMP_MILLIS/MICROS/NANOS annotation</li>
 *   <li>INT96 (deprecated, not pushable)</li>
 * </ul>
 * Using ESQL's epoch millis directly against non-millis statistics would cause incorrect
 * row group skipping — a correctness issue, not just suboptimal performance.
 */
final class ParquetPushedExpressions {

    private static final Logger logger = LogManager.getLogger(ParquetPushedExpressions.class);

    static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();

    private final List<Expression> expressions;
    /**
     * Cache of compiled {@link CompiledWildcard} forms per {@link WildcardLike} expression.
     * Building a {@link ByteRunAutomaton} from a wildcard pattern (in particular the determinize
     * step in {@link org.apache.lucene.util.automaton.Operations#determinize}) is non-trivial —
     * well into tens of microseconds for moderately complex patterns — and the same expression
     * instance is reused across every batch of every row group. {@link IdentityHashMap} is
     * intentional: ESQL shares expression nodes by reference, so identity is the correct equality.
     * The {@link CompiledWildcard#FAILED} sentinel marks expressions that could not be compiled
     * (e.g. too complex to determinize) so we do not retry on every batch.
     *
     * <p>Synchronized via the cache field as the lock object. The same
     * {@link ParquetPushedExpressions} instance is shared by every iterator created from a
     * {@link ParquetFormatReader}, and iterators for different files may run on different driver
     * threads. The lock is held only across the cache lookup and (on miss) the automaton build —
     * one build per pattern per query — so contention is negligible compared to the per-batch
     * automaton run, which executes outside the lock against the immutable {@link ByteRunAutomaton}.
     */
    private final IdentityHashMap<WildcardLike, CompiledWildcard> automatonCache = new IdentityHashMap<>();

    /**
     * Compiled form of a {@link WildcardLike}: the runnable matcher and a flag indicating that the
     * source automaton accepts every input. The flag is computed against the case-aware automaton
     * (the same one passed to {@link ByteRunAutomaton}), so the {@link #matchesAll} fast path in
     * {@link #evaluateWildcardLike} is consistent with the runtime case-sensitivity setting — it
     * does not silently fall through to the per-row loop just because the pattern's internal
     * case-insensitive cache disagrees with the requested flag.
     *
     * <p>{@code matcher} is {@code null} when the pattern failed to determinize; the caller treats
     * that as "fall back to FilterExec" (return {@code null} from evaluateWildcardLike).
     */
    private record CompiledWildcard(ByteRunAutomaton matcher, boolean matchesAll) {
        static final CompiledWildcard FAILED = new CompiledWildcard(null, false);
    }

    ParquetPushedExpressions(List<Expression> expressions) {
        this.expressions = expressions;
    }

    List<Expression> expressions() {
        return expressions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ParquetPushedExpressions other) {
            return Objects.equals(expressions, other.expressions);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(expressions);
    }

    @Override
    public String toString() {
        return "ParquetPushedExpressions[expressions=" + expressions + "]";
    }

    /**
     * Translates the held expressions to a combined Parquet {@link FilterPredicate} using
     * the actual file schema for type-correct value conversion.
     *
     * @param schema the Parquet file's MessageType schema (from footer metadata)
     * @return a combined FilterPredicate, or null if no expressions could be translated
     */
    FilterPredicate toFilterPredicate(MessageType schema) {
        List<FilterPredicate> translated = new ArrayList<>();
        for (Expression expr : expressions) {
            FilterPredicate fp = translateExpression(expr, schema);
            if (fp != null) {
                translated.add(fp);
            }
        }
        if (translated.isEmpty()) {
            return null;
        }
        FilterPredicate combined = translated.get(0);
        for (int i = 1; i < translated.size(); i++) {
            combined = FilterApi.and(combined, translated.get(i));
        }
        return combined;
    }

    /**
     * Returns {@code true} when at least one held conjunct is YES-eligible per
     * {@link ParquetFilterPushdownSupport#isFullyEvaluable(Expression)} (so {@code FilterExec}
     * has been dropped for it) AND its translation to a Parquet
     * {@link FilterPredicate} for {@code schema} is {@code null} (so it is not represented in
     * the {@link #toFilterPredicate} output).
     *
     * <p>Consumers of {@link #toFilterPredicate} that bypass {@link #evaluateFilter} on the basis
     * of stats reasoning over the predicate (e.g. the trivially-passes shortcut in
     * {@code OptimizedParquetColumnIterator}) MUST disable that shortcut when this method returns
     * {@code true}: the YES conjunct is silently absent from the predicate they reasoned about
     * and would otherwise leak rows it does not match. RECHECK conjuncts that fail to translate
     * are excluded from this check on purpose — their downstream {@code FilterExec} still
     * re-applies them, masking the shortcut's over-inclusion.
     *
     * <p>Today the only canConvert-but-not-translatable expression is {@link WildcardLike}
     * (and {@code Not(WildcardLike)}), which is also the only YES-eligible non-comparator
     * expression — so in practice this method returns {@code true} exactly when a {@code LIKE}
     * conjunct is present alongside other translatable conjuncts.
     *
     * <p>YES is determined here by {@link ParquetFilterPushdownSupport#isFullyEvaluable(Expression)}
     * rather than the full {@code canPush} check. The full check additionally probes
     * {@code canCompileAllPatterns}; a pattern that fails that probe at plan time is downgraded
     * to RECHECK and stays in {@code FilterExec}, so it would not be a YES conjunct at runtime.
     * Using only {@code isFullyEvaluable} here is therefore conservative — it may flag an
     * expression as YES that the planner already downgraded, suppressing the shortcut for that
     * file. The wasted work is bounded: at most one extra {@code evaluateFilter} pass per
     * trivially-passing row group, which is exactly the cost the shortcut was saving.
     *
     * <p><b>Do not weaken this method.</b> Returning {@code false} when it should return
     * {@code true} causes silent wrong-results: rows that do not match a YES conjunct (today:
     * a {@code LIKE} pattern) are emitted as if they did, because the trivially-passes shortcut
     * routes them around the late-mat evaluator and there is no downstream {@code FilterExec}
     * to catch the over-inclusion. The contract is exercised by
     * {@code ParquetPushedExpressionsTests#testHasYesConjunctOutsideFilterPredicate*} and the
     * integration regression test
     * {@code OptimizedFilteredReaderTests#testPushedExpressionsLikeWithStatsTrivialEqDoesNotLeak}.
     */
    boolean hasYesConjunctOutsideFilterPredicate(MessageType schema) {
        for (Expression expr : expressions) {
            if (ParquetFilterPushdownSupport.isFullyEvaluable(expr) && translateExpression(expr, schema) == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} when {@code expr} translates to a Parquet {@link FilterPredicate}
     * with no silent-drop branch — i.e. the resulting predicate has the same matching set as
     * {@code expr} (modulo TVL on nulls, which apache-mr handles compatibly for the basic
     * comparators below). Used by {@link #translateExpression}'s {@code Not} branch to
     * refuse pushing a negation over an expression that would silently drop a sub-arm:
     * negation flips a looser-than-truth predicate into a STRICTER-than-truth one, which
     * leaks rows during stats-based pruning.
     *
     * <p>Today this whitelist mirrors the leaf cases handled directly in
     * {@link #translateExpression} (no recursion into {@code And}/{@code Or}/{@code Not}).
     * That keeps the rule simple and obviously correct; it can be relaxed later (e.g. allow
     * {@code Not(And(translatable, translatable))}) once we have explicit test coverage for
     * the additional shapes.
     */
    private static boolean isExactlyTranslatable(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression && bc.right().foldable()) {
            return true;
        }
        if (expr instanceof In in && in.value() instanceof NamedExpression) {
            return true;
        }
        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression) {
            return true;
        }
        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression) {
            return true;
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression) {
            return true;
        }
        return false;
    }

    private FilterPredicate translateExpression(Expression expr, MessageType schema) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            String name = ne.name();
            DataType dataType = ne.dataType();
            Object value = literalValueOf(bc.right());

            if (value == null) {
                return null;
            }

            return switch (bc) {
                case Equals ignored -> buildPredicate(name, dataType, value, PredicateOp.EQ, schema);
                case NotEquals ignored -> buildPredicate(name, dataType, value, PredicateOp.NOT_EQ, schema);
                case GreaterThan ignored -> buildPredicate(name, dataType, value, PredicateOp.GT, schema);
                case GreaterThanOrEqual ignored -> buildPredicate(name, dataType, value, PredicateOp.GTE, schema);
                case LessThan ignored -> buildPredicate(name, dataType, value, PredicateOp.LT, schema);
                case LessThanOrEqual ignored -> buildPredicate(name, dataType, value, PredicateOp.LTE, schema);
                default -> null;
            };
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            return translateIn(ne.name(), ne.dataType(), inExpr.list(), schema);
        }
        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            return buildPredicate(ne.name(), ne.dataType(), null, PredicateOp.EQ, schema);
        }
        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            return buildPredicate(ne.name(), ne.dataType(), null, PredicateOp.NOT_EQ, schema);
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            return translateRange(ne.name(), ne.dataType(), range, schema);
        }
        if (expr instanceof And and) {
            // For AND, dropping an arm produces a LOOSER predicate (one that admits at least
            // as many rows). That is safe for stats pruning, RowRanges, and the
            // trivially-passes shortcut, all of which require a SUPERSET of the truth.
            FilterPredicate leftPred = translateExpression(and.left(), schema);
            FilterPredicate rightPred = translateExpression(and.right(), schema);
            if (leftPred != null && rightPred != null) {
                return FilterApi.and(leftPred, rightPred);
            }
            return leftPred != null ? leftPred : rightPred;
        }
        if (expr instanceof Or or) {
            // For OR, BOTH arms must translate or the predicate is unsafe. Dropping one OR
            // arm yields a STRICTER predicate (the surviving arm alone), which would prune
            // rows the original would have matched via the dropped arm. Return null so the
            // shortcut/RowRanges path skips this expression entirely.
            FilterPredicate leftPred = translateExpression(or.left(), schema);
            FilterPredicate rightPred = translateExpression(or.right(), schema);
            if (leftPred != null && rightPred != null) {
                return FilterApi.or(leftPred, rightPred);
            }
            return null;
        }
        if (expr instanceof Not not) {
            // Negation flips the looser/stricter polarity of any silent drop in the inner
            // expression: an inner AND that silently dropped an untranslatable arm produces a
            // looser inner predicate; NOT of looser is STRICTER, which prunes rows the
            // original would have matched (e.g. NOT(AND(LIKE, id<N)) becomes NOT(id<N), which
            // wrongly drops rows where LIKE doesn't match and id<N). To stay safe we require
            // the inner translation to be EXACT — i.e. it must contain no untranslatable
            // sub-expression at all. Practically this means the inner must be a leaf
            // comparator/range/equality that the translator handles directly. Anything more
            // complex returns null so the predicate is not pushed, leaving the row to the
            // late-mat evaluator (which evaluates the original ESQL expression, including
            // the conjuncts under the inner AND, with TVL-correct semantics).
            //
            // DO NOT REMOVE the isExactlyTranslatable guard — without it, NOT over a
            // silent-drop AND produces a stricter-than-truth predicate that silently loses
            // rows during stats-based row-group / page pruning. There is no FilterExec safety
            // net at the row-group/page level (FilterExec runs per-row on what survives).
            // Regression tests live in {@code ParquetReaderFilterDifferentialTests} (see the
            // randomized {@code NOT(AND(LIKE, ...))} cases that surfaced this bug).
            if (isExactlyTranslatable(not.field()) == false) {
                return null;
            }
            FilterPredicate inner = translateExpression(not.field(), schema);
            return inner != null ? FilterApi.not(inner) : null;
        }
        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression ne && sw.prefix().foldable()) {
            Object prefixValue = literalValueOf(sw.prefix());
            if (prefixValue == null) {
                return null;
            }
            BytesRef prefix = (BytesRef) prefixValue;
            var col = FilterApi.binaryColumn(ne.name());
            FilterPredicate lower = FilterApi.gtEq(col, toBinary(prefix));
            BytesRef upper = StringPrefixUtils.nextPrefixUpperBound(prefix);
            if (upper != null) {
                return FilterApi.and(lower, FilterApi.lt(col, toBinary(upper)));
            }
            return lower;
        }
        // WildcardLike has no native Parquet FilterPredicate translation: Parquet only supports
        // ordered comparisons, equality, and IN. The pattern is evaluated during late materialization
        // by evaluateWildcardLike. A future enhancement could derive a prefix range from
        // WildcardPattern#extractPrefix to enable row-group skipping for patterns like "https*google*".
        return null;
    }

    // -----------------------------------------------------------------------------------
    // Predicate building — type dispatch happens once, operations are applied generically
    // -----------------------------------------------------------------------------------

    enum PredicateOp {
        EQ,
        NOT_EQ,
        GT,
        GTE,
        LT,
        LTE;

        boolean isOrdered() {
            return this == GT || this == GTE || this == LT || this == LTE;
        }
    }

    private FilterPredicate buildPredicate(String columnName, DataType dataType, Object value, PredicateOp op, MessageType schema) {
        if (value == null && op.isOrdered()) {
            return null;
        }
        return switch (dataType) {
            case INTEGER -> orderedPredicate(FilterApi.intColumn(columnName), value != null ? ((Number) value).intValue() : null, op);
            case LONG -> orderedPredicate(FilterApi.longColumn(columnName), value != null ? ((Number) value).longValue() : null, op);
            case DOUBLE -> orderedPredicate(FilterApi.doubleColumn(columnName), value != null ? ((Number) value).doubleValue() : null, op);
            case KEYWORD -> orderedPredicate(FilterApi.binaryColumn(columnName), value != null ? toBinary(value) : null, op);
            case BOOLEAN -> {
                var col = FilterApi.booleanColumn(columnName);
                Boolean v = value != null ? (Boolean) value : null;
                yield switch (op) {
                    case EQ -> FilterApi.eq(col, v);
                    case NOT_EQ -> FilterApi.notEq(col, v);
                    default -> null;
                };
            }
            case DATETIME -> buildDatetimePredicate(columnName, value, op, schema);
            default -> null;
        };
    }

    private static FilterPredicate buildDatetimePredicate(String columnName, Object value, PredicateOp op, MessageType schema) {
        if (schema.containsField(columnName) == false) {
            return null;
        }
        PrimitiveType ptype = schema.getType(columnName).asPrimitiveType();
        LogicalTypeAnnotation logical = ptype.getLogicalTypeAnnotation();

        if (value == null) {
            return switch (ptype.getPrimitiveTypeName()) {
                case INT32 -> orderedPredicate(FilterApi.intColumn(columnName), null, op);
                case INT64 -> orderedPredicate(FilterApi.longColumn(columnName), null, op);
                default -> null;
            };
        }

        long millis = ((Number) value).longValue();
        return switch (ptype.getPrimitiveTypeName()) {
            case INT32 -> {
                if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    int days = (int) Math.floorDiv(millis, MILLIS_PER_DAY);
                    yield orderedPredicate(FilterApi.intColumn(columnName), days, op);
                }
                yield null;
            }
            case INT64 -> {
                try {
                    long physicalValue = convertMillisToPhysical(millis, logical);
                    yield orderedPredicate(FilterApi.longColumn(columnName), physicalValue, op);
                } catch (ArithmeticException e) {
                    yield null;
                }
            }
            default -> null;
        };
    }

    /**
     * Converts ESQL epoch millis to the physical unit used in the Parquet file.
     * Uses {@link Math#multiplyExact} to detect overflow — timestamps beyond ~year 2262
     * would overflow when scaled to nanos.
     */
    static long convertMillisToPhysical(long millis, LogicalTypeAnnotation logical) {
        if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return switch (ts.getUnit()) {
                case MILLIS -> millis;
                case MICROS -> Math.multiplyExact(millis, 1000L);
                case NANOS -> Math.multiplyExact(millis, 1_000_000L);
            };
        }
        return millis;
    }

    private static <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsLtGt> FilterPredicate orderedPredicate(
        C col,
        T value,
        PredicateOp op
    ) {
        return switch (op) {
            case EQ -> FilterApi.eq(col, value);
            case NOT_EQ -> FilterApi.notEq(col, value);
            case GT -> FilterApi.gt(col, value);
            case GTE -> FilterApi.gtEq(col, value);
            case LT -> FilterApi.lt(col, value);
            case LTE -> FilterApi.ltEq(col, value);
        };
    }

    private FilterPredicate translateIn(String columnName, DataType dataType, List<Expression> items, MessageType schema) {
        List<Object> rawValues = new ArrayList<>();
        for (Expression item : items) {
            Object val = literalValueOf(item);
            if (val != null) {
                rawValues.add(val);
            }
        }
        if (rawValues.isEmpty()) {
            return null;
        }
        return switch (dataType) {
            case INTEGER -> inPredicate(FilterApi.intColumn(columnName), rawValues, v -> ((Number) v).intValue());
            case LONG -> inPredicate(FilterApi.longColumn(columnName), rawValues, v -> ((Number) v).longValue());
            case DOUBLE -> inPredicate(FilterApi.doubleColumn(columnName), rawValues, v -> ((Number) v).doubleValue());
            case KEYWORD -> inPredicate(FilterApi.binaryColumn(columnName), rawValues, ParquetPushedExpressions::toBinary);
            case BOOLEAN -> inPredicate(FilterApi.booleanColumn(columnName), rawValues, v -> (Boolean) v);
            case DATETIME -> translateDatetimeIn(columnName, rawValues, schema);
            default -> null;
        };
    }

    private static FilterPredicate translateDatetimeIn(String columnName, List<Object> rawValues, MessageType schema) {
        if (schema.containsField(columnName) == false) {
            return null;
        }
        PrimitiveType ptype = schema.getType(columnName).asPrimitiveType();
        LogicalTypeAnnotation logical = ptype.getLogicalTypeAnnotation();
        try {
            return switch (ptype.getPrimitiveTypeName()) {
                case INT32 -> {
                    if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                        yield inPredicate(
                            FilterApi.intColumn(columnName),
                            rawValues,
                            v -> (int) Math.floorDiv(((Number) v).longValue(), MILLIS_PER_DAY)
                        );
                    }
                    yield null;
                }
                case INT64 -> inPredicate(
                    FilterApi.longColumn(columnName),
                    rawValues,
                    v -> convertMillisToPhysical(((Number) v).longValue(), logical)
                );
                default -> null;
            };
        } catch (ArithmeticException e) {
            return null;
        }
    }

    private static <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsEqNotEq> FilterPredicate inPredicate(
        C col,
        List<Object> values,
        Function<Object, T> converter
    ) {
        Set<T> converted = new HashSet<>();
        for (Object v : values) {
            converted.add(converter.apply(v));
        }
        return FilterApi.in(col, converted);
    }

    private FilterPredicate translateRange(String columnName, DataType dataType, Range range, MessageType schema) {
        Object lower = literalValueOf(range.lower());
        Object upper = literalValueOf(range.upper());

        FilterPredicate lowerBound = buildPredicate(
            columnName,
            dataType,
            lower,
            range.includeLower() ? PredicateOp.GTE : PredicateOp.GT,
            schema
        );
        FilterPredicate upperBound = buildPredicate(
            columnName,
            dataType,
            upper,
            range.includeUpper() ? PredicateOp.LTE : PredicateOp.LT,
            schema
        );

        if (lowerBound != null && upperBound != null) {
            return FilterApi.and(lowerBound, upperBound);
        }
        return null;
    }

    private static Binary toBinary(Object value) {
        if (value instanceof BytesRef bytesRef) {
            return Binary.fromConstantByteArray(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        return Binary.fromString(value.toString());
    }

    /**
     * Returns the set of column names referenced by the pushed filter expressions.
     * This is useful for identifying which columns participate in predicates so that
     * they can be read even when not explicitly projected.
     */
    Set<String> predicateColumnNames() {
        Set<String> names = new HashSet<>();
        for (Expression expr : expressions) {
            collectColumnNames(expr, names);
        }
        return names;
    }

    /**
     * Returns the set of column names referenced by a single expression.
     * Used by multi-stage Phase 1 to group expressions into per-column stages.
     */
    static Set<String> columnNamesOf(Expression expr) {
        Set<String> names = new HashSet<>();
        collectColumnNames(expr, names);
        return names;
    }

    private static void collectColumnNames(Expression expr, Set<String> names) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne) {
            names.add(ne.name());
        } else if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            names.add(ne.name());
        } else if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            names.add(ne.name());
        } else if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            names.add(ne.name());
        } else if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            names.add(ne.name());
        } else if (expr instanceof And and) {
            collectColumnNames(and.left(), names);
            collectColumnNames(and.right(), names);
        } else if (expr instanceof Or or) {
            collectColumnNames(or.left(), names);
            collectColumnNames(or.right(), names);
        } else if (expr instanceof Not not) {
            collectColumnNames(not.field(), names);
        } else if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression ne) {
            names.add(ne.name());
        } else if (expr instanceof WildcardLike wl && wl.field() instanceof NamedExpression ne) {
            names.add(ne.name());
        }
    }

    /**
     * Evaluates all held filter expressions against the given predicate blocks and returns
     * a survivor mask indicating which rows pass all predicates. Returns {@code null} if
     * all rows survive (no filtering needed), signaling that compaction can be skipped.
     *
     * @param predicateBlocks map of column name to decoded Block for predicate columns
     * @param rowCount        the number of rows in the current batch
     * @param reusable        a reusable WordMask instance to avoid allocation
     * @return the survivor mask with bits set for passing rows, or null if all rows survive
     */
    /**
     * Evaluates a single expression against blocks decoded from specific columns. Used by
     * multi-stage Phase 1 where each stage evaluates one expression at a time.
     *
     * @param expr             the expression to evaluate
     * @param blocks           decoded blocks indexed by column position (may have nulls for non-stage columns)
     * @param attributes       the projected attribute list for column name resolution
     * @param rowCount         the number of rows in the batch
     * @param intermediateMask optional cumulative mask from prior stages (for mask short-circuit)
     * @return survivor mask, or null if all rows survive
     */
    WordMask evaluateSingleExpression(
        Expression expr,
        Block[] blocks,
        List<Attribute> attributes,
        int rowCount,
        @Nullable WordMask intermediateMask
    ) {
        Map<String, Block> blockMap = new HashMap<>();
        for (int i = 0; i < blocks.length; i++) {
            if (blocks[i] != null && i < attributes.size()) {
                blockMap.put(attributes.get(i).name(), blocks[i]);
            }
        }
        return evaluateExpression(expr, blockMap, rowCount, intermediateMask);
    }

    WordMask evaluateFilter(Map<String, Block> predicateBlocks, int rowCount, WordMask reusable) {
        reusable.setAll(rowCount);
        for (Expression expr : expressions) {
            WordMask exprResult = evaluateExpression(expr, predicateBlocks, rowCount, reusable);
            if (exprResult != null) {
                reusable.and(exprResult);
            }
        }
        if (reusable.isAll()) {
            return null;
        }
        return reusable;
    }

    // Note: not static — uses the per-instance automaton cache in evaluateWildcardLike.
    // The intermediateMask is the cumulative AND of all previously evaluated conjuncts;
    // expensive evaluators (LIKE, StartsWith) use it to skip already-eliminated rows.
    private WordMask evaluateExpression(Expression expr, Map<String, Block> blocks, int rowCount, @Nullable WordMask intermediateMask) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            Object literal = literalValueOf(bc.right());
            if (literal == null) {
                return null;
            }
            return evaluateComparison(bc, block, literal, rowCount);
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            return evaluateIn(inExpr, block, rowCount);
        }
        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            WordMask mask = new WordMask();
            mask.reset(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    mask.set(i);
                }
            }
            return mask;
        }
        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            WordMask mask = new WordMask();
            mask.reset(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false) {
                    mask.set(i);
                }
            }
            return mask;
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            return evaluateRange(range, block, rowCount);
        }
        if (expr instanceof And and) {
            WordMask left = evaluateExpression(and.left(), blocks, rowCount, intermediateMask);
            WordMask right = evaluateExpression(and.right(), blocks, rowCount, intermediateMask);
            if (left != null && right != null) {
                left.and(right);
                return left;
            }
            return left != null ? left : right;
        }
        if (expr instanceof Or or) {
            WordMask left = evaluateExpression(or.left(), blocks, rowCount, intermediateMask);
            WordMask right = evaluateExpression(or.right(), blocks, rowCount, intermediateMask);
            if (left != null && right != null) {
                left.or(right);
                return left;
            }
            // conservative: if either arm is unknown, the whole OR is unknown
            return null;
        }
        if (expr instanceof Not not) {
            // Special case: NOT (col LIKE p) needs SQL three-valued logic so that
            // NOT (NULL LIKE p) → UNKNOWN → row is filtered out (bit 0), not flipped
            // to bit 1 by the generic negate. This is a hard requirement for the YES
            // pushability of WildcardLike (see ParquetFilterPushdownSupport.isFullyEvaluable);
            // without this branch, dropping FilterExec for NOT (LIKE) would let null rows
            // survive the predicate, giving wrong results.
            //
            // Implementation: build the LIKE mask and the null mask, OR them ("matches OR
            // null"), then negate to get "non-null AND no-match" — the TVL-correct survivor
            // set. The mask for the inner WildcardLike already maps null rows to bit 0, so
            // OR-ing the explicit null mask is what restores the missing TVL bit before
            // the bitwise complement.
            if (not.field() instanceof WildcardLike wl && wl.field() instanceof NamedExpression ne) {
                Block block = blocks.get(ne.name());
                if (block == null) {
                    return null;
                }
                return evaluateNotWildcardLike(wl, block, rowCount, intermediateMask);
            }
            WordMask inner = evaluateExpression(not.field(), blocks, rowCount, intermediateMask);
            if (inner != null) {
                inner.negate();
                return inner;
            }
            return null;
        }
        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression ne) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            return evaluateStartsWith(sw, block, rowCount, intermediateMask);
        }
        if (expr instanceof WildcardLike wl && wl.field() instanceof NamedExpression ne) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            return evaluateWildcardLike(wl, block, rowCount, intermediateMask);
        }
        return null;
    }

    private static WordMask evaluateComparison(EsqlBinaryComparison bc, Block block, Object literal, int rowCount) {
        WordMask mask = new WordMask();
        mask.reset(rowCount);
        if (block instanceof IntBlock ib) {
            int val = ((Number) literal).intValue();
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && compareResult(Integer.compare(ib.getInt(i), val), bc)) {
                    mask.set(i);
                }
            }
        } else if (block instanceof LongBlock lb) {
            Long boxed = toLongValue(literal);
            if (boxed == null) {
                return null;
            }
            long val = boxed;
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && compareResult(Long.compare(lb.getLong(i), val), bc)) {
                    mask.set(i);
                }
            }
        } else if (block instanceof DoubleBlock db) {
            double val = ((Number) literal).doubleValue();
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && compareResult(Double.compare(db.getDouble(i), val), bc)) {
                    mask.set(i);
                }
            }
        } else if (block instanceof OrdinalBytesRefBlock obb && shouldShortCircuitOnDictionary(obb)) {
            // Dictionary short-circuit: evaluate the comparison once per dictionary entry,
            // then map each row's ordinal to a precomputed boolean. Avoids one string compareTo
            // per row in favor of one int lookup per row.
            BytesRef val = toByteRef(literal);
            boolean[] dictMatches = matchingDictionaryEntries(obb.getDictionaryVector(), entry -> compareResult(entry.compareTo(val), bc));
            applyDictionaryMatches(obb, dictMatches, mask, rowCount);
        } else if (block instanceof BytesRefBlock bb) {
            BytesRef val = toByteRef(literal);
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && compareResult(bb.getBytesRef(i, scratch).compareTo(val), bc)) {
                    mask.set(i);
                }
            }
        } else if (block instanceof BooleanBlock boolBlock) {
            boolean val = (Boolean) literal;
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && compareResult(Boolean.compare(boolBlock.getBoolean(i), val), bc)) {
                    mask.set(i);
                }
            }
        } else {
            return null;
        }
        return mask;
    }

    private static boolean compareResult(int cmp, EsqlBinaryComparison bc) {
        if (bc instanceof Equals) {
            return cmp == 0;
        } else if (bc instanceof NotEquals) {
            return cmp != 0;
        } else if (bc instanceof LessThan) {
            return cmp < 0;
        } else if (bc instanceof LessThanOrEqual) {
            return cmp <= 0;
        } else if (bc instanceof GreaterThan) {
            return cmp > 0;
        } else if (bc instanceof GreaterThanOrEqual) {
            return cmp >= 0;
        }
        return true;
    }

    private static Long toLongValue(Object literal) {
        if (literal instanceof Number n) {
            return n.longValue();
        }
        return null;
    }

    private static BytesRef toByteRef(Object literal) {
        if (literal instanceof BytesRef br) {
            return br;
        }
        if (literal instanceof String s) {
            return new BytesRef(s);
        }
        return new BytesRef(literal.toString());
    }

    private static WordMask evaluateIn(In inExpr, Block block, int rowCount) {
        List<Object> values = new ArrayList<>();
        for (Expression item : inExpr.list()) {
            Object val = literalValueOf(item);
            if (val != null) {
                values.add(val);
            }
        }
        if (values.isEmpty()) {
            return null;
        }
        WordMask mask = new WordMask();
        mask.reset(rowCount);
        if (block instanceof IntBlock ib) {
            Set<Integer> intSet = new HashSet<>();
            for (Object v : values) {
                intSet.add(((Number) v).intValue());
            }
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && intSet.contains(ib.getInt(i))) {
                    mask.set(i);
                }
            }
        } else if (block instanceof LongBlock lb) {
            Set<Long> longSet = new HashSet<>();
            for (Object v : values) {
                longSet.add(((Number) v).longValue());
            }
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && longSet.contains(lb.getLong(i))) {
                    mask.set(i);
                }
            }
        } else if (block instanceof DoubleBlock db) {
            Set<Double> doubleSet = new HashSet<>();
            for (Object v : values) {
                doubleSet.add(((Number) v).doubleValue());
            }
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && doubleSet.contains(db.getDouble(i))) {
                    mask.set(i);
                }
            }
        } else if (block instanceof OrdinalBytesRefBlock obb && shouldShortCircuitOnDictionary(obb)) {
            Set<BytesRef> refSet = new HashSet<>();
            for (Object v : values) {
                refSet.add(toByteRef(v));
            }
            boolean[] dictMatches = matchingDictionaryEntries(obb.getDictionaryVector(), refSet::contains);
            applyDictionaryMatches(obb, dictMatches, mask, rowCount);
        } else if (block instanceof BytesRefBlock bb) {
            Set<BytesRef> refSet = new HashSet<>();
            for (Object v : values) {
                refSet.add(toByteRef(v));
            }
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && refSet.contains(bb.getBytesRef(i, scratch))) {
                    mask.set(i);
                }
            }
        } else if (block instanceof BooleanBlock boolBlock) {
            Set<Boolean> boolSet = new HashSet<>();
            for (Object v : values) {
                boolSet.add((Boolean) v);
            }
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && boolSet.contains(boolBlock.getBoolean(i))) {
                    mask.set(i);
                }
            }
        } else {
            return null;
        }
        return mask;
    }

    private static WordMask evaluateRange(Range range, Block block, int rowCount) {
        Object lower = literalValueOf(range.lower());
        Object upper = literalValueOf(range.upper());
        if (lower == null && upper == null) {
            return null;
        }
        boolean incLo = range.includeLower();
        boolean incHi = range.includeUpper();
        WordMask mask = new WordMask();
        mask.reset(rowCount);
        if (block instanceof IntBlock ib) {
            boolean hasLo = lower != null;
            boolean hasHi = upper != null;
            int lo = hasLo ? ((Number) lower).intValue() : 0;
            int hi = hasHi ? ((Number) upper).intValue() : 0;
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false) {
                    int val = ib.getInt(i);
                    if (hasLo && (incLo ? val < lo : val <= lo)) continue;
                    if (hasHi && (incHi ? val > hi : val >= hi)) continue;
                    mask.set(i);
                }
            }
        } else if (block instanceof LongBlock lb) {
            boolean hasLo = lower != null;
            boolean hasHi = upper != null;
            long lo = hasLo ? ((Number) lower).longValue() : 0;
            long hi = hasHi ? ((Number) upper).longValue() : 0;
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false) {
                    long val = lb.getLong(i);
                    if (hasLo && (incLo ? val < lo : val <= lo)) continue;
                    if (hasHi && (incHi ? val > hi : val >= hi)) continue;
                    mask.set(i);
                }
            }
        } else if (block instanceof DoubleBlock db) {
            boolean hasLo = lower != null;
            boolean hasHi = upper != null;
            double lo = hasLo ? ((Number) lower).doubleValue() : 0;
            double hi = hasHi ? ((Number) upper).doubleValue() : 0;
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false) {
                    double val = db.getDouble(i);
                    if (hasLo && (incLo ? val < lo : val <= lo)) continue;
                    if (hasHi && (incHi ? val > hi : val >= hi)) continue;
                    mask.set(i);
                }
            }
        } else {
            return null;
        }
        return mask;
    }

    private static <T extends Comparable<T>> boolean inRange(T val, T lower, T upper, boolean includeLower, boolean includeUpper) {
        if (lower != null) {
            int cmp = val.compareTo(lower);
            if (includeLower ? cmp < 0 : cmp <= 0) {
                return false;
            }
        }
        if (upper != null) {
            int cmp = val.compareTo(upper);
            if (includeUpper ? cmp > 0 : cmp >= 0) {
                return false;
            }
        }
        return true;
    }

    private static WordMask evaluateStartsWith(StartsWith sw, Block block, int rowCount, @Nullable WordMask intermediateMask) {
        Object prefixValue = literalValueOf(sw.prefix());
        if (prefixValue == null) {
            return null;
        }
        BytesRef prefix = toByteRef(prefixValue);
        if (block instanceof OrdinalBytesRefBlock obb && shouldShortCircuitOnDictionary(obb)) {
            WordMask mask = new WordMask();
            mask.reset(rowCount);
            boolean[] dictMatches = matchingDictionaryEntries(
                obb.getDictionaryVector(),
                entry -> entry.length >= prefix.length && startsWith(entry, prefix)
            );
            applyDictionaryMatches(obb, dictMatches, mask, rowCount);
            return mask;
        }
        if (block instanceof BytesRefBlock bb) {
            WordMask mask = new WordMask();
            mask.reset(rowCount);
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < rowCount; i++) {
                if (intermediateMask != null && intermediateMask.get(i) == false) {
                    continue;
                }
                if (block.isNull(i) == false) {
                    BytesRef val = bb.getBytesRef(i, scratch);
                    if (val.length >= prefix.length && startsWith(val, prefix)) {
                        mask.set(i);
                    }
                }
            }
            return mask;
        }
        return null;
    }

    private static boolean startsWith(BytesRef value, BytesRef prefix) {
        for (int j = 0; j < prefix.length; j++) {
            if (value.bytes[value.offset + j] != prefix.bytes[prefix.offset + j]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Evaluates a {@link WildcardLike} predicate against a block of values, returning a survivor mask.
     *
     * <p>The implementation follows the same shape as {@link #evaluateStartsWith}: a dictionary
     * short-circuit for {@link OrdinalBytesRefBlock}s with {@code rowCount >= 2 * dictSize}, and a
     * scalar per-row fallback for plain {@link BytesRefBlock}s. The big win for high-volume scans
     * (e.g. {@code URL LIKE "*google*"} on web-traffic logs) comes from the dictionary path, which
     * collapses {@code O(rowCount)} automaton runs into {@code O(dictionarySize)} runs plus a
     * per-row int lookup.
     *
     * <p><b>Null semantics.</b> The mask is two-valued: a row's bit is set when the value is
     * non-null and the automaton accepts its bytes. Nulls map to bit {@code 0}, the same convention
     * as {@link #evaluateStartsWith} and the standard runtime
     * {@link org.elasticsearch.xpack.esql.expression.function.scalar.string.AutomataMatch#process}
     * (which returns {@code false} for null input). For a bare {@code col LIKE p}, this is the
     * SQL three-valued-logic answer ({@code NULL LIKE p} → unknown → not a survivor) and the
     * predicate can be pushed as
     * {@link org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport.Pushability#YES}.
     *
     * <p><b>{@code NOT (col LIKE p)} semantics.</b> A naive {@link WordMask#negate} on this mask
     * is wrong for nulls: bit {@code 0} for "no match" is correctly flipped to bit {@code 1}, but
     * bit {@code 0} for "null" is also flipped to bit {@code 1} — and SQL TVL says
     * {@code NOT (NULL LIKE p)} is unknown and must not survive. The {@code Not(WildcardLike)}
     * branch in {@link #evaluateExpression} routes through {@link #evaluateNotWildcardLike}, which
     * OR-s the explicit null mask before negating. <b>YES pushability for {@code NOT (col LIKE p)}
     * depends on that special case</b>, and on the gating in
     * {@link ParquetFilterPushdownSupport#isFullyEvaluable}, which only allows {@code YES} for
     * {@code Not} when its child is a bare {@link WildcardLike}.
     *
     * <p>Returns {@code null} when the block is neither an {@link OrdinalBytesRefBlock} on the
     * dense path nor a {@link BytesRefBlock} (e.g. a constant-null block) — the conservative
     * "all rows survive" sentinel that {@link #evaluateFilter} treats as a no-op for this
     * predicate. Returns {@code null} also when the pattern is unusable (failed to determinize).
     * Both cases are safe under RECHECK because {@code FilterExec} re-checks; under YES they are
     * prevented at plan time by {@link ParquetFilterPushdownSupport#canPush}, which probes
     * {@link org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern#createAutomaton}
     * up front and falls back to RECHECK if it throws. The Parquet KEYWORD reader always produces
     * one of the two supported block types, so the block-type {@code null} sentinel is unreachable
     * on the YES path in practice.
     */
    private WordMask evaluateWildcardLike(WildcardLike wl, Block block, int rowCount, @Nullable WordMask intermediateMask) {
        CompiledWildcard compiled = automatonFor(wl);
        if (compiled.matcher == null) {
            return null;
        }
        if (compiled.matchesAll) {
            return maskNonNullRows(block, rowCount);
        }
        ByteRunAutomaton runner = compiled.matcher;
        if (block instanceof OrdinalBytesRefBlock obb && shouldShortCircuitOnDictionary(obb)) {
            WordMask mask = new WordMask();
            mask.reset(rowCount);
            boolean[] dictMatches = matchingDictionaryEntries(
                obb.getDictionaryVector(),
                entry -> runner.run(entry.bytes, entry.offset, entry.length)
            );
            applyDictionaryMatches(obb, dictMatches, mask, rowCount);
            return mask;
        }
        if (block instanceof BytesRefBlock bb) {
            WordMask mask = new WordMask();
            mask.reset(rowCount);
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < rowCount; i++) {
                if (intermediateMask != null && intermediateMask.get(i) == false) {
                    continue;
                }
                if (block.isNull(i) == false) {
                    BytesRef val = bb.getBytesRef(i, scratch);
                    if (runner.run(val.bytes, val.offset, val.length)) {
                        mask.set(i);
                    }
                }
            }
            return mask;
        }
        return null;
    }

    /**
     * Evaluates {@code NOT (col LIKE p)} with SQL three-valued logic.
     *
     * <p>The straightforward {@code WordMask#negate} flip on the result of
     * {@link #evaluateWildcardLike} is wrong for null rows: the inner mask sets bit {@code 0}
     * for both "non-match" and "null", so the complement would mark nulls as survivors. SQL
     * says {@code NOT (NULL LIKE p)} is unknown and must not survive the predicate.
     *
     * <p>This method computes the survivor set "non-null AND no-match" directly:
     * {@code mask = LIKE(col, p)} (bit {@code 1} on match, bit {@code 0} on null/no-match);
     * then for every row that is null, set the bit (turning the mask into "match OR null");
     * then negate. The result has bit {@code 1} only for rows that are non-null and don't
     * match — TVL-correct.
     *
     * <p>Returns {@code null} when {@link #evaluateWildcardLike} returns {@code null}
     * (block type unsupported or pattern failed to determinize). The caller propagates that
     * up; {@link #evaluateFilter} treats it as "all rows survive" — the same conservative
     * sentinel used everywhere in this evaluator. <b>That null-return is only safe when the
     * predicate is RECHECK'd downstream</b>, but the YES path in
     * {@link ParquetFilterPushdownSupport} only fires when the block is a
     * {@link BytesRefBlock}/{@link OrdinalBytesRefBlock} (the Parquet KEYWORD reader's only
     * output) and the pattern is determinizable (KEYWORD inputs guarantee valid UTF-8 and
     * {@link org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern}'s
     * automaton build only throws {@code TooComplexToDeterminize} for pathological patterns
     * far beyond {@code "*google*"}). If a future change broadens the YES-eligible set, this
     * contract must be revisited.
     */
    private WordMask evaluateNotWildcardLike(WildcardLike wl, Block block, int rowCount, @Nullable WordMask intermediateMask) {
        WordMask likeMask = evaluateWildcardLike(wl, block, rowCount, intermediateMask);
        if (likeMask == null) {
            return null;
        }
        // Set bit i for null rows so the subsequent negate turns them into 0 (filtered out).
        // mayHaveNulls() is a cheap pre-check that lets the all-non-nulls common case skip
        // the per-row scan; matches the WildcardLike scalar path.
        if (block.mayHaveNulls()) {
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    likeMask.set(i);
                }
            }
        }
        likeMask.negate();
        return likeMask;
    }

    /**
     * Returns a mask with one bit set per non-null position. Used as the {@code matchesAll()}
     * shortcut in {@link #evaluateWildcardLike} — {@code LIKE "*"} accepts every value but, by
     * SQL three-valued-logic semantics, still rejects nulls.
     */
    private static WordMask maskNonNullRows(Block block, int rowCount) {
        WordMask mask = new WordMask();
        if (block.mayHaveNulls() == false) {
            mask.setAll(rowCount);
            return mask;
        }
        mask.reset(rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i) == false) {
                mask.set(i);
            }
        }
        return mask;
    }

    /**
     * Returns the compiled form of the given {@link WildcardLike}, building it once on first use
     * and caching it on the per-query {@link #automatonCache}. Returns {@link CompiledWildcard#FAILED}
     * when the pattern cannot be determinized (logged once at debug); the caller treats that as
     * "fall back to FilterExec".
     *
     * <p>Note on byte-vs-character semantics: {@link
     * org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern#createAutomaton}
     * returns a UTF-32 (character-level) automaton on both paths — case-sensitive via
     * {@link org.apache.lucene.search.WildcardQuery#toAutomaton}, case-insensitive via
     * {@link org.apache.lucene.util.automaton.RegExp}. Both therefore need the implicit
     * UTF-32→UTF-8 conversion that the single-argument
     * {@link ByteRunAutomaton#ByteRunAutomaton(Automaton)} constructor performs internally; the
     * {@code (Automaton, true)} two-arg form would skip that conversion and silently produce
     * incorrect matches for any non-ASCII byte. This mirrors
     * {@code StringScriptFieldWildcardQuery} in {@code org.elasticsearch.search.runtime}, which
     * uses the same single-arg constructor for the same reason.
     *
     * <p>{@code matchesAll} is computed against the case-aware automaton — the same one passed to
     * {@link ByteRunAutomaton} — so the {@link #evaluateWildcardLike} fast path stays in sync with
     * {@link WildcardLike#caseInsensitive()}.
     */
    private CompiledWildcard automatonFor(WildcardLike wl) {
        synchronized (automatonCache) {
            CompiledWildcard cached = automatonCache.get(wl);
            if (cached != null) {
                return cached;
            }
            CompiledWildcard compiled;
            try {
                Automaton automaton = wl.pattern().createAutomaton(wl.caseInsensitive());
                // Operations.isTotal returns true iff the automaton accepts every code-point sequence
                // over its alphabet (Unicode 0..0x10FFFF for WildcardPattern's UTF-32 output). After
                // the implicit UTF-32->UTF-8 conversion in the ByteRunAutomaton ctor, "total" carries
                // over to "accepts every valid UTF-8 byte sequence". Our inputs come from KEYWORD
                // columns, which Elasticsearch guarantees to be valid UTF-8, so this is a sound
                // proxy for "this LIKE accepts every non-null row" — the contract of matchesAll.
                // (For invalid UTF-8 — outside the KEYWORD contract — the byte-level automaton would
                // simply reject the malformed prefix, matching the per-row scalar path's behavior.)
                boolean matchesAll = Operations.isTotal(automaton);
                compiled = new CompiledWildcard(new ByteRunAutomaton(automaton), matchesAll);
            } catch (IllegalArgumentException | TooComplexToDeterminizeException e) {
                logger.debug(
                    "Cannot push WildcardLike pattern [{}] to Parquet late materialization, falling back to FilterExec",
                    wl.pattern().pattern(),
                    e
                );
                compiled = CompiledWildcard.FAILED;
            }
            automatonCache.put(wl, compiled);
            return compiled;
        }
    }

    // Package-private hook so tests can directly assert that automaton compilation is memoized
    // across batches (there is no public metric for it). Not part of the production contract.
    int automatonCacheSizeForTesting() {
        synchronized (automatonCache) {
            return automatonCache.size();
        }
    }

    /**
     * Returns {@code true} when evaluating the predicate against dictionary entries is expected
     * to be cheaper than per-row evaluation. For ordinal-encoded blocks, the dictionary path
     * runs the predicate once per unique value and then scatters results via integer lookups —
     * always cheaper than running the predicate per row since dictionary size &lt;= row count.
     * The minimum of 10 positions avoids the boolean[] allocation overhead for tiny blocks.
     */
    private static boolean shouldShortCircuitOnDictionary(OrdinalBytesRefBlock block) {
        return block.getPositionCount() >= 10;
    }

    /**
     * Evaluates {@code matcher} against every entry of {@code dictionary} and returns a
     * boolean array indexed by ordinal — {@code true} at position {@code k} means the
     * entry at ordinal {@code k} satisfies the predicate.
     *
     * <p>This is the core of the dictionary short-circuit: instead of running a per-row
     * predicate on every materialized value, we run it once per unique entry. For dictionary
     * encodings that are well chosen by the writer, the dictionary holds far fewer entries
     * than the row count, so we collapse O(rowCount) string compares into O(dictSize) plus
     * a per-row int lookup.
     */
    private static boolean[] matchingDictionaryEntries(BytesRefVector dictionary, Predicate<BytesRef> matcher) {
        int size = dictionary.getPositionCount();
        boolean[] matches = new boolean[size];
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < size; i++) {
            matches[i] = matcher.test(dictionary.getBytesRef(i, scratch));
        }
        return matches;
    }

    /**
     * Sets bits in {@code mask} for rows whose dictionary ordinal is flagged in
     * {@code dictMatches}, skipping null rows.
     *
     * <p>This relies on the ordinals block being <strong>single-valued</strong>: position
     * {@code i} maps directly to value index {@code i}. The Parquet reader's dictionary
     * path always satisfies this — see {@code PageColumnReader#buildOrdinalsBlock}, which
     * constructs the ordinals block with {@code firstValueIndexes == null}. The assertion
     * below documents and guards the invariant for any future producer.
     */
    private static void applyDictionaryMatches(OrdinalBytesRefBlock block, boolean[] dictMatches, WordMask mask, int rowCount) {
        IntBlock ordinals = block.getOrdinalsBlock();
        assert rowCount == block.getPositionCount() : "rowCount " + rowCount + " != block positions " + block.getPositionCount();
        assert ordinals.asVector() != null || ordinals.mayHaveMultivaluedFields() == false
            : "OrdinalBytesRefBlock with multivalued ordinals is not supported by the dictionary short-circuit";
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i) == false && dictMatches[ordinals.getInt(i)]) {
                mask.set(i);
            }
        }
    }
}
