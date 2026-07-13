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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.UninitializedArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.ByteMatchers;
import org.elasticsearch.xpack.esql.datasources.pushdown.StringPrefixUtils;
import org.elasticsearch.xpack.esql.datasources.pushdown.WildcardLikeShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Contains;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
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
     * Compiled form of a {@link WildcardLike}: the runnable matcher, a flag indicating that the
     * source automaton accepts every input, and an optional shape decomposition extracted from
     * case-sensitive patterns of the affix-contains family (see {@link WildcardLikeShape}). The
     * {@link #matchesAll} flag is computed against the case-aware automaton (the same one passed
     * to {@link ByteRunAutomaton}), so the fast path in {@link #evaluateWildcardLike} is
     * consistent with the runtime case-sensitivity setting — it does not silently fall through
     * to the per-row loop just because the pattern's internal case-insensitive cache disagrees
     * with the requested flag.
     *
     * <p>{@code matcher} is {@code null} when the pattern failed to determinize; the caller treats
     * that as "fall back to FilterExec" (return {@code null} from evaluateWildcardLike).
     *
     * <p>{@code shape} is non-null only for case-sensitive patterns of the form
     * {@code prefix*literal*suffix} (and all degenerate forms — {@code prefix*}, {@code *suffix},
     * {@code *literal*}, {@code prefix*suffix}, {@code prefix*literal*}, {@code *literal*suffix}).
     * When present, {@link #evaluateWildcardLike} dispatches to
     * {@link ByteMatchers#affixContains} instead of the per-byte automaton: short JDK-intrinsified
     * affix equality checks reject the bulk of non-matching values cheaply, and the SIMD-backed
     * substring scan handles the literal middle. Byte-substring against UTF-8 is codepoint-correct
     * because UTF-8 is self-synchronizing on valid inputs (the KEYWORD contract).
     */
    private record CompiledWildcard(ByteRunAutomaton matcher, boolean matchesAll, @Nullable WildcardLikeShape shape) {
        static final CompiledWildcard FAILED = new CompiledWildcard(null, false, null);
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
     * <p>Today the canConvert-but-not-translatable expressions are the LIKE-family predicates
     * {@link WildcardLike}, {@code Contains}, {@code EndsWith} and any {@code Not} over them —
     * none representable as a Parquet {@link FilterPredicate}. {@link StartsWith} and its
     * negation both translate (bare → prefix range; negated → {@code FilterApi.not(range)}).
     * All YES-eligible LIKE-family conjuncts that land here are untranslatable.
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
        // StartsWith is a leaf (no child sub-expressions that could untranslatably drop); its
        // translateExpression path produces a pure prefix-range FilterPredicate, so Not(StartsWith)
        // can safely push as FilterApi.not(range) for row-group/page pruning instead of relying
        // solely on the late-mat evaluator. EndsWith/Contains/WildcardLike are NOT exactly
        // translatable (they have no native FilterPredicate form at all).
        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression && sw.prefix().foldable()) {
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
            if (physicalPrimitiveIs(schema, ne.name(), PrimitiveType.PrimitiveTypeName.BINARY) == false) {
                return null; // declared keyword over a non-BINARY physical: decline, let FilterExec re-apply
            }
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
        // IS NULL / IS NOT NULL (null-valued EQ/NOT_EQ) over a list column (resolves to a LIST group,
        // not a primitive) must decline: pushing notEq(column("v"), null) names a leaf-absent column
        // that parquet-mr drops entirely. The null-mask evaluator that answers instead is multivalue-safe.
        // esql-planning#1056. Value predicates (comparisons/IN/LIKE) are deliberately NOT declined here —
        // their decoded-block evaluator reads by position index and is not multivalue-safe.
        if (value == null && resolveNestedPrimitive(schema, columnName) == null) {
            return null;
        }
        return switch (dataType) {
            case INTEGER -> buildIntPredicate(columnName, value, op, schema);
            case LONG -> buildLongPredicate(columnName, value, op, schema);
            case DOUBLE -> {
                if (isPhysicalDouble(schema, columnName)) {
                    yield orderedPredicate(FilterApi.doubleColumn(columnName), value != null ? ((Number) value).doubleValue() : null, op);
                }
                yield null;
            }
            case KEYWORD -> physicalPrimitiveIs(schema, columnName, PrimitiveType.PrimitiveTypeName.BINARY)
                ? orderedPredicate(FilterApi.binaryColumn(columnName), value != null ? toBinary(value) : null, op)
                : null;
            case BOOLEAN -> {
                if (physicalPrimitiveIs(schema, columnName, PrimitiveType.PrimitiveTypeName.BOOLEAN) == false) {
                    yield null;
                }
                var col = FilterApi.booleanColumn(columnName);
                Boolean v = value != null ? (Boolean) value : null;
                yield switch (op) {
                    case EQ -> FilterApi.eq(col, v);
                    case NOT_EQ -> FilterApi.notEq(col, v);
                    default -> null;
                };
            }
            case DATETIME -> buildDatetimePredicate(columnName, value, op, schema);
            case DATE_NANOS -> buildDateNanosPredicate(columnName, value, op, schema);
            default -> null;
        };
    }

    /**
     * Whether a raw integral predicate pushed against the file's raw footer statistics would mis-prune because the
     * scan decodes the column with a scaling transform the stats do not carry (parquet-mr prunes against the raw
     * physical values; the scan applies the transform on top, so any factor between them drops matching row groups).
     * The set of scaling annotations is owned by {@link ParquetColumnDecoding#integralDecodeScalesRelativeToRawStats}
     * — co-located with the decode transforms so the two cannot drift. This is the single authority every integral
     * push path ({@link #buildLongPredicate}/{@link #translateLongIn} and the {@code INTEGER} arms of
     * {@link #buildPredicate}/{@link #translateIn}) consults before pushing.
     */
    private static boolean pushDeclinedForUnitMismatch(LogicalTypeAnnotation annotation) {
        return ParquetColumnDecoding.integralDecodeScalesRelativeToRawStats(annotation);
    }

    /**
     * Returns {@code true} when the file's physical primitive at {@code columnName} (which may be a
     * dotted path into a nested STRUCT) is exactly {@code expected}.
     *
     * <p>The INTEGER/KEYWORD/BOOLEAN predicate arms guard on this before minting a {@link FilterApi}
     * column of the matching kind. A declared retype is a supported coercion ({@code
     * DeclaredTypeCoercions.supports}), so {@code keyword} over a physical {@code INT64}, or {@code
     * integer} over a physical {@code INT64}, reaches those arms — without the guard they would push a
     * BINARY/INT32/BOOLEAN predicate against a column the file stores as something else, which
     * parquet-mr rejects as a declared-type mismatch or (worse) mis-prunes. Declining is safe: these
     * predicates are RECHECK, so {@code FilterExec} re-applies the real ESQL semantics — the same
     * reasoning as {@link #buildLongPredicate} and {@link #isPhysicalDouble}.
     */
    private static boolean physicalPrimitiveIs(MessageType schema, String columnName, PrimitiveType.PrimitiveTypeName expected) {
        PrimitiveType primitive = resolveNestedPrimitive(schema, columnName);
        return primitive != null && primitive.getPrimitiveTypeName() == expected;
    }

    /**
     * Returns {@code true} when the file's physical primitive at {@code columnName} (which may be
     * a dotted path into a nested STRUCT) is {@link PrimitiveType.PrimitiveTypeName#DOUBLE}.
     */
    private static boolean isPhysicalDouble(MessageType schema, String columnName) {
        return physicalPrimitiveIs(schema, columnName, PrimitiveType.PrimitiveTypeName.DOUBLE);
    }

    /**
     * Builds a predicate for an ESQL {@code LONG} column, dispatching on the file's <b>physical</b>
     * primitive rather than the (possibly widened) ESQL type — see the class Javadoc's
     * {@code INT32}/{@code INT64} split. Two Parquet shapes surface as ESQL {@code LONG} while their
     * physical primitive is {@code INT32}: an unsigned 32-bit integer (values can exceed signed
     * {@code int} range) and {@code TIME_MILLIS} (signed, but ESQL has no distinct "time of day" type).
     * A {@link FilterApi#longColumn} pushed against either would describe a column the file doesn't
     * have — {@code INT64} — and parquet-mr rejects it as a declared-type mismatch
     * (github.com/elastic/esql-planning/issues/1030).
     *
     * <p>When the physical primitive is {@code INT32}, the literal is narrowed via
     * {@link #narrowLongToPhysicalInt32}; when narrowing fails (the literal cannot possibly match any
     * value the column can hold) this returns {@code null} rather than push an incorrect predicate.
     * That is safe: LONG comparisons are always RECHECK, never YES (see
     * {@link ParquetFilterPushdownSupport#isFullyEvaluable}), so {@code FilterExec} re-applies the
     * real ESQL semantics regardless of whether this predicate was pushed for pruning.
     */
    private static FilterPredicate buildLongPredicate(String columnName, Object value, PredicateOp op, MessageType schema) {
        PrimitiveType ptype = resolveNestedPrimitive(schema, columnName);
        if (ptype == null) {
            return null;
        }
        // A temporal-annotated column reaching a LONG predicate has a unit transform between the block value and
        // the raw physical value the row-group statistics hold: TIMESTAMP(MICROS)->epoch-nanos (x1000),
        // DATE(days)->epoch-millis (x86_400_000), TIME(MICROS)->nanos-of-day (x1000). The literal is in the decoded
        // unit, the stats are in the physical unit, so pushing it prunes row groups that genuinely match. Pruning is
        // unrecoverable — RECHECK guards against false positives, not against rows we never read — so decline and let
        // FilterExec apply the real semantics. Reached via a DECLARED long over a TIMESTAMP/DATE column or an
        // inferred TIME(MICROS) column; inferred datetime/date_nanos go through the unit-aware build*Predicate arms.
        if (pushDeclinedForUnitMismatch(ptype.getLogicalTypeAnnotation())) {
            return null;
        }
        return switch (ptype.getPrimitiveTypeName()) {
            case INT64 -> {
                if (ParquetColumnDecoding.isUnsignedInt64(ptype) && op != PredicateOp.EQ && op != PredicateOp.NOT_EQ) {
                    // ORDERED comparison over an UNSIGNED_64 column: the block decodes uint64 via signed sign-wrap
                    // (raws >= 2^63 read as negative), so signed-block ordering disagrees with parquet-mr's UNSIGNED
                    // row-group comparator in BOTH directions and for either literal sign: lt/lte drop the negative-
                    // block groups (large unsigned) that genuinely match, and gt/gte — though merely over-including on
                    // their own — become UNDER-including once wrapped in NOT, which the schema-blind
                    // isExactlyTranslatable pushes as if exact. So decline every ordered op; only eq/notEq (and IN)
                    // are bit-exact and stay pushable. The INT32 sibling declines the analogous unsigned mismatch.
                    yield null;
                }
                yield orderedPredicate(FilterApi.longColumn(columnName), value != null ? ((Number) value).longValue() : null, op);
            }
            case INT32 -> {
                if (value == null) {
                    yield orderedPredicate(FilterApi.intColumn(columnName), null, op);
                }
                Integer narrowed = narrowLongToPhysicalInt32(((Number) value).longValue(), ptype);
                yield narrowed != null ? orderedPredicate(FilterApi.intColumn(columnName), narrowed, op) : null;
            }
            default -> null;
        };
    }

    /**
     * Builds a predicate for an ESQL {@code INTEGER} column over a physical {@code INT32} column. Mirrors
     * {@link #buildLongPredicate}: it consults {@link #pushDeclinedForUnitMismatch}, so a declared {@code integer}
     * over a {@code DATE} (INT32, x86_400_000) or {@code DECIMAL(INT32, scale>0)} (÷10^scale) column — whose scan
     * decode carries a transform the raw {@code INT32} footer stats do not — declines rather than mis-prunes.
     * Declining is safe — INTEGER comparisons are RECHECK, so {@code FilterExec} re-applies the exact semantics.
     */
    private static FilterPredicate buildIntPredicate(String columnName, Object value, PredicateOp op, MessageType schema) {
        PrimitiveType ptype = resolveNestedPrimitive(schema, columnName);
        if (ptype == null || ptype.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT32) {
            return null;
        }
        if (pushDeclinedForUnitMismatch(ptype.getLogicalTypeAnnotation())) {
            return null;
        }
        return orderedPredicate(FilterApi.intColumn(columnName), value != null ? ((Number) value).intValue() : null, op);
    }

    /**
     * Narrows an ESQL {@code LONG} literal to the raw {@code int} bit pattern to push against a
     * physical {@code INT32} column, or returns {@code null} when {@code value} cannot possibly be
     * held by that column (in which case the caller must not push a predicate for it — see
     * {@link #buildLongPredicate}).
     *
     * <p>For {@code UINT_32} (unsigned), any value in {@code [0, 2^32 - 1]} round-trips through
     * {@code (int) value}: the cast reinterprets the low 32 bits, which is exactly the column's raw
     * on-disk representation, and parquet-mr's statistics comparator already applies unsigned
     * ordering for {@code UINT_32} columns when evaluating the pushed predicate against row-group /
     * page stats, so the signed-vs-unsigned interpretation is handled beneath this method.
     *
     * <p>For a signed {@code INT32} widened to {@code LONG} (today: {@code TIME_MILLIS}, whose values
     * are always small and positive), the standard signed round trip applies.
     */
    @Nullable
    private static Integer narrowLongToPhysicalInt32(long value, PrimitiveType ptype) {
        if (ParquetColumnDecoding.isUnsignedInt32(ptype)) {
            return (value >= 0 && value <= 0xFFFFFFFFL) ? (int) value : null;
        }
        int narrowed = (int) value;
        return narrowed == value ? narrowed : null;
    }

    /**
     * Resolves a (possibly dotted) {@code name} to the leaf {@link PrimitiveType} in {@code schema}.
     * Applies the same D2 precedence as the prior PR's projection-time flattener: a literal
     * top-level field named exactly {@code "a.b.c"} wins over the dotted-path traversal
     * {@code a -> b -> c}. Returns {@code null} when the path is missing or lands on a group
     * (e.g. an intermediate STRUCT, MAP, or LIST) rather than a primitive — predicate pushdown
     * is only meaningful at primitive leaves.
     *
     * <p>This is the single dotted-path resolver used by {@link #isPhysicalDouble} and
     * {@link #buildDatetimePredicate} (and {@link #translateDatetimeIn}). Translation of the
     * raw column name into a parquet-mr {@link Operators.Column} happens via
     * {@link FilterApi#binaryColumn(String)} etc. which internally build a multi-segment
     * {@link org.apache.parquet.hadoop.metadata.ColumnPath} via
     * {@code ColumnPath.fromDotString} — so the dotted name flows end-to-end through the row
     * group filter without any additional wrapping here.
     */
    @Nullable
    static PrimitiveType resolveNestedPrimitive(MessageType schema, String dottedName) {
        if (schema.containsField(dottedName)) {
            Type leaf = schema.getType(dottedName);
            return leaf.isPrimitive() ? leaf.asPrimitiveType() : null;
        }
        // Walk left-to-right, allowing literal-dot top-level prefixes to compose with nested
        // children — the exact-name fast path above already handled the no-dot case. Probe each
        // prefix via substring rather than re-joining segments on every iteration; when
        // {@code dottedName} has no dot at all the loop is skipped and we return null below.
        String[] segments = dottedName.split("\\.");
        int probeDot = dottedName.indexOf('.');
        int prefixLen = 1;
        while (probeDot >= 0) {
            String topLevel = dottedName.substring(0, probeDot);
            if (schema.containsField(topLevel)) {
                Type field = schema.getType(topLevel);
                for (int i = prefixLen; i < segments.length; i++) {
                    if (field.isPrimitive()) {
                        return null;
                    }
                    GroupType group = field.asGroupType();
                    if (group.containsField(segments[i]) == false) {
                        field = null;
                        break;
                    }
                    field = group.getType(segments[i]);
                }
                if (field != null && field.isPrimitive()) {
                    return field.asPrimitiveType();
                }
            }
            probeDot = dottedName.indexOf('.', probeDot + 1);
            prefixLen++;
        }
        return null;
    }

    private static FilterPredicate buildDatetimePredicate(String columnName, Object value, PredicateOp op, MessageType schema) {
        PrimitiveType ptype = resolveNestedPrimitive(schema, columnName);
        if (ptype == null) {
            return null;
        }
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
                    // A DATE column stores whole days; the literal is epoch-millis. The bound must be rounded to a
                    // day boundary OUTWARD per operator (floorDiv for every op silently prunes matching days on `<`
                    // and `!=` — a non-midnight literal makes `< X` exclude the day it falls in, and `!= X` drop the
                    // all-day-D row group that genuinely matches). boundToPhysicalUnit handles the direction and
                    // declines EQ/NOT_EQ on a non-midnight literal.
                    Long dayBound = boundToPhysicalUnit(millis, op, MILLIS_PER_DAY);
                    yield dayBound == null ? null : orderedPredicate(FilterApi.intColumn(columnName), (int) (long) dayBound, op);
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

    /**
     * Builds a predicate for an ESQL {@code DATE_NANOS} column, whose query literal is epoch-nanoseconds. A
     * {@code DATE_NANOS} column is only ever INFERRED from a physical {@code TIMESTAMP(MICROS)} (×1_000 to nanos) or
     * {@code TIMESTAMP(NANOS)} (identity) column — {@code date_nanos} is not a declarable type, and {@code
     * TIMESTAMP(MILLIS)} infers to {@code datetime} — so the nanosecond bound is converted to microseconds (or left
     * as nanos), rounded outward via {@link #boundToPhysicalUnit} so the pushed predicate is never stricter than the
     * true nanosecond predicate. Safe because temporal pushdown is always RECHECK (see
     * {@link ParquetFilterPushdownSupport#isFullyEvaluable}), so {@code FilterExec} re-applies the exact semantics.
     */
    private static FilterPredicate buildDateNanosPredicate(String columnName, Object value, PredicateOp op, MessageType schema) {
        PrimitiveType ptype = resolveNestedPrimitive(schema, columnName);
        if (ptype == null || ptype.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64) {
            return null;
        }
        if (value == null) {
            return orderedPredicate(FilterApi.longColumn(columnName), null, op);
        }
        long nanos = ((Number) value).longValue();
        long divisor = ParquetColumnDecoding.isMicrosTimestamp(ptype.getLogicalTypeAnnotation())
            ? ParquetColumnDecoding.NANOS_PER_MICRO
            : 1L;
        Long bound = boundToPhysicalUnit(nanos, op, divisor);
        return bound == null ? null : orderedPredicate(FilterApi.longColumn(columnName), bound, op);
    }

    /**
     * Converts a bound expressed in a fine unit to the coarse physical-unit bound to push against a column stored in
     * that coarse unit ({@code divisor} fine units per stored tick — e.g. nanos→micros is 1_000, nanos→millis is
     * 1_000_000, epoch-millis→epoch-days is {@link #MILLIS_PER_DAY}), rounding each comparison OUTWARD so the pushed
     * predicate never excludes a matching row (a stored tick {@code t} represents the fine value {@code t × divisor}):
     * {@code >}/{@code <=} round down, {@code >=}/{@code <} round up, and {@code ==}/{@code !=} are only representable
     * when the bound lands exactly on a tick — otherwise {@code null} is returned and no predicate is pushed (the scan
     * plus {@code FilterExec} recheck still yields the correct result, only without pruning). Getting the direction
     * wrong (e.g. floor for {@code <}) is a silent false-negative: it prunes a row group the query genuinely matches.
     * {@code divisor == 1} is the identity case, where floor/ceil/mod all leave the bound unchanged.
     */
    private static Long boundToPhysicalUnit(long value, PredicateOp op, long divisor) {
        return switch (op) {
            case GT, LTE -> Math.floorDiv(value, divisor);
            case GTE, LT -> Math.ceilDiv(value, divisor);
            case EQ, NOT_EQ -> value % divisor == 0 ? value / divisor : null;
        };
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
            case INTEGER -> translateIntIn(columnName, rawValues, schema);
            case LONG -> translateLongIn(columnName, rawValues, schema);
            case DOUBLE -> {
                if (isPhysicalDouble(schema, columnName)) {
                    yield inPredicate(FilterApi.doubleColumn(columnName), rawValues, v -> ((Number) v).doubleValue());
                }
                yield null;
            }
            case KEYWORD -> physicalPrimitiveIs(schema, columnName, PrimitiveType.PrimitiveTypeName.BINARY)
                ? inPredicate(FilterApi.binaryColumn(columnName), rawValues, ParquetPushedExpressions::toBinary)
                : null;
            case BOOLEAN -> physicalPrimitiveIs(schema, columnName, PrimitiveType.PrimitiveTypeName.BOOLEAN)
                ? inPredicate(FilterApi.booleanColumn(columnName), rawValues, v -> (Boolean) v)
                : null;
            case DATETIME -> translateDatetimeIn(columnName, rawValues, schema);
            case DATE_NANOS -> translateDateNanosIn(columnName, rawValues, schema);
            default -> null;
        };
    }

    /**
     * {@code IN} counterpart to {@link #buildLongPredicate}: dispatches on the physical primitive
     * rather than the widened ESQL {@code LONG} type. For a physical {@code INT32} column, literals
     * that fail to narrow (see {@link #narrowLongToPhysicalInt32}) are dropped from the pushed set
     * rather than aborting the whole predicate — such a literal can never equal any value the column
     * holds, so omitting it only makes the pushed set a (still-correct) subset of the true domain,
     * never stricter than the truth for the values that remain.
     */
    private static FilterPredicate translateLongIn(String columnName, List<Object> rawValues, MessageType schema) {
        PrimitiveType ptype = resolveNestedPrimitive(schema, columnName);
        if (ptype == null) {
            return null;
        }
        // Same unit-mismatch decline as buildLongPredicate — a temporal physical carries a unit transform the
        // raw stats do not, so an IN over a declared/inferred LONG temporal column must not push a raw predicate.
        if (pushDeclinedForUnitMismatch(ptype.getLogicalTypeAnnotation())) {
            return null;
        }
        return switch (ptype.getPrimitiveTypeName()) {
            case INT64 -> inPredicate(FilterApi.longColumn(columnName), rawValues, v -> ((Number) v).longValue());
            case INT32 -> {
                List<Object> narrowed = new ArrayList<>();
                for (Object v : rawValues) {
                    Integer n = narrowLongToPhysicalInt32(((Number) v).longValue(), ptype);
                    if (n != null) {
                        narrowed.add(n);
                    }
                }
                yield narrowed.isEmpty() ? null : inPredicate(FilterApi.intColumn(columnName), narrowed, v -> (Integer) v);
            }
            default -> null;
        };
    }

    /**
     * {@code IN} counterpart to {@link #buildIntPredicate}: pushes an {@code IN} over a physical {@code INT32} column,
     * declining via {@link #pushDeclinedForUnitMismatch} when the declared {@code integer} sits over a {@code DATE} or
     * {@code DECIMAL(scale>0)} column whose decode transform the raw footer stats do not carry.
     */
    private static FilterPredicate translateIntIn(String columnName, List<Object> rawValues, MessageType schema) {
        PrimitiveType ptype = resolveNestedPrimitive(schema, columnName);
        if (ptype == null || ptype.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT32) {
            return null;
        }
        if (pushDeclinedForUnitMismatch(ptype.getLogicalTypeAnnotation())) {
            return null;
        }
        return inPredicate(FilterApi.intColumn(columnName), rawValues, v -> ((Number) v).intValue());
    }

    private static FilterPredicate translateDatetimeIn(String columnName, List<Object> rawValues, MessageType schema) {
        PrimitiveType ptype = resolveNestedPrimitive(schema, columnName);
        if (ptype == null) {
            return null;
        }
        LogicalTypeAnnotation logical = ptype.getLogicalTypeAnnotation();
        try {
            return switch (ptype.getPrimitiveTypeName()) {
                case INT32 -> {
                    if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                        // Only a midnight literal can equal a stored day; a non-midnight literal matches no day, so
                        // drop it from the pushed set (a correct subset). Unlike a floorDiv'd day that over-includes,
                        // an exact set stays correct when this IN is wrapped in NOT. Empty set => push nothing.
                        List<Object> days = new ArrayList<>();
                        for (Object v : rawValues) {
                            long m = ((Number) v).longValue();
                            if (m % MILLIS_PER_DAY == 0) {
                                days.add((int) (m / MILLIS_PER_DAY));
                            }
                        }
                        yield days.isEmpty() ? null : inPredicate(FilterApi.intColumn(columnName), days, v -> (Integer) v);
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

    /**
     * {@code IN} counterpart to {@link #buildDateNanosPredicate}. The query literals are epoch-nanoseconds. For a
     * physical {@code NANOS} column each value is pushed exactly. For a {@code MICROS} column, an element can only
     * equal a stored value when it is an exact multiple of 1_000 ns; non-multiples are dropped (they can never match,
     * so omitting them keeps the pushed set a correct subset). If every element is dropped, no predicate is pushed and
     * the scan + recheck yields the (empty) result.
     */
    private static FilterPredicate translateDateNanosIn(String columnName, List<Object> rawValues, MessageType schema) {
        PrimitiveType ptype = resolveNestedPrimitive(schema, columnName);
        if (ptype == null || ptype.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64) {
            return null;
        }
        // A DATE_NANOS column is only ever inferred from TIMESTAMP(MICROS) (÷1_000) or TIMESTAMP(NANOS) (identity).
        if (ParquetColumnDecoding.isMicrosTimestamp(ptype.getLogicalTypeAnnotation()) == false) {
            return inPredicate(FilterApi.longColumn(columnName), rawValues, v -> ((Number) v).longValue());
        }
        List<Object> micros = new ArrayList<>();
        for (Object v : rawValues) {
            long nanos = ((Number) v).longValue();
            if (nanos % ParquetColumnDecoding.NANOS_PER_MICRO == 0) {
                micros.add(nanos / ParquetColumnDecoding.NANOS_PER_MICRO);
            }
        }
        return micros.isEmpty() ? null : inPredicate(FilterApi.longColumn(columnName), micros, v -> (Long) v);
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
        } else if (expr instanceof Contains c && c.singleValueField() instanceof NamedExpression ne) {
            names.add(ne.name());
        } else if (expr instanceof EndsWith ew && ew.singleValueField() instanceof NamedExpression ne) {
            names.add(ne.name());
        } else if (expr instanceof WildcardLike wl && wl.field() instanceof NamedExpression ne) {
            names.add(ne.name());
        }
    }

    /**
     * Evaluates a single expression against blocks decoded from specific columns. Used by
     * multi-stage Phase 1 where each stage evaluates one expression at a time. Dictionary
     * memoization is intentionally not threaded here: this path does not own a row-group
     * lifecycle compatible with the cache's invalidation contract, so the underlying
     * {@code evaluateExpression} call receives a {@code null} cache and dictionary bitmaps
     * are recomputed per batch. The full-filter path through {@link #evaluateFilter} is
     * what carries the memoization map across batches.
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
        return evaluateExpression(expr, blockMap, rowCount, intermediateMask, null);
    }

    /**
     * Evaluates the held filter expressions against {@code predicateBlocks} and returns a
     * survivor mask. Returns {@code null} when every row survives (the caller can then skip
     * compaction entirely); otherwise returns {@code reusable} populated with the surviving
     * positions. Equivalent to calling the four-argument overload with a {@code null}
     * dictionary cache — no cross-batch memoization, dictionary bitmaps are recomputed on
     * every call. Used by tests and by call sites that have no row-group lifecycle to hand
     * a cache to.
     */
    WordMask evaluateFilter(Map<String, Block> predicateBlocks, int rowCount, WordMask reusable) {
        return evaluateFilter(predicateBlocks, rowCount, reusable, null);
    }

    /**
     * Same as {@link #evaluateFilter(Map, int, WordMask)} but reuses dictionary-match bitmaps
     * across batches via {@code dictCache}, a map keyed by leaf {@link Expression} identity
     * holding one {@code boolean[]} per pushed predicate. The caller (typically
     * {@link OptimizedParquetColumnIterator#dictionaryBitmapsForCurrentRowGroup}) is
     * responsible for handing in a map scoped to the current row group — within a row group
     * the dictionary content is fixed so memoized results remain valid for every batch.
     *
     * <p>A {@code null} cache is permitted and preserves the original per-batch behavior;
     * this matters for callers that do not know the row-group lifecycle (e.g. unit tests
     * and the multi-stage single-expression path).
     */
    WordMask evaluateFilter(
        Map<String, Block> predicateBlocks,
        int rowCount,
        WordMask reusable,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
        reusable.setAll(rowCount);
        lastEvaluateExpressionCalls = 0;
        int evaluated = 0;
        for (Expression expr : expressions) {
            WordMask exprResult = evaluateExpression(expr, predicateBlocks, rowCount, reusable, dictCache);
            evaluated++;
            if (exprResult != null) {
                reusable.and(exprResult);
                // Early exit when no rows survive the conjunction so far. Subsequent expressions
                // would only AND further (the result can never grow); their evaluation cost —
                // notably the per-batch dictionary scan that does not consult the intermediate
                // mask — is therefore pure waste. WordMask#isEmpty is a 128-word scan for an
                // 8192-row batch, trivially cheap next to a dictionary scan of thousands of
                // entries plus an ordinal-to-boolean per-row mapping. Plan-time ordering by
                // FilterEvaluationOrderEstimator places selective predicates first, which is
                // what makes this short-circuit effective in practice (the discriminating
                // predicate runs first, then we skip the rest when its batch has no survivors).
                if (reusable.isEmpty()) {
                    lastExpressionsEvaluated = evaluated;
                    return reusable;
                }
            }
        }
        lastExpressionsEvaluated = evaluated;
        if (reusable.isAll()) {
            return null;
        }
        return reusable;
    }

    // Test-only observability. {@code lastExpressionsEvaluated} counts the number of
    // top-level conjuncts the most recent evaluateFilter actually walked before either
    // short-circuiting on an empty mask or running to completion.
    // {@code lastEvaluateExpressionCalls} counts every entry to {@code evaluateExpression}
    // — including recursive descents into nested And/Or — and resets at the start of each
    // evaluateFilter. The pair lets tests distinguish the top-level loop's early exit from
    // the nested-And short-circuit. Production code does not read these fields.
    private int lastExpressionsEvaluated;
    private int lastEvaluateExpressionCalls;

    int lastExpressionsEvaluatedForTesting() {
        return lastExpressionsEvaluated;
    }

    int lastEvaluateExpressionCallsForTesting() {
        return lastEvaluateExpressionCalls;
    }

    // Note: not static — uses the per-instance automaton cache in evaluateWildcardLike.
    // The intermediateMask is the cumulative AND of all previously evaluated conjuncts;
    // expensive evaluators (LIKE, StartsWith) use it to skip already-eliminated rows. The
    // dictCache (optional) memoizes per-batch dictionary-match bitmaps for the lifetime of
    // one row group; the caller owns the lifecycle (see
    // OptimizedParquetColumnIterator#dictionaryBitmapsForCurrentRowGroup).
    private WordMask evaluateExpression(
        Expression expr,
        Map<String, Block> blocks,
        int rowCount,
        @Nullable WordMask intermediateMask,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
        lastEvaluateExpressionCalls++;
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            Object literal = literalValueOf(bc.right());
            if (literal == null) {
                return null;
            }
            return evaluateComparison(bc, block, literal, rowCount, dictCache);
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            return evaluateIn(inExpr, block, rowCount, dictCache);
        }
        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            WordMask mask = new WordMask();
            // Fast path: the block has no nulls at all -> the survivor set is empty.
            // mayHaveNulls() is O(1) on every Block implementation; skipping the per-row
            // scan avoids rowCount calls to isNull() on the (very common) no-null case.
            // The zeroed mask is the correct result and contributes to the empty-mask
            // early exit in evaluateFilter for batches where this conjunct fires first.
            if (block.mayHaveNulls() == false) {
                mask.reset(rowCount);
                return mask;
            }
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
            // IsNotNull is exactly "the non-null rows", which is the primitive shared by
            // the LIKE "*" shortcut and the dictionary ALL fast path. The mayHaveNulls()
            // gate inside maskNonNullRows means a block with no nulls collapses to a
            // single setAll without scanning rows.
            return maskNonNullRows(block, rowCount);
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            Block block = blocks.get(ne.name());
            if (block == null) {
                return null;
            }
            // No dictionary cache threaded here: evaluateRange currently only handles numeric
            // blocks (Int/Long/Double), which are not dictionary-encoded. If ever extended to
            // BytesRef ranges over OrdinalBytesRefBlock, the cache would need to be wired in
            // alongside the other dictionary-aware predicate evaluators.
            return evaluateRange(range, block, rowCount);
        }
        if (expr instanceof And and) {
            WordMask left = evaluateExpression(and.left(), blocks, rowCount, intermediateMask, dictCache);
            // Nested-AND empty-mask short-circuit. Mirrors the top-level early exit in
            // evaluateFilter: once the left arm has eliminated every row in this batch, no
            // result from the right arm can rescue a row (AND is monotone over the survivor
            // set), so evaluating right is pure waste — notably for predicates that do a
            // per-batch dictionary scan ignoring the intermediateMask. The outer
            // evaluateFilter loop only short-circuits between top-level conjuncts; this
            // catches the same waste inside a planner-produced nested And. Note: we do not
            // additionally tighten the intermediateMask passed to right with left's bits
            // here, to avoid the per-And allocation; the existing intermediateMask
            // threaded from the outer loop already carries the cumulative narrowing across
            // previously evaluated top-level conjuncts.
            if (left != null && left.isEmpty()) {
                return left;
            }
            WordMask right = evaluateExpression(and.right(), blocks, rowCount, intermediateMask, dictCache);
            if (left != null && right != null) {
                left.and(right);
                return left;
            }
            return left != null ? left : right;
        }
        if (expr instanceof Or or) {
            WordMask left = evaluateExpression(or.left(), blocks, rowCount, intermediateMask, dictCache);
            WordMask right = evaluateExpression(or.right(), blocks, rowCount, intermediateMask, dictCache);
            if (left != null && right != null) {
                left.or(right);
                return left;
            }
            // conservative: if either arm is unknown, the whole OR is unknown
            return null;
        }
        if (expr instanceof Not not) {
            // NOT (LIKE-family) needs TVL: null rows must stay filtered out, so each LIKE-family
            // child routes through a tvlNegate helper instead of the generic bitwise negate below.
            // YES pushability of WildcardLike/Contains/EndsWith depends on this branch.
            if (not.field() instanceof WildcardLike wl) {
                Block block = namedBlock(wl.field(), blocks);
                return block == null ? null : evaluateNotWildcardLike(wl, block, rowCount, intermediateMask, dictCache);
            }
            if (not.field() instanceof StartsWith sw) {
                Block block = namedBlock(sw.singleValueField(), blocks);
                return block == null ? null : evaluateNotStartsWith(sw, block, rowCount, intermediateMask, dictCache);
            }
            if (not.field() instanceof Contains c) {
                Block block = namedBlock(c.singleValueField(), blocks);
                return block == null ? null : evaluateNotContains(c, block, rowCount, intermediateMask, dictCache);
            }
            if (not.field() instanceof EndsWith ew) {
                Block block = namedBlock(ew.singleValueField(), blocks);
                return block == null ? null : evaluateNotEndsWith(ew, block, rowCount, intermediateMask, dictCache);
            }
            WordMask inner = evaluateExpression(not.field(), blocks, rowCount, intermediateMask, dictCache);
            if (inner != null) {
                inner.negate();
                return inner;
            }
            return null;
        }
        if (expr instanceof StartsWith sw) {
            Block block = namedBlock(sw.singleValueField(), blocks);
            return block == null ? null : evaluateStartsWith(sw, block, rowCount, intermediateMask, dictCache);
        }
        if (expr instanceof Contains c) {
            Block block = namedBlock(c.singleValueField(), blocks);
            return block == null ? null : evaluateContains(c, block, rowCount, intermediateMask, dictCache);
        }
        if (expr instanceof EndsWith ew) {
            Block block = namedBlock(ew.singleValueField(), blocks);
            return block == null ? null : evaluateEndsWith(ew, block, rowCount, intermediateMask, dictCache);
        }
        if (expr instanceof WildcardLike wl) {
            Block block = namedBlock(wl.field(), blocks);
            return block == null ? null : evaluateWildcardLike(wl, block, rowCount, intermediateMask, dictCache);
        }
        return null;
    }

    /**
     * Returns the {@link Block} mapped from {@code field}'s column name, or {@code null} when
     * {@code field} is not a {@link NamedExpression} or no block is registered. Centralises the
     * "field-as-name → block" dispatch idiom used by every per-predicate branch above.
     */
    @Nullable
    private static Block namedBlock(Expression field, Map<String, Block> blocks) {
        if (field instanceof NamedExpression ne) {
            return blocks.get(ne.name());
        }
        return null;
    }

    private static WordMask evaluateComparison(
        EsqlBinaryComparison bc,
        Block block,
        Object literal,
        int rowCount,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
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
            // per row in favor of one int lookup per row. The dictionary content is fixed
            // within a row group, so the bitmap is memoized across batches when dictCache is
            // provided — typically reducing dictSize * batchCount predicate calls to dictSize
            // per row group.
            BytesRef val = toByteRef(literal);
            Predicate<BytesRef> matcher = bytesRefComparisonMatcher(bc, val);
            boolean[] dictMatches = memoizedDictionaryMatches(dictCache, bc, obb.getDictionaryVector(), matcher);
            applyDictionaryMatches(obb, dictMatches, mask, rowCount);
        } else if (block instanceof BytesRefBlock bb) {
            BytesRef val = toByteRef(literal);
            Predicate<BytesRef> matcher = bytesRefComparisonMatcher(bc, val);
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i) == false && matcher.test(bb.getBytesRef(i, scratch))) {
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

    /**
     * Returns the per-entry predicate for comparing a {@link BytesRef} block value to {@code val}.
     * For {@link Equals}/{@link NotEquals} this is {@link ByteMatchers#equals}, which routes
     * through the JDK's vectorized {@code Arrays#equals} intrinsic — the length pre-check rejects
     * the bulk of non-matching values without touching the byte content. For lexicographic
     * comparisons (LT, LE, GT, GE) the predicate falls back to {@link BytesRef#compareTo}, which
     * is the only correct semantic on UTF-8 byte order; the JDK still vectorizes the underlying
     * mismatch-finding step.
     */
    private static Predicate<BytesRef> bytesRefComparisonMatcher(EsqlBinaryComparison bc, BytesRef val) {
        if (bc instanceof Equals) {
            return entry -> ByteMatchers.equals(entry, val);
        }
        if (bc instanceof NotEquals) {
            return entry -> ByteMatchers.equals(entry, val) == false;
        }
        return entry -> compareResult(entry.compareTo(val), bc);
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

    private static WordMask evaluateIn(In inExpr, Block block, int rowCount, @Nullable Map<Expression, boolean[]> dictCache) {
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
            boolean[] dictMatches = memoizedDictionaryMatches(dictCache, inExpr, obb.getDictionaryVector(), refSet::contains);
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

    private static WordMask evaluateStartsWith(
        StartsWith sw,
        Block block,
        int rowCount,
        @Nullable WordMask intermediateMask,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
        Object prefixValue = literalValueOf(sw.prefix());
        if (prefixValue == null) {
            return null;
        }
        BytesRef prefix = toByteRef(prefixValue);
        // ByteMatchers#startsWith routes through Arrays#equals, which HotSpot intrinsifies on
        // x86 (AVX2/AVX-512) and ARM (NEON) with partial-inlining for sizes <= 64 bytes — typical
        // URL/path prefixes ("https://", "https://www.") fit comfortably in that fast path. The
        // helper performs the length pre-check internally.
        return evaluateLiteralPredicate(sw, block, rowCount, intermediateMask, dictCache, entry -> ByteMatchers.startsWith(entry, prefix));
    }

    private static WordMask evaluateEndsWith(
        EndsWith ew,
        Block block,
        int rowCount,
        @Nullable WordMask intermediateMask,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
        Object suffixValue = literalValueOf(ew.suffix());
        if (suffixValue == null) {
            return null;
        }
        BytesRef suffix = toByteRef(suffixValue);
        return evaluateLiteralPredicate(ew, block, rowCount, intermediateMask, dictCache, entry -> ByteMatchers.endsWith(entry, suffix));
    }

    private static WordMask evaluateContains(
        Contains c,
        Block block,
        int rowCount,
        @Nullable WordMask intermediateMask,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
        Object substrValue = literalValueOf(c.substr());
        if (substrValue == null) {
            return null;
        }
        BytesRef literal = toByteRef(substrValue);
        return evaluateLiteralPredicate(
            c,
            block,
            rowCount,
            intermediateMask,
            dictCache,
            entry -> ByteMatchers.containsLiteral(entry, literal)
        );
    }

    /**
     * Shared mask-builder for the LIKE-family literal predicates (StartsWith, EndsWith, Contains).
     * The dictionary path ignores {@code intermediateMask}, the scalar path honors it (same
     * contract as evaluateWildcardLike).
     *
     * <p><b>Cache key.</b> {@code expr} is used as an <em>identity</em> key in {@code dictCache}
     * (intentional, matching the WildcardLike path): two structurally-equal but distinct
     * expression instances get separate cache slots. Switching to value-equality keying would
     * be correctness-safe but is a deliberate non-goal — it would couple cache reuse to ESQL
     * expression equality semantics, which evolve independently of this evaluator.
     */
    private static WordMask evaluateLiteralPredicate(
        Expression expr,
        Block block,
        int rowCount,
        @Nullable WordMask intermediateMask,
        @Nullable Map<Expression, boolean[]> dictCache,
        Predicate<BytesRef> matcher
    ) {
        if (block instanceof OrdinalBytesRefBlock obb && shouldShortCircuitOnDictionary(obb)) {
            WordMask mask = new WordMask();
            mask.reset(rowCount);
            boolean[] dictMatches = memoizedDictionaryMatches(dictCache, expr, obb.getDictionaryVector(), matcher);
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
                    if (matcher.test(val)) {
                        mask.set(i);
                    }
                }
            }
            return mask;
        }
        return null;
    }

    /**
     * Adds TVL-correct null handling to an already-computed match mask from a LIKE-family
     * predicate, then negates. Shared by {@code NOT (Contains)} and {@code NOT (EndsWith)};
     * {@link #evaluateNotWildcardLike} inlines the same algorithm. Returns {@code null} when
     * {@code likeMask} is {@code null} (block type unsupported), preserving the conservative
     * "all rows survive" sentinel.
     */
    @Nullable
    private static WordMask tvlNegate(@Nullable WordMask likeMask, Block block, int rowCount) {
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
    private WordMask evaluateWildcardLike(
        WildcardLike wl,
        Block block,
        int rowCount,
        @Nullable WordMask intermediateMask,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
        CompiledWildcard compiled = automatonFor(wl);
        if (compiled.matcher == null) {
            return null;
        }
        if (compiled.matchesAll) {
            return maskNonNullRows(block, rowCount);
        }
        // Use the affix-contains dispatch when the pattern matches that shape; see CompiledWildcard.
        Predicate<BytesRef> matcher = matcherFor(compiled);
        if (block instanceof OrdinalBytesRefBlock obb && shouldShortCircuitOnDictionary(obb)) {
            WordMask mask = new WordMask();
            mask.reset(rowCount);
            boolean[] dictMatches = memoizedDictionaryMatches(dictCache, wl, obb.getDictionaryVector(), matcher);
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
                    if (matcher.test(val)) {
                        mask.set(i);
                    }
                }
            }
            return mask;
        }
        return null;
    }

    private static Predicate<BytesRef> matcherFor(CompiledWildcard compiled) {
        WildcardLikeShape shape = compiled.shape();
        if (shape != null) {
            BytesRef prefix = shape.prefix();
            BytesRef literal = shape.literal();
            BytesRef suffix = shape.suffix();
            return entry -> ByteMatchers.affixContains(entry, prefix, literal, suffix);
        }
        ByteRunAutomaton runner = compiled.matcher;
        return entry -> runner.run(entry.bytes, entry.offset, entry.length);
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
     *
     * <p>Unlike {@link #evaluateNotContains} / {@link #evaluateNotEndsWith}, this method is an
     * instance method because it delegates to {@link #evaluateWildcardLike}, which reads the
     * per-instance automaton/affix cache populated by {@link #automatonFor(WildcardLike)}.
     */
    private WordMask evaluateNotWildcardLike(
        WildcardLike wl,
        Block block,
        int rowCount,
        @Nullable WordMask intermediateMask,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
        return tvlNegate(evaluateWildcardLike(wl, block, rowCount, intermediateMask, dictCache), block, rowCount);
    }

    private static WordMask evaluateNotStartsWith(
        StartsWith sw,
        Block block,
        int rowCount,
        @Nullable WordMask intermediateMask,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
        return tvlNegate(evaluateStartsWith(sw, block, rowCount, intermediateMask, dictCache), block, rowCount);
    }

    private static WordMask evaluateNotContains(
        Contains c,
        Block block,
        int rowCount,
        @Nullable WordMask intermediateMask,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
        return tvlNegate(evaluateContains(c, block, rowCount, intermediateMask, dictCache), block, rowCount);
    }

    private static WordMask evaluateNotEndsWith(
        EndsWith ew,
        Block block,
        int rowCount,
        @Nullable WordMask intermediateMask,
        @Nullable Map<Expression, boolean[]> dictCache
    ) {
        return tvlNegate(evaluateEndsWith(ew, block, rowCount, intermediateMask, dictCache), block, rowCount);
    }

    /**
     * Returns a mask with one bit set per non-null position. Used as the {@code matchesAll()}
     * shortcut in {@link #evaluateWildcardLike} — {@code LIKE "*"} accepts every value but, by
     * SQL three-valued-logic semantics, still rejects nulls.
     */
    private static WordMask maskNonNullRows(Block block, int rowCount) {
        WordMask mask = new WordMask();
        maskNonNullRowsInto(block, rowCount, mask);
        return mask;
    }

    /**
     * Populates {@code mask} so that bit {@code i} is set iff {@code block.isNull(i)} is
     * false. The caller owns the mask; this method does not allocate. Mirrors
     * {@link #maskNonNullRows} which is the allocating wrapper. Both the {@code IsNotNull}
     * predicate path and the dictionary fast-path {@code ALL} branch share this primitive
     * so the {@code mayHaveNulls() == false} shortcut and the null-scan loop are written
     * exactly once.
     */
    private static void maskNonNullRowsInto(Block block, int rowCount, WordMask mask) {
        if (block.mayHaveNulls() == false) {
            mask.setAll(rowCount);
            return;
        }
        mask.reset(rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i) == false) {
                mask.set(i);
            }
        }
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
                // Affix-contains shape detection is opt-in for case-sensitive patterns. Skip the
                // matchesAll case (already handled by an upstream shortcut) so the shape only
                // exists on the SIMD-eligible path; this keeps the dispatcher contract trivial.
                WildcardLikeShape shape = (wl.caseInsensitive() || matchesAll) ? null : WildcardLikeShape.of(wl.pattern().pattern());
                compiled = new CompiledWildcard(new ByteRunAutomaton(automaton), matchesAll, shape);
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
     * boolean array indexed by ordinal — {@code true} at position {@code k} means the entry
     * at ordinal {@code k} satisfies the predicate. This is the core of the dictionary
     * short-circuit: we run the predicate once per unique entry rather than once per row.
     */
    private static boolean[] matchingDictionaryEntries(BytesRefVector dictionary, Predicate<BytesRef> matcher) {
        int size = dictionary.getPositionCount();
        boolean[] matches = UninitializedArrays.newBooleanArray(size);
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < size; i++) {
            matches[i] = matcher.test(dictionary.getBytesRef(i, scratch));
        }
        return matches;
    }

    /**
     * Returns the dictionary-match bitmap for {@code key}, computing it on first use and
     * reusing the cached array on every subsequent batch within the row group whose lifecycle
     * the caller's map represents. A {@code null} cache falls through to a fresh per-call
     * computation; the cache is null for unit tests and for the multi-stage single-expression
     * path that does not have a row-group lifecycle.
     */
    private static boolean[] memoizedDictionaryMatches(
        @Nullable Map<Expression, boolean[]> cache,
        Expression key,
        BytesRefVector dictionary,
        Predicate<BytesRef> matcher
    ) {
        if (cache == null) {
            return matchingDictionaryEntries(dictionary, matcher);
        }
        boolean[] cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        boolean[] fresh = matchingDictionaryEntries(dictionary, matcher);
        cache.put(key, fresh);
        return fresh;
    }

    /**
     * Sets bits in {@code mask} for rows whose dictionary ordinal is flagged in
     * {@code dictMatches}, skipping null rows.
     *
     * <p>Before walking the per-row ordinal indirection, this method scans the (typically
     * small) {@code dictMatches} array once to detect two bulk-action shapes that arise
     * routinely on real-world data:
     * <ul>
     *   <li><b>No dictionary entry matches</b> — every non-null row's ordinal points at a
     *       {@code false}, so every row is filtered out. The pre-zeroed mask is already
     *       correct; we return immediately and skip the {@code rowCount} ordinal lookups.
     *       This is the dictionary-level analogue of the empty-mask early exit in
     *       {@link #evaluateFilter}: the outer loop sees the resulting empty mask and
     *       short-circuits the remaining expressions for this batch.</li>
     *   <li><b>Every dictionary entry matches</b> — every non-null row passes. We delegate
     *       to {@link #maskNonNullRows}, which uses {@link WordMask#setAll} when the block
     *       has no nulls and a single-pass null scan otherwise. This is the same primitive
     *       the {@code matchesAll} fast path in {@link #evaluateWildcardLike} already uses,
     *       so this branch unifies the {@code LIKE "*"} shortcut with predicates that
     *       happen to accept every entry in the current row group's dictionary (e.g.
     *       {@code col != ""} on a column whose dictionary holds no empty strings, or
     *       {@code col IN (...)} on a small column whose dictionary is a subset of the set).
     *       The win is concrete: a 10K-entry dictionary is scanned in microseconds, while
     *       an 8K-row ordinal-to-boolean mapping costs tens of microseconds per batch.</li>
     * </ul>
     * Plan-time row-group statistics rule out many of these cases, but not all (e.g. they
     * cannot prove "no empty strings" when {@code numNulls > 0}, and they cannot reason
     * about {@code LIKE} patterns at all). The dictionary-level check closes those gaps
     * without compromising correctness — we are looking at the actual per-row-group
     * dictionary, not at file-level metadata.
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
        DictionaryMatchShape shape = classifyDictionaryMatches(dictMatches);
        if (shape == DictionaryMatchShape.NONE) {
            return; // mask is already zero-initialized via reset(rowCount); no row can pass
        }
        if (shape == DictionaryMatchShape.ALL) {
            // Every non-null row's ordinal points at a true entry, so the survivor set
            // collapses to "non-null rows" — the same primitive the LIKE "*" shortcut and
            // IsNotNull already use. Write directly into the caller's mask to avoid the
            // extra WordMask allocation in this hot path.
            maskNonNullRowsInto(block, rowCount, mask);
            return;
        }
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i) == false && dictMatches[ordinals.getInt(i)]) {
                mask.set(i);
            }
        }
    }

    /**
     * Classifies a {@code dictMatches} bitmap as {@code NONE} (no entry matches), {@code ALL}
     * (every entry matches), or {@code MIXED}. Empty arrays are treated as {@code NONE} so
     * the caller's "no row can pass" branch fires — there are no ordinals to look up.
     *
     * <p>Single-pass with early exit as soon as both polarities are observed: the worst-case
     * cost is bounded by the dictionary size, but for the {@code MIXED} case we typically
     * exit after a handful of comparisons. Compared to the {@code O(rowCount)} per-row loop
     * the caller would otherwise run, this is a low-cost gate even when it returns
     * {@code MIXED} and the per-row loop still executes.
     */
    enum DictionaryMatchShape {
        NONE,
        ALL,
        MIXED
    }

    static DictionaryMatchShape classifyDictionaryMatches(boolean[] dictMatches) {
        boolean sawTrue = false;
        boolean sawFalse = false;
        for (boolean m : dictMatches) {
            if (m) {
                sawTrue = true;
            } else {
                sawFalse = true;
            }
            if (sawTrue && sawFalse) {
                return DictionaryMatchShape.MIXED;
            }
        }
        if (sawTrue) {
            return DictionaryMatchShape.ALL;
        }
        return DictionaryMatchShape.NONE;
    }
}
