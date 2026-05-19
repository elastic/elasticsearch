/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Parquet-specific filter pushdown support that validates ESQL filter expressions
 * for pushdown eligibility and collects them into a {@link ParquetPushedExpressions}
 * wrapper for deferred translation at read time.
 * <p>
 * Translation to Parquet {@link org.apache.parquet.filter2.predicate.FilterPredicate}
 * is deferred because DATETIME columns can have different physical representations
 * across files (INT32 DATE, INT64 TIMESTAMP_MILLIS/MICROS/NANOS, INT96). The actual
 * file schema is needed for correct value conversion.
 * <p>
 * When set on {@link org.apache.parquet.ParquetReadOptions}, parquet-java automatically
 * applies four levels of filtering:
 * <ol>
 *   <li>Statistics (min/max) — skips row groups where value is outside range</li>
 *   <li>Dictionary — skips row groups where dictionary-encoded column doesn't contain value</li>
 *   <li>Bloom filter — skips row groups where bloom filter says value definitely absent</li>
 *   <li>Page index (ColumnIndex/OffsetIndex) — skips individual pages within row groups using
 *       per-page min/max statistics (active by default via {@code useColumnIndexFilter=true}
 *       since parquet-mr 1.12.0)</li>
 * </ol>
 * <p>
 * <b>Pushability semantics.</b> Most pushed filters use {@link Pushability#RECHECK}: the original
 * filter remains in {@code FilterExec} for per-row correctness because the per-row evaluator
 * (see {@link ParquetPushedExpressions#evaluateFilter}) is two-valued — nulls and unknowns map
 * to bit {@code 0}, which is correct for the predicate itself but not for a wrapping {@code Not}.
 * <p>
 * {@link WildcardLike} (and {@code Not(WildcardLike)}, and conjunctions thereof) are exceptions:
 * they push as {@link Pushability#YES} so {@code FilterExec} can be dropped entirely. The late-mat
 * evaluator handles {@code NOT (col LIKE p)} with three-valued logic by AND-ing out nulls before
 * negation (see {@link ParquetPushedExpressions#evaluateExpression}'s {@code Not(WildcardLike)}
 * special case), so removing the safety net does not change result semantics. The motivation: with
 * {@code RECHECK}, every surviving row pays the LIKE cost twice — once in the reader's late-mat
 * filter, once again in {@code FilterExec}. On large keyword scans (e.g. {@code URL LIKE
 * "*google*"} on the public hits dataset) this duplicate evaluation is the dominant CPU cost.
 */
public class ParquetFilterPushdownSupport implements FilterPushdownSupport {

    private static final Logger logger = LogManager.getLogger(ParquetFilterPushdownSupport.class);

    static final Predicate<DataType> TYPE_SUPPORTED = dt -> dt == DataType.INTEGER
        || dt == DataType.LONG
        || dt == DataType.DOUBLE
        || dt == DataType.KEYWORD
        || dt == DataType.BOOLEAN
        || dt == DataType.DATETIME;

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        List<Expression> pushed = new ArrayList<>();
        // Only RECHECK conjuncts need to remain in FilterExec; YES conjuncts are guaranteed
        // TVL-correct by the late-mat evaluator and can be dropped from the plan entirely
        // (see canPush for the per-expression rule and the class-level Javadoc for the
        // motivation: avoiding double LIKE evaluation on every surviving row).
        List<Expression> remainder = new ArrayList<>();
        for (Expression filter : filters) {
            Pushability p = canPush(filter);
            if (p == Pushability.YES) {
                pushed.add(filter);
            } else if (p == Pushability.RECHECK) {
                pushed.add(filter);
                remainder.add(filter);
            } else {
                remainder.add(filter);
            }
        }

        if (pushed.isEmpty()) {
            return PushdownResult.none(filters);
        }

        logger.debug(
            "Parquet filter pushdown: pushed {} of {} expressions ({} need re-check in FilterExec)",
            pushed.size(),
            filters.size(),
            remainder.size()
        );
        return new PushdownResult(new ParquetPushedExpressions(pushed), pushed, remainder);
    }

    @Override
    public Pushability canPush(Expression expr) {
        if (canConvert(expr) == false) {
            return Pushability.NO;
        }
        if (isFullyEvaluable(expr) == false) {
            return Pushability.RECHECK;
        }
        // YES requires the late-mat evaluator to be exactly equivalent to FilterExec for this
        // expression. If a WildcardLike inside the tree fails to determinize at runtime, the
        // evaluator returns null ("all rows survive"), which without FilterExec means false
        // positives. Probe every WildcardLike now so any TooComplexToDeterminize is caught at
        // plan time and we can fall back to RECHECK before FilterExec is removed from the plan.
        // The probe cost is negligible — it is a single per-pattern automaton build done once
        // per query at plan time, and the result is cached for runtime via the evaluator's
        // own per-instance cache (different cache, same cost class).
        return canCompileAllPatterns(expr) ? Pushability.YES : Pushability.RECHECK;
    }

    private static boolean canCompileAllPatterns(Expression expr) {
        if (expr instanceof WildcardLike wl) {
            try {
                wl.pattern().createAutomaton(wl.caseInsensitive());
                return true;
            } catch (IllegalArgumentException | TooComplexToDeterminizeException e) {
                logger.debug(
                    "WildcardLike pattern [{}] cannot be determinized at plan time; falling back to RECHECK",
                    wl.pattern().pattern(),
                    e
                );
                return false;
            }
        }
        if (expr instanceof Not not) {
            return canCompileAllPatterns(not.field());
        }
        if (expr instanceof And and) {
            return canCompileAllPatterns(and.left()) && canCompileAllPatterns(and.right());
        }
        return true;
    }

    /**
     * Returns {@code true} when the late-mat evaluator produces a survivor mask that is
     * <em>SQL three-valued-logic correct</em> for {@code expr}, i.e. equivalent to what
     * {@code FilterExec} would compute for the same expression. Such expressions can be
     * dropped from {@code FilterExec} (pushed as {@link Pushability#YES}); others must be
     * re-checked downstream ({@link Pushability#RECHECK}).
     *
     * <p>The two-valued bitmask path is TVL-correct by construction for predicates that
     * map nulls to bit {@code 0}: {@link WildcardLike} on a non-null value matches the
     * automaton; on a null value it sets bit {@code 0}. The remaining work is to ensure
     * {@code Not(WildcardLike)} also stays TVL-correct — the late-mat evaluator does this
     * by AND-ing out the null mask before negating
     * (see {@link ParquetPushedExpressions#evaluateExpression}).
     *
     * <p><b>Why {@code Not} only allows a bare {@link WildcardLike} child.</b> The evaluator's
     * generic {@code Not} branch is a bitwise complement on the inner mask. For
     * {@code Not(And(...))} that means {@code ~(m1 & m2)}, which is <em>not</em>
     * {@code NOT (a AND b)} under SQL three-valued logic — e.g. {@code (NULL AND TRUE)} is
     * {@code UNKNOWN} (mask bit 0); the bitwise complement flips it to 1, so the row
     * incorrectly survives. Only {@code Not(WildcardLike)} has a TVL-aware special case in
     * {@link ParquetPushedExpressions#evaluateExpression} that AND-s out the null mask before
     * negation. Anything else under {@code Not} stays {@link Pushability#RECHECK} so that
     * {@code FilterExec} fixes the null handling.
     *
     * <p>Conjunctions of YES-eligible predicates are themselves YES-eligible: {@code AND} of
     * TVL-correct masks is TVL-correct (a 0 bit on either side blocks the row, regardless of
     * whether it came from "no-match" or "null"). {@code OR} is intentionally excluded
     * because {@code evaluateExpression}'s {@code Or} branch returns {@code null} (i.e.
     * "all rows survive") when an arm is unevaluable — fine for {@link Pushability#RECHECK}
     * (where {@code FilterExec} re-checks), but unsafe for {@link Pushability#YES}.
     *
     * <p>Other predicate families ({@code Eq}, {@code In}, {@code Range}, {@code StartsWith},
     * {@code IsNull}, {@code IsNotNull}) remain {@link Pushability#RECHECK}; their {@code Not}
     * handling has the same two-valued-mask null bug as LIKE used to, and is left unchanged
     * to keep this PR focused on the LIKE win that motivated the work.
     */
    static boolean isFullyEvaluable(Expression expr) {
        if (expr instanceof WildcardLike) {
            return true;
        }
        if (expr instanceof Not not) {
            // Only Not(WildcardLike) has a TVL-aware evaluator special case. Any other inner
            // expression would fall through to the generic two-valued negate, which is unsafe
            // under YES (see Javadoc above for the And-under-Not example).
            return not.field() instanceof WildcardLike;
        }
        if (expr instanceof And and) {
            return isFullyEvaluable(and.left()) && isFullyEvaluable(and.right());
        }
        return false;
    }

    /**
     * Validates whether an expression can be converted to a Parquet FilterPredicate.
     * For AND, partial pushdown is safe (at least one side convertible).
     * For OR and NOT, all children must be convertible.
     */
    static boolean canConvert(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc) {
            if (PushdownPredicates.isComparison(bc, TYPE_SUPPORTED) == false) {
                return false;
            }
            // SQL null comparisons are always UNKNOWN — only IsNull/IsNotNull handle nulls
            if (bc.right().foldable() && literalValueOf(bc.right()) == null) {
                return false;
            }
            // BooleanColumn doesn't implement SupportsLtGt — only eq/notEq are valid
            if (bc.left() instanceof NamedExpression ne && ne.dataType() == DataType.BOOLEAN) {
                return bc instanceof Equals || bc instanceof NotEquals;
            }
            return true;
        }
        if (expr instanceof In inExpr) {
            return PushdownPredicates.isIn(inExpr, TYPE_SUPPORTED);
        }
        if (expr instanceof IsNull isNull) {
            return PushdownPredicates.isIsNull(isNull, TYPE_SUPPORTED);
        }
        if (expr instanceof IsNotNull isNotNull) {
            return PushdownPredicates.isIsNotNull(isNotNull, TYPE_SUPPORTED);
        }
        if (expr instanceof Range range) {
            // BooleanColumn doesn't implement SupportsLtGt — Range requires ordered comparisons
            if (range.value() instanceof NamedExpression ne && ne.dataType() == DataType.BOOLEAN) {
                return false;
            }
            return PushdownPredicates.isRange(range, TYPE_SUPPORTED);
        }
        if (expr instanceof And and) {
            return canConvert(and.left()) || canConvert(and.right());
        }
        if (expr instanceof Or or) {
            return canConvert(or.left()) && canConvert(or.right());
        }
        if (expr instanceof Not not) {
            return canConvert(not.field());
        }
        if (expr instanceof StartsWith sw) {
            return PushdownPredicates.isStartsWith(sw, dt -> dt == DataType.KEYWORD) && literalValueOf(sw.prefix()) != null;
        }
        if (expr instanceof WildcardLike wl) {
            // Parquet has no native LIKE support; this pushdown evaluates the pattern during late
            // materialization (see ParquetPushedExpressions#evaluateWildcardLike) so the reader can
            // skip decoding projection columns for non-matching rows. Only KEYWORD-typed fields with
            // a non-null pattern qualify; the dictionary short-circuit collapses the per-row automaton
            // run to one run per dictionary entry. The wl.pattern() != null check is structurally
            // unreachable today (WildcardLike's constructor takes a non-null WildcardPattern), but is
            // kept as a cheap boundary guard so an upstream regression cannot turn into an NPE in the
            // per-batch evaluator. Mirrors the prefix() != null guard on StartsWith above.
            return wl.field() instanceof NamedExpression ne && ne.dataType() == DataType.KEYWORD && wl.pattern() != null;
        }
        return false;
    }
}
