/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;

/**
 * A left-semi (mark) join used to implement {@code field IN (subquery)} when the {@code IN} is
 * embedded in an arbitrary boolean expression — typically as a child of {@code OR} where
 * {@link SemiJoin} / {@link AntiJoin}'s row-filtering shape is not applicable.
 * <p>
 * Unlike {@link SemiJoin}, this operator preserves every left row and adds a single boolean
 * <em>mark</em> attribute that captures the value of {@code field IN (subquery)} for that row
 * with full three-valued logic:
 * <ul>
 *   <li>{@code TRUE} — left key matches at least one row in the subquery</li>
 *   <li>{@code FALSE} — no match, left key non-NULL, subquery had no NULLs</li>
 *   <li>{@code NULL} — left key is NULL, OR no match but the subquery contains NULL(s)</li>
 * </ul>
 * The mark is a normal boolean attribute; the original WHERE condition references it via
 * the InSubqueryResolver's rewrite, so {@code OR}, {@code AND}, {@code NOT} and other boolean
 * operators in the surrounding expression are evaluated by the standard expression machinery —
 * preserving the SQL semantics that the previous OR-rewrite-to-UnionAll approach violated when
 * NULLs were involved.
 * <p>
 * Like {@link SemiJoin}, the right side is an independent subquery executed first; once its
 * result arrives as a {@link LocalRelation}, {@link SemiJoin#inlineData} routes the dedup result
 * into one of this class's {@link #buildFilterPathPlan} / {@link #buildHashJoinPathPlan} hooks
 * which substitute an {@link Eval} that materializes the mark.
 */
public class LeftSemiJoin extends SemiJoin {

    /**
     * Mark attribute exposed by this join. The surrounding {@link Filter} condition references
     * this attribute (placed there by {@link org.elasticsearch.xpack.esql.analysis.InSubqueryResolver
     * InSubqueryResolver} as the substitute for the original {@code InSubquery} expression). When
     * {@link SemiJoin#inlineData} converts this node into an {@link Eval}, the {@link Alias} it
     * produces shares this attribute's {@link org.elasticsearch.xpack.esql.core.expression.NameId
     * NameId} so existing references continue to resolve.
     */
    private final Attribute markAttribute;

    public LeftSemiJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config, Attribute markAttribute) {
        super(source, left, right, config);
        this.markAttribute = markAttribute;
    }

    public LeftSemiJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Attribute markAttribute
    ) {
        super(source, left, right, JoinTypes.LEFT_SEMI, leftFields, rightFields);
        this.markAttribute = markAttribute;
    }

    public Attribute markAttribute() {
        return markAttribute;
    }

    @Override
    protected NodeInfo<Join> info() {
        JoinConfig config = config();
        return NodeInfo.create(this, LeftSemiJoin::new, left(), right(), config.leftFields(), config.rightFields(), markAttribute);
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new LeftSemiJoin(source(), left, right, config(), markAttribute);
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        List<NamedExpression> output = new ArrayList<>(left);
        output.add(markAttribute);
        return output;
    }

    // -- inlineData hook overrides ---------------------------------------------------------------
    // Each path produces an Eval that materializes the mark attribute (sharing the synthetic
    // NameId allocated by the resolver), preserving every left row.

    @Override
    protected LogicalPlan buildEmptyRightSidePlan(Source source) {
        // Subquery returned no rows → x IN () is FALSE for every (non-NULL) row, but NULL keys
        // still yield NULL. With an empty right side there are no NULL right values, so the mark
        // is just FALSE for every row regardless of x. (NULL IN () is also commonly defined as
        // FALSE — there is no NULL on the right to propagate — and matches the SemiJoin path
        // which collapses to Filter(FALSE) here.)
        return new Eval(source, left(), List.of(markAlias(source, Literal.FALSE)));
    }

    @Override
    protected LogicalPlan buildShortCircuitPlan(Source source, boolean allRightNull) {
        // For LEFT_SEMI {@link #shortCircuitOnAnyRightNull()} stays false, so this is only reached
        // when every right value is NULL. The mark is then NULL for every row (no match possible
        // and the right contains a NULL).
        return new Eval(source, left(), List.of(markAlias(source, new Literal(source, null, DataType.BOOLEAN))));
    }

    @Override
    protected LogicalPlan buildFilterPathPlan(
        Block dedupKeys,
        DataType keyType,
        Attribute leftField,
        Source source,
        boolean rightHadNulls
    ) {
        // The natural three-valued semantics of {@link In} produce exactly the mark we want:
        // match → TRUE; no match, non-NULL key → FALSE; NULL key → NULL;
        // no match with NULL in the literal list → NULL.
        // We strip NULLs from the dedup input for efficiency, so we re-introduce a single NULL
        // literal here when {@code rightHadNulls} so In's no-match-with-NULL-in-list branch fires.
        int positionCount = dedupKeys.getPositionCount();
        List<Expression> literals = new ArrayList<>(positionCount + (rightHadNulls ? 1 : 0));
        for (int i = 0; i < positionCount; i++) {
            literals.add(new Literal(source, toJavaObject(dedupKeys, i), keyType));
        }
        if (rightHadNulls) {
            literals.add(new Literal(source, null, keyType));
        }
        Expression in = new In(source, leftField, literals);
        return new Eval(source, left(), List.of(markAlias(source, in)));
    }

    @Override
    protected LogicalPlan buildHashJoinPathPlan(
        LogicalPlan leftSide,
        LocalRelation deduplicatedData,
        JoinConfig leftJoinConfig,
        Attribute sentinelAttr,
        Source source,
        boolean rightHadNulls
    ) {
        // LEFT join → Eval($mark = CASE …) → Project(left output + mark).
        // The CASE turns the join's sentinel-IS-NOT-NULL match indicator plus the
        // right-had-NULLs flag into a three-valued mark.
        Join leftJoin = new Join(source, leftSide, deduplicatedData, leftJoinConfig);
        Expression caseExpr = buildMarkCase(source, sentinelAttr, rightHadNulls);
        Eval eval = new Eval(source, leftJoin, List.of(markAlias(source, caseExpr)));
        // Drop the right-side columns (the sentinel; the join key is already dropped by LEFT
        // join semantics). Keep the original left output and append the mark attribute so
        // downstream Filter/Project nodes see exactly what the LeftSemiJoin's output schema
        // promised.
        List<NamedExpression> projection = new ArrayList<>(left().output());
        projection.add(markAttribute);
        return new Project(source, eval, projection);
    }

    @Override
    protected boolean filterNullLeftKeysBeforeHashJoin() {
        // We must keep NULL-keyed rows in the output (with mark=NULL); the CASE in
        // {@link #buildMarkCase} handles the NULL-key case explicitly.
        return false;
    }

    /**
     * Build the CASE expression that converts the LEFT-join sentinel + the right-had-NULLs flag
     * into the three-valued mark.
     * <ul>
     *   <li>{@code rightHadNulls = false}:
     *       {@code CASE WHEN leftField IS NULL THEN NULL WHEN sentinel IS NOT NULL THEN TRUE ELSE FALSE END}
     *       — explicit NULL-key check is required because the dedup right side has no NULLs and
     *       the join would otherwise route the row to the FALSE branch.</li>
     *   <li>{@code rightHadNulls = true}:
     *       {@code CASE WHEN sentinel IS NOT NULL THEN TRUE END} — no else clause, so non-matches
     *       (including NULL-keyed rows whose sentinel is also NULL) produce the implicit NULL.</li>
     * </ul>
     */
    private Expression buildMarkCase(Source source, Attribute sentinelAttr, boolean rightHadNulls) {
        Expression matched = new IsNotNull(source, sentinelAttr);
        if (rightHadNulls) {
            // Two children → 1 condition, no else → implicit NULL on no-match.
            return new Case(source, matched, List.of(Literal.TRUE));
        }
        Attribute leftField = config().leftFields().get(0);
        Expression leftIsNull = new IsNull(source, leftField);
        Expression nullLit = new Literal(source, null, DataType.BOOLEAN);
        // (leftIsNull, NULL), (matched, TRUE), else FALSE
        return new Case(source, leftIsNull, List.of(nullLit, matched, Literal.TRUE, Literal.FALSE));
    }

    /**
     * Build an {@link Alias} that materializes the mark attribute. The alias inherits the
     * mark's name and {@link org.elasticsearch.xpack.esql.core.expression.NameId NameId}, so
     * references previously placed by {@link org.elasticsearch.xpack.esql.analysis.InSubqueryResolver
     * InSubqueryResolver} resolve to the value produced here.
     */
    private Alias markAlias(Source source, Expression value) {
        return new Alias(source, markAttribute.name(), value, markAttribute.id(), true);
    }
}
