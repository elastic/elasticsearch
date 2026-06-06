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
import java.util.Objects;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;

/**
 * A mark join used to implement {@code field IN (subquery)} when the {@code IN} is embedded in an
 * arbitrary boolean expression — typically as a child of {@code OR} where {@link SemiJoin} /
 * {@link AntiJoin}'s row-filtering shape is not applicable.
 * <p>
 * Unlike {@link SemiJoin}, this operator preserves every left row and adds a single boolean
 * <em>mark</em> attribute holding the value of {@code field IN (subquery)} for that row, with full
 * three-valued logic:
 * <ul>
 *   <li>{@code TRUE} — left key matches at least one row in the subquery</li>
 *   <li>{@code FALSE} — no match, left key non-NULL, subquery had no NULLs</li>
 *   <li>{@code NULL} — left key is NULL, OR no match but the subquery contains NULL(s)</li>
 * </ul>
 * The mark is a normal boolean attribute that the rewritten WHERE condition references, so the
 * surrounding {@code OR}/{@code AND}/{@code NOT} operators are evaluated by the standard expression
 * machinery.
 * <p>
 * Like {@link SemiJoin}, the right side is an independent subquery executed first; once its result
 * arrives as a {@link LocalRelation}, {@link SemiJoin#inlineData} routes the dedup result into the
 * {@link #buildFilterPathPlan} / {@link #buildHashJoinPathPlan} hooks, which substitute an
 * {@link Eval} that materializes the mark.
 */
public class MarkJoin extends SemiJoin {

    /**
     * Mark attribute exposed by this join. The surrounding {@link Filter} condition references it
     * (placed there by {@link org.elasticsearch.xpack.esql.analysis.InSubqueryResolver} as the
     * substitute for the original {@code InSubquery}). When {@link SemiJoin#inlineData} converts
     * this node into an {@link Eval}, the {@link Alias} it produces shares this attribute's
     * {@link org.elasticsearch.xpack.esql.core.expression.NameId} so existing references resolve.
     */
    private final Attribute markAttribute;

    public MarkJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config, Attribute markAttribute) {
        super(source, left, right, config);
        assert config.type() == JoinTypes.MARK : "MarkJoin requires join type MARK, got [" + config.type() + "]";
        this.markAttribute = markAttribute;
    }

    public MarkJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Attribute markAttribute
    ) {
        super(source, left, right, JoinTypes.MARK, leftFields, rightFields);
        this.markAttribute = markAttribute;
    }

    public Attribute markAttribute() {
        return markAttribute;
    }

    @Override
    protected NodeInfo<Join> info() {
        JoinConfig config = config();
        return NodeInfo.create(this, MarkJoin::new, left(), right(), config.leftFields(), config.rightFields(), markAttribute);
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new MarkJoin(source(), left, right, config(), markAttribute);
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
        // Subquery returned no rows → the mark is FALSE for every row (no NULL on the right to
        // propagate, so even NULL keys yield FALSE).
        return new Eval(source, left(), List.of(markAlias(source, Literal.FALSE)));
    }

    @Override
    protected LogicalPlan buildShortCircuitPlan(Source source, boolean allRightNull) {
        // Reached only when every right value is NULL (MARK keeps shortCircuitOnAnyRightNull false).
        // No match is possible and the right contains a NULL, so the mark is NULL for every row.
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
        // The three-valued semantics of {@link In} produce exactly the mark we want: match → TRUE;
        // no match, non-NULL key → FALSE; NULL key → NULL; no match with a NULL literal → NULL.
        // BlockHash collapses every NULL into its reserved group 0, so the deduplicated keys
        // already carry a NULL at index 0 when {@code rightHadNulls}; the loop emits it naturally.
        int positionCount = dedupKeys.getPositionCount();
        List<Expression> literals = new ArrayList<>(positionCount);
        for (int i = 0; i < positionCount; i++) {
            literals.add(new Literal(source, toJavaObject(dedupKeys, i), keyType));
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
        // LEFT join → Eval($mark = CASE …) → Project(original left output + mark).
        //
        // {@link SemiJoin#inlineAsHashJoin} has already wrapped the left side in an
        // {@code Eval(svKey = MvSingleValueOrNull(leftField))} so multi-valued positions become
        // NULL before the join. {@code leftJoinConfig.leftFields()} therefore refers to the
        // SV-guarded attribute, which we use in the CASE so NULL/MV keys collapse to mark=NULL.
        Join leftJoin = new Join(source, leftSide, deduplicatedData, leftJoinConfig);
        Attribute svKeyAttr = leftJoinConfig.leftFields().get(0);
        Expression caseExpr = buildMarkCase(source, svKeyAttr, sentinelAttr, rightHadNulls);
        Eval eval = new Eval(source, leftJoin, List.of(markAlias(source, caseExpr)));
        // Keep the original left output and append the mark; drop the synthetic SV-guard attribute.
        // Use {@code left().output()} rather than {@code leftSide.output()} because the latter now
        // includes the SV-guard attribute.
        List<NamedExpression> projection = new ArrayList<>(left().output());
        projection.add(markAttribute);
        return new Project(source, eval, projection);
    }

    @Override
    protected boolean filterNullLeftKeysBeforeHashJoin() {
        // NULL-keyed rows must survive (with mark=NULL); the CASE handles them explicitly.
        return false;
    }

    /**
     * Build the CASE expression that turns the LEFT-join sentinel plus the right-had-NULLs flag
     * into the three-valued mark. {@code keyAttr} is the SV-guarded left key, whose NULL covers
     * both originally NULL keys and originally multi-valued keys.
     * <ul>
     *   <li>{@code rightHadNulls = false}:
     *       {@code CASE WHEN keyAttr IS NULL THEN NULL WHEN sentinel IS NOT NULL THEN TRUE ELSE FALSE END}
     *       — the explicit NULL-key check is needed because the dedup right side has no NULLs.</li>
     *   <li>{@code rightHadNulls = true}:
     *       {@code CASE WHEN sentinel IS NOT NULL THEN TRUE END} — no else clause, so non-matches
     *       (including NULL-keyed rows) produce the implicit NULL.</li>
     * </ul>
     */
    private Expression buildMarkCase(Source source, Attribute keyAttr, Attribute sentinelAttr, boolean rightHadNulls) {
        Expression matched = new IsNotNull(source, sentinelAttr);
        if (rightHadNulls) {
            return new Case(source, matched, List.of(Literal.TRUE));
        }
        Expression keyIsNull = new IsNull(source, keyAttr);
        Expression nullLit = new Literal(source, null, DataType.BOOLEAN);
        return new Case(source, keyIsNull, List.of(nullLit, matched, Literal.TRUE, Literal.FALSE));
    }

    /**
     * Build an {@link Alias} that materializes the mark attribute, inheriting the mark's name and
     * {@link org.elasticsearch.xpack.esql.core.expression.NameId} so existing references resolve to
     * the value produced here.
     */
    private Alias markAlias(Source source, Expression value) {
        return new Alias(source, markAttribute.name(), value, markAttribute.id(), true);
    }

    // The mark attribute is part of this node's identity (it carries the NameId that the
    // surrounding condition references), so two MarkJoins with identical config and children but
    // different marks must not compare equal.
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), markAttribute);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(markAttribute, ((MarkJoin) obj).markAttribute);
    }
}
