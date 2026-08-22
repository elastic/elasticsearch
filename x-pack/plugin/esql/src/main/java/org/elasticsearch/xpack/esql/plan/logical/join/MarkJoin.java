/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A mark join used to implement {@code field IN (subquery)} when the {@code IN} is embedded in an arbitrary boolean expression — typically
 * as a child of {@code OR} where {@link SemiJoin} / {@link AntiJoin}'s row-filtering shape is not applicable.
 * <p>
 * Unlike {@link SemiJoin}, this operator preserves every left row and adds a single boolean <em>mark</em> attribute holding the value of
 * {@code field IN (subquery)} for that row, with full three-valued logic:
 * <ul>
 *   <li>{@code TRUE} — left key matches at least one row in the subquery</li>
 *   <li>{@code FALSE} — no match, left key non-NULL, subquery had no NULLs</li>
 *   <li>{@code NULL} — left key is NULL, OR no match but the subquery contains NULL(s)</li>
 * </ul>
 * The mark is a normal boolean attribute that the rewritten WHERE condition references, so the surrounding
 * {@code OR}/{@code AND}/{@code NOT} operators are evaluated by the standard expression machinery.
 * <p>
 * Like {@link SemiJoin}, the right side is an independent subquery executed first; once its result arrives as a {@link LocalRelation},
 * {@link AbstractSubqueryJoin#inlineData} routes the dedup result into the {@link #buildFilterPathPlan} / {@link #buildHashJoinPathPlan}
 * hooks, which substitute an {@link Eval} that materializes the mark.
 */
public class MarkJoin extends AbstractSubqueryJoin {

    /**
     * Mark attribute exposed by this join. The surrounding {@link Filter} condition references it (placed there by
     * {@link org.elasticsearch.xpack.esql.analysis.InSubqueryResolver} as the substitute for the original {@code InSubquery}).
     * When {@link AbstractSubqueryJoin#inlineData} converts this node into an {@link Eval}, the {@link Alias} it produces shares this
     * attribute's {@link org.elasticsearch.xpack.esql.core.expression.NameId} so existing references resolve.
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
    // Each path produces an Eval that materializes the mark attribute (sharing the synthetic NameId allocated by the resolver),
    // preserving every left row.

    @Override
    protected LogicalPlan buildEmptyRightSidePlan(Source source) {
        // Subquery returned no rows → the mark is FALSE for every row (no NULL on the right to propagate, so even NULL keys yield FALSE).
        return new Eval(source, left(), List.of(markAlias(source, Literal.FALSE)));
    }

    @Override
    protected LogicalPlan buildShortCircuitPlan(Source source, boolean allRightNull) {
        // Reached only when every right value is NULL (MARK keeps shortCircuitOnAnyRightNull false). No match is possible and the right
        // contains a NULL, so the mark is NULL for every row.
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
        // The three-valued semantics of {@link In} produce exactly the mark we want: match → TRUE; no match, non-NULL key → FALSE;
        // NULL key → NULL; no match with a NULL literal → NULL. BlockHash collapses every NULL into its reserved group 0, so the
        // deduplicated keys already carry a NULL at index 0 when {@code rightHadNulls}; the IN list emits it naturally.
        Expression in = inListFromDedupKeys(source, leftField, dedupKeys, keyType);
        return new Eval(source, left(), List.of(markAlias(source, in)));
    }

    @Override
    protected LogicalPlan buildFilterPathPlanFromExpression(Expression condition, Source source) {
        return new Eval(source, left(), List.of(markAlias(source, condition)));
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
        // LEFT join → Eval($mark = CASE …) → Project(original left output + mark). {@link AbstractSubqueryJoin#inlineAsHashJoin} has
        // already wrapped the left side in an {@code Eval(svKey = MvSingleValueOrNull(leftField))} so multi-valued positions become
        // NULL before the join. {@code leftJoinConfig.leftFields()} therefore refers to the SV-guarded attribute, which we use in the
        // CASE so NULL/MV keys collapse to mark=NULL.
        Join leftJoin = new Join(source, leftSide, deduplicatedData, leftJoinConfig, ExecuteLocation.ANY);
        List<Attribute> svKeyAttrs = leftJoinConfig.leftFields();
        Expression caseExpr = buildMarkCase(source, svKeyAttrs, sentinelAttr, rightHadNulls);
        Eval eval = new Eval(source, leftJoin, List.of(markAlias(source, caseExpr)));
        // Keep the original left output and append the mark; drop the synthetic SV-guard attribute. Use {@code left().output()} rather
        // than {@code leftSide.output()} because the latter now includes the SV-guard attribute.
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
     * Build the CASE expression that derives the three-valued mark from the LEFT-join sentinel and the {@code rightHadNulls} flag,
     * following SQL {@code IN} semantics. {@code keyAttrs} are the SV-guarded left keys, so a NULL in any key covers both originally
     * NULL and originally multi-valued left keys (the SV guard collapsed MV positions to NULL). For multi-column keys the row is
     * considered null-keyed when <em>any</em> component is NULL.
     * <p>
     * The mark has three outcomes:
     * <ul>
     *   <li><b>TRUE</b> — the left key matched at least one right row (the sentinel is non-NULL).</li>
     *   <li><b>FALSE</b> — a non-NULL key with no match, and the right side had no NULLs.</li>
     *   <li><b>NULL</b> — the left key is NULL/MV, or there was no match but the right side contained a NULL
     *       ({@code x IN (.., NULL)} is never FALSE).</li>
     * </ul>
     * The two values of {@code rightHadNulls} need different CASE shapes because a non-match means FALSE only when the right side
     * is NULL-free:
     * <ul>
     *   <li>{@code rightHadNulls == false}: {@code CASE WHEN key IS NULL THEN NULL WHEN matched THEN TRUE ELSE FALSE END}.</li>
     *   <li>{@code rightHadNulls == true}: {@code CASE WHEN matched THEN TRUE END} — the implicit {@code ELSE NULL} covers every
     *       non-match, including NULL/MV keys.</li>
     * </ul>
     */
    private Expression buildMarkCase(Source source, List<Attribute> keyAttrs, Attribute sentinelAttr, boolean rightHadNulls) {
        Expression matched = new IsNotNull(source, sentinelAttr);

        if (rightHadNulls) {
            return new Case(source, matched, List.of(Literal.TRUE));
        }

        // Key is considered null if ANY component is null (multi-column: any column null → no match)
        Expression keyIsNull;
        if (keyAttrs.size() == 1) {
            keyIsNull = new IsNull(source, keyAttrs.get(0));
        } else {
            List<Expression> nullChecks = new ArrayList<>();
            for (Attribute keyAttr : keyAttrs) {
                nullChecks.add(new IsNull(source, keyAttr));
            }
            keyIsNull = Predicates.combineOr(nullChecks);
        }
        Expression nullMark = new Literal(source, null, DataType.BOOLEAN);
        return new Case(source, keyIsNull, List.of(nullMark, matched, Literal.TRUE, Literal.FALSE));
    }

    /**
     * Build an {@link Alias} that materializes the mark attribute, inheriting the mark's name and
     * {@link org.elasticsearch.xpack.esql.core.expression.NameId} so existing references resolve to the value produced here.
     */
    private Alias markAlias(Source source, Expression value) {
        return new Alias(source, markAttribute.name(), value, markAttribute.id(), true);
    }

    /**
     * Returns {@code true} when {@code plan} is the top node of a subtree synthesized by this join's
     * {@link AbstractSubqueryJoin#inlineData} to materialise the mark attribute. Two shapes are possible:
     * <ul>
     *   <li><b>Filter / empty / short-circuit path</b> ({@link #buildFilterPathPlan}, {@link #buildEmptyRightSidePlan},
     *       {@link #buildShortCircuitPlan}): a bare {@link Eval} whose every alias carries either the
     *       {@link InSubqueryResolver#MARK_ATTRIBUTE_NAME_PREFIX} or the
     *       {@link InSubqueryResolver#CONST_ATTRIBUTE_NAME_PREFIX} name prefix.</li>
     *   <li><b>Hash-join path</b> ({@link #buildHashJoinPathPlan}): a {@link Project} whose direct child is such a mark
     *       {@link Eval}.</li>
     * </ul>
     * Update this method whenever any of the four {@code build*} methods above changes its top-level node type.
     */
    static boolean isInlinedBoundary(LogicalPlan plan) {
        if (plan instanceof Eval eval) {
            return eval.fields().isEmpty() == false
                && eval.fields()
                    .stream()
                    .allMatch(
                        alias -> alias.name().startsWith(InSubqueryResolver.MARK_ATTRIBUTE_NAME_PREFIX)
                            || alias.name().startsWith(InSubqueryResolver.CONST_ATTRIBUTE_NAME_PREFIX)
                    );
        }
        if (plan instanceof Project project && project.child() instanceof Eval eval) {
            return isInlinedBoundary(eval);
        }
        return false;
    }

    // The mark attribute is part of this node's identity (it carries the NameId that the surrounding condition references), so two
    // MarkJoins with identical config and children but different marks must not compare equal.
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), markAttribute);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(markAttribute, ((MarkJoin) obj).markAttribute);
    }
}
