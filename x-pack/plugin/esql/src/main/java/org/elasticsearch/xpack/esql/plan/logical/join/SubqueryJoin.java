/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.areTypesCompatible;

/**
 * A {@code JOIN} whose right side is a subquery written by the user, as in {@code FROM a | LEFT OUTER JOIN (FROM b | ...) ON key}.
 * <p>
 * The subquery is an independent query: it runs before the main query and only its result reaches the join, so plan walks that
 * reason about the main query stop at its right side (see {@link IndependentSubqueryJoin}). The optimizer's last batch before Plan
 * Fetch replaces this node with a {@link HashJoin} that the coordinator runs, so it is never serialized.
 * <p>
 * The subquery's columns are {@link Join#makeReference references} in the join's output: they come from the subquery's result,
 * not from an index the main query reads, so rules that look fields up in the main query's indices must not see them as fields.
 */
public class SubqueryJoin extends Join implements IndependentSubqueryJoin {

    public SubqueryJoin(Source source, LogicalPlan left, LogicalPlan subquery, JoinConfig config) {
        super(source, left, subquery, config, ExecuteLocation.ANY);
    }

    public SubqueryJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan subquery,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Expression joinOnConditions
    ) {
        this(source, left, subquery, new JoinConfig(type, leftFields, rightFields, joinOnConditions));
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<Join> info() {
        // Split the config so that its expressions are visible to expression transformations.
        JoinConfig config = config();
        return NodeInfo.create(
            this,
            SubqueryJoin::new,
            left(),
            right(),
            config.type(),
            config.leftFields(),
            config.rightFields(),
            config.joinOnConditions()
        );
    }

    @Override
    public SubqueryJoin withConfig(JoinConfig config) {
        return new SubqueryJoin(source(), left(), right(), config);
    }

    @Override
    public SubqueryJoin replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new SubqueryJoin(source(), left, right, config());
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        List<NamedExpression> subqueryColumns = new ArrayList<>(right.size());
        for (NamedExpression column : right) {
            // Only attributes become references. An Alias, such as the one PruneLeftJoinOnNullMatchingField adds so that columns of
            // the same type share one null, defines its column and must stay.
            subqueryColumns.add(column instanceof Attribute attribute ? makeReference(List.of(attribute)).getFirst() : column);
        }
        return Join.computeOutputExpressions(left, subqueryColumns, config());
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        for (int i = 0; i < config().leftFields().size(); i++) {
            Attribute leftField = config().leftFields().get(i);
            Attribute rightField = config().rightFields().get(i);
            if (comparableKeyTypes(leftField.dataType(), rightField.dataType()) == false) {
                failures.add(
                    fail(
                        leftField,
                        "JOIN subquery left field [{}] of type [{}] is incompatible with right field [{}] of type [{}]",
                        leftField.name(),
                        leftField.dataType(),
                        rightField.name(),
                        rightField.dataType()
                    )
                );
            }
            if (AbstractInSubqueryJoin.isSubqueryJoinUnsupported(rightField.dataType())) {
                failures.add(
                    fail(
                        leftField,
                        "JOIN subquery with right field [{}] of type [{}] is not supported",
                        rightField.name(),
                        rightField.dataType()
                    )
                );
            }
        }

        Set<String> remoteIndexPatterns = new TreeSet<>();
        collectRemoteIndexPatterns(left(), remoteIndexPatterns);
        collectRemoteIndexPatterns(right(), remoteIndexPatterns);
        if (remoteIndexPatterns.isEmpty() == false) {
            failures.add(fail(this, "JOIN subquery does not support remote indices yet, found {}", remoteIndexPatterns));
        }
    }

    /**
     * Same as {@code LOOKUP JOIN}: numeric keys of different types match by value, with one exception. An {@code unsigned_long}
     * only matches another {@code unsigned_long}: its common type with a signed integer cannot hold negative values, and a common
     * floating-point type would round it. For now, though, {@link AbstractInSubqueryJoin#isSubqueryJoinUnsupported} rejects every
     * {@code unsigned_long} key.
     */
    static boolean comparableKeyTypes(DataType leftType, DataType rightType) {
        if (leftType == NULL) {
            // A null literal matches nothing, as in LOOKUP JOIN.
            return true;
        }
        if (leftType.isNumeric() && rightType.isNumeric()) {
            return areTypesCompatible(leftType, rightType);
        }
        return leftType.noText() == rightType.noText();
    }

    /**
     * Collects the remote index patterns read by {@code plan}'s own query. A nested {@code SubqueryJoin} reports both of its own
     * sides, so that a remote index is reported once. The right side of any other {@link IndependentSubqueryJoin} runs on its own and
     * is not this join's to check.
     */
    private static void collectRemoteIndexPatterns(LogicalPlan plan, Set<String> remoteIndexPatterns) {
        if (plan instanceof SubqueryJoin) {
            return;
        }
        if (plan instanceof EsRelation relation
            && relation.concreteIndices().keySet().stream().anyMatch(cluster -> cluster.equals(LOCAL_CLUSTER_GROUP_KEY) == false)) {
            remoteIndexPatterns.add(relation.indexPattern());
        }
        for (LogicalPlan child : IndependentSubqueryJoin.mainScopeChildren(plan)) {
            collectRemoteIndexPatterns(child, remoteIndexPatterns);
        }
    }
}
