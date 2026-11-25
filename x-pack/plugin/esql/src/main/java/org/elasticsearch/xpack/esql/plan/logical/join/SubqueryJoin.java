/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.VerificationException;
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
 * reason about the main query stop at its right side (see {@link IndependentSubqueryJoin}). The coordinator will replace the
 * subquery with its result before the plan is mapped, so this node is never serialized. Until that is implemented,
 * {@link #rejectUntilSupported} rejects it after analysis.
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

    /**
     * Rejects a plan that holds a {@code JOIN} on a subquery, which nothing can run yet. It runs right after analysis: later,
     * optimization may have dropped the join, and other subqueries may have run already. The {@code Mapper} runs it too, for plans
     * that do not come through the session.
     */
    public static void rejectUntilSupported(LogicalPlan plan) {
        plan.forEachDown(SubqueryJoin.class, join -> {
            throw new VerificationException(List.of(fail(join, "JOIN on a subquery is not supported yet")));
        });
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
     * Checks the subquery's columns after optimization, once the columns that nothing reads have been pruned from it: the hash join
     * copies a matched row's columns with {@code Block#lookup}, which some block types do not implement.
     */
    @Override
    public void postOptimizationVerification(Failures failures) {
        for (Attribute rightField : rightOutputFields()) {
            if (supportsPayloadType(rightField.dataType()) == false) {
                failures.add(
                    fail(
                        rightField,
                        "JOIN subquery cannot return column [{}] of type [{}]; remove it with KEEP or DROP",
                        rightField.name(),
                        rightField.dataType()
                    )
                );
            }
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
     * Whether the join can copy a subquery column of this type into its output.
     */
    static boolean supportsPayloadType(DataType dataType) {
        return switch (dataType) {
            // Block#lookup is not implemented for these block types.
            case AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, DATE_RANGE, DOUBLE_RANGE, DOC_DATA_TYPE, PARTIAL_AGG -> false;
            case UNSUPPORTED, NULL, BOOLEAN, COUNTER_LONG, COUNTER_INTEGER, COUNTER_DOUBLE, LONG, INTEGER, UNSIGNED_LONG, DOUBLE, SHORT,
                BYTE, FLOAT, HALF_FLOAT, SCALED_FLOAT, KEYWORD, TEXT, DATETIME, DATE_NANOS, IP, VERSION, OBJECT, SOURCE, DATE_PERIOD,
                TIME_DURATION, GEO_POINT, CARTESIAN_POINT, CARTESIAN_SHAPE, GEO_SHAPE, GEOHASH, GEOTILE, GEOHEX, TSID_DATA_TYPE,
                TIME_SERIES_METADATA_DATA_TYPE, HISTOGRAM, DENSE_VECTOR, FLATTENED -> true;
        };
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
