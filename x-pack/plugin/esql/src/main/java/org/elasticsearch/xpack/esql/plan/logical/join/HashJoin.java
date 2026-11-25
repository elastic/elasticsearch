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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.function.Predicate.not;
import static java.util.stream.Stream.concat;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Coordinator hash join over a local build side (maps to {@code HashJoinExec}).
 * <p>
 * The {@link Kind} says which command the join runs, and with it the join type and how the build side's keys match.
 * {@link JoinTypes#INNER} drops unmatched probe rows; {@link JoinTypes#LEFT} keeps them.
 */
public class HashJoin extends Join implements SortPreserving {

    /**
     * The command a hash join runs.
     */
    public enum Kind {
        /**
         * A PromQL one-to-one vector match: a probe row matches at most one build row, and a repeated build key fails.
         */
        PROMQL_ONE_TO_ONE(JoinTypes.INNER),
        /**
         * A PromQL many-to-one or one-to-many vector match ({@code group_left}, {@code group_right}): the build keys are unique and
         * the probe keys may repeat. A repeated build key fails.
         */
        PROMQL_MANY_TO_ONE(JoinTypes.INNER),
        /**
         * The ES|QL {@code LEFT OUTER JOIN} command. A probe row comes out once for each build row it matches. A null or multivalued
         * key matches nothing, as in {@code LOOKUP JOIN}, and a multivalued build key warns once, even if no probe key would have
         * matched it (see {@code RowInTableLookupOperator}).
         */
        ESQL_LEFT_OUTER_JOIN(JoinTypes.LEFT),
        /**
         * The ES|QL {@code INNER JOIN} command: as {@link #ESQL_LEFT_OUTER_JOIN}, but a probe row that matches nothing is dropped.
         */
        ESQL_INNER_JOIN(JoinTypes.INNER);

        private final JoinType joinType;

        Kind(JoinType joinType) {
            this.joinType = joinType;
        }
    }

    private final List<Attribute> addedFields;
    private final Kind kind;
    private volatile List<Attribute> lazyOutput;

    public HashJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields,
        Kind kind
    ) {
        super(source, left, right, kind.joinType, leftFields, rightFields, null, ExecuteLocation.COORDINATOR);
        this.addedFields = addedFields;
        this.kind = kind;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public List<Attribute> leftFields() {
        return config().leftFields();
    }

    public List<Attribute> rightFields() {
        return config().rightFields();
    }

    public List<Attribute> addedFields() {
        return addedFields;
    }

    public Kind kind() {
        return kind;
    }

    /**
     * Whether a probe row matches at most one build row, which fails on a repeated build key at runtime.
     */
    public boolean unique() {
        return switch (kind) {
            case PROMQL_ONE_TO_ONE -> true;
            case PROMQL_MANY_TO_ONE, ESQL_LEFT_OUTER_JOIN, ESQL_INNER_JOIN -> false;
        };
    }

    /**
     * Whether the build side may repeat a key. Without it, building the lookup table fails on a repeated key.
     */
    public boolean repeatedKeys() {
        return switch (kind) {
            case PROMQL_ONE_TO_ONE, PROMQL_MANY_TO_ONE -> false;
            case ESQL_LEFT_OUTER_JOIN, ESQL_INNER_JOIN -> true;
        };
    }

    /**
     * The ES|QL command this join runs, which names it in the errors of its checks, or {@code null} for a PromQL vector match,
     * whose command checks it.
     */
    private String esqlCommand() {
        return switch (kind) {
            case PROMQL_ONE_TO_ONE, PROMQL_MANY_TO_ONE -> null;
            case ESQL_LEFT_OUTER_JOIN -> "LEFT OUTER JOIN";
            case ESQL_INNER_JOIN -> "INNER JOIN";
        };
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(
                concat(right().output().stream().filter(not(rightFields()::contains)), leftFields().stream()).toList(),
                left().output().stream().filter(not(leftFields()::contains)).toList()
            );
        }
        return lazyOutput;
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        return new ArrayList<>(output());
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    /**
     * Finds the first (bottom-up) {@link HashJoin} whose right subquery has not yet been replaced with results.
     */
    public static LogicalPlanTuple firstSubPlan(LogicalPlan optimizedPlan, Set<LocalRelation> subPlansResults) {
        var subPlanHolder = new Holder<LogicalPlan>();
        optimizedPlan.forEachUp(HashJoin.class, join -> {
            if (subPlanHolder.get() == null) {
                if ((join.right() instanceof LocalRelation lr && subPlansResults.contains(lr)) == false) {
                    subPlanHolder.set(join.right());
                }
            }
        });

        var subPlan = subPlanHolder.get();
        if (subPlan == null) {
            return null;
        }
        subPlan.setOptimized();
        // same instance held on the join's right side, so it doubles as the identity key used to substitute the
        // materialized result back into the main plan hence both tuple slots are the same.
        return new LogicalPlanTuple(subPlan, subPlan);
    }

    /**
     * Rebuilds the main plan after the right-side subquery has been materialized, replacing the matching HashJoin's right child with
     * the materialized local relation.
     */
    public static LogicalPlan newMainPlan(LogicalPlan optimizedPlan, LogicalPlanTuple subPlans, LocalRelation resultWrapper) {
        LogicalPlan newPlan = optimizedPlan.transformUp(
            HashJoin.class,
            join -> join.right() == subPlans.originalSubPlan() ? join.replaceRight(resultWrapper) : join
        );
        newPlan.setOptimized();
        return newPlan;
    }

    @Override
    public HashJoin replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new HashJoin(source(), left, right, leftFields(), rightFields(), addedFields, kind);
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(this, HashJoin::new, left(), right(), leftFields(), rightFields(), addedFields, kind);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        HashJoin hashJoin = (HashJoin) o;
        return kind == hashJoin.kind && addedFields.equals(hashJoin.addedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), addedFields, kind);
    }

    /**
     * Checks the right side's columns of an ES|QL join: the hash join copies a matched row's columns with {@code Block#lookup},
     * which some block types do not implement. The optimizer has pruned the columns that nothing reads.
     */
    @Override
    public void postOptimizationVerification(Failures failures) {
        super.postOptimizationVerification(failures);
        String command = esqlCommand();
        if (command == null) {
            return;
        }
        for (Attribute field : addedFields) {
            if (supportsPayloadType(field.dataType()) == false) {
                failures.add(
                    fail(
                        field,
                        "{} cannot return column [{}] of type [{}]; remove it with KEEP or DROP",
                        command,
                        field.name(),
                        field.dataType()
                    )
                );
            }
        }
    }

    /**
     * Whether the join can copy a right-side column of this type into its output.
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
     * Tuple holding the subplan to execute and the original plan node used as the identity key when substituting the result back.
     */
    public record LogicalPlanTuple(LogicalPlan subPlan, LogicalPlan originalSubPlan) {}
}
