/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.commonType;

/**
 * Replaces each {@link SubqueryJoin} of an optimized plan with a {@link HashJoin} whose build side is the subquery. The session runs
 * the subquery as a sub-plan and swaps in its result, as it does for PromQL's joins (see {@link HashJoin#firstSubPlan}), and the
 * join runs on the coordinator.
 * <p>
 * The join stays a {@link SubqueryJoin} through optimization and verification, which have rules and checks of their own for it
 * (see {@link IndependentSubqueryJoin}), and is replaced right after them, so that no optimizer rule sees the {@link HashJoin}.
 */
public final class SubqueryJoinLowering {

    private SubqueryJoinLowering() {}

    /**
     * Replaces every {@link SubqueryJoin} in {@code plan}, which must be optimized and verified. The plan's output doesn't change.
     */
    public static LogicalPlan lower(LogicalPlan plan) {
        List<Project> lowered = new ArrayList<>();
        LogicalPlan result = plan.transformUp(SubqueryJoin.class, join -> {
            Project project = toHashJoin(join);
            lowered.add(project);
            return project;
        });
        // A join comes after the joins nested in its sides, so walking backwards narrows a join before the ones below it, which
        // then see what it no longer reads. Narrowing a join rebuilds only the plans above it, so the joins below keep their
        // identity.
        for (int i = lowered.size() - 1; i >= 0; i--) {
            result = pruneUnusedColumns(result, lowered.get(i));
        }
        return result;
    }

    /**
     * Joins the left side with the subquery, keeping the column order of {@code join}. Numeric keys of different types are cast to
     * the same type on both sides, since the hash table only matches keys stored the same way.
     */
    private static Project toHashJoin(SubqueryJoin join) {
        Source source = join.source();
        List<Attribute> leftFields = join.config().leftFields();
        List<Attribute> rightFields = join.config().rightFields();
        List<Alias> leftCasts = new ArrayList<>();
        List<Alias> rightCasts = new ArrayList<>();
        List<Attribute> leftKeys = new ArrayList<>(leftFields.size());
        List<Attribute> rightKeys = new ArrayList<>(rightFields.size());
        for (int i = 0; i < leftFields.size(); i++) {
            Attribute left = leftFields.get(i);
            Attribute right = rightFields.get(i);
            DataType hashType = hashType(left.dataType(), right.dataType());
            leftKeys.add(castKey(source, left, hashType, leftCasts));
            rightKeys.add(castKey(source, right, hashType, rightCasts));
        }

        LogicalPlan left = leftCasts.isEmpty() ? join.left() : new Eval(source, join.left(), leftCasts);
        LogicalPlan subquery = join.right();
        if (rightCasts.isEmpty() == false) {
            // Drop the subquery's own keys: the join's output doesn't have them, and they would shadow the left columns of the
            // same name.
            List<NamedExpression> projections = new ArrayList<>();
            for (Attribute output : subquery.output()) {
                if (rightFields.contains(output) == false) {
                    projections.add(output);
                }
            }
            projections.addAll(rightKeys);
            subquery = new Project(source, new Eval(source, subquery, rightCasts), projections);
        }

        HashJoin hashJoin = new HashJoin(
            source,
            left,
            subquery,
            leftKeys,
            rightKeys,
            join.rightOutputFields(),
            false,
            join.config().type(),
            true
        );
        return new Project(source, hashJoin, join.output());
    }

    /**
     * The type both keys are hashed as, or {@code null} if their values are already stored the same way. The verifier only accepts
     * numeric keys whose types share a common type, and other keys that hash the same, such as {@code keyword} and {@code text}.
     */
    private static DataType hashType(DataType left, DataType right) {
        if (left.isNumeric() == false || right.isNumeric() == false) {
            return null;
        }
        DataType widenedLeft = left.widenSmallNumeric();
        DataType widenedRight = right.widenSmallNumeric();
        if (widenedLeft == widenedRight) {
            return null;
        }
        DataType common = commonType(widenedLeft, widenedRight);
        return switch (common) {
            case INTEGER, LONG, DOUBLE -> common;
            case null, default -> throw new IllegalStateException(
                "JOIN subquery keys of types [" + left + "] and [" + right + "] have no common type to hash"
            );
        };
    }

    /**
     * Returns {@code key}, or an attribute holding it cast to {@code hashType}, whose definition is added to {@code casts}.
     */
    private static Attribute castKey(Source source, Attribute key, DataType hashType, List<Alias> casts) {
        if (hashType == null || key.dataType().widenSmallNumeric() == hashType) {
            return key;
        }
        Expression cast = switch (hashType) {
            case INTEGER -> new ToInteger(source, key);
            case LONG -> new ToLong(source, key);
            case DOUBLE -> new ToDouble(source, key);
            default -> throw new IllegalStateException("can't cast JOIN subquery key [" + key.name() + "] to [" + hashType + "]");
        };
        Alias alias = new Alias(source, Attribute.rawTemporaryName(key.name(), "join_key"), cast, null, true);
        casts.add(alias);
        return alias.toAttribute();
    }

    /**
     * Narrows {@code project}, a join that {@link #toHashJoin} put in {@code plan}, to the columns the rest of the plan reads, and the
     * left side of its hash join to those columns and the keys. The join is replaced after the optimizer pruned the plan, so nothing
     * else narrows it. Its left side runs on the data nodes and the join on the coordinator, and the physical planner sends the
     * coordinator every column that a plan there reads, which for the join's projection and the hash join's own output is every
     * left column.
     */
    private static LogicalPlan pruneUnusedColumns(LogicalPlan plan, Project project) {
        Set<LogicalPlan> joinNodes = Collections.newSetFromMap(new IdentityHashMap<>());
        project.forEachDown(joinNodes::add);
        AttributeSet.Builder used = plan.outputSet().asBuilder();
        plan.forEachDown(node -> {
            if (joinNodes.contains(node)) {
                return;
            }
            used.addAll(node.references());
            if (node instanceof MergePlan) {
                // A merge lines up its branches' columns by position, without referencing them.
                for (LogicalPlan child : node.children()) {
                    used.addAll(child.output());
                }
            }
        });

        List<NamedExpression> kept = new ArrayList<>();
        for (NamedExpression projection : project.projections()) {
            if (used.contains(projection.toAttribute())) {
                kept.add(projection);
            }
        }
        HashJoin hashJoin = (HashJoin) project.child();
        if (kept.isEmpty()) {
            // A page of no columns can't count its rows. Keep one column, a key if it is in the output, since the join reads it anyway.
            NamedExpression column = project.projections().getFirst();
            for (NamedExpression projection : project.projections()) {
                if (hashJoin.leftFields().contains(projection.toAttribute())) {
                    column = projection;
                    break;
                }
            }
            kept.add(column);
        }

        AttributeSet.Builder leftNeeded = AttributeSet.builder();
        for (NamedExpression projection : kept) {
            leftNeeded.addAll(projection.references());
        }
        leftNeeded.addAll(hashJoin.leftFields());
        LogicalPlan left = hashJoin.left();
        List<Attribute> leftOutput = new ArrayList<>();
        for (Attribute attribute : left.output()) {
            if (leftNeeded.contains(attribute)) {
                leftOutput.add(attribute);
            }
        }

        if (kept.size() == project.projections().size() && leftOutput.size() == left.output().size()) {
            return plan;
        }
        LogicalPlan narrowedJoin = leftOutput.size() == left.output().size()
            ? hashJoin
            : hashJoin.replaceChildren(new Project(left.source(), left, leftOutput), hashJoin.right());
        Project narrowed = new Project(project.source(), narrowedJoin, kept);
        return plan.transformUp(LogicalPlan.class, node -> node == project ? narrowed : node);
    }
}
