/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.premapper;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.ColocatedJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Chooses how each {@code JOIN} command executes (see {@link Join#isCandicateColocatedJoin()}). The parser and the optimizer only know
 * that it is an inner join of two indices; whether the right index can be looked up shard by shard depends on index metadata,
 * which only the coordinator's cluster state tells. When all of the following hold, the join becomes a {@link ColocatedJoin}:
 * <ul>
 *     <li>each side is read through streaming commands only ({@code WHERE}, {@code EVAL}, {@code KEEP} and the like on the
 *     left, {@code WHERE} on the right): the join runs inside the data-node fragment, so a {@code LIMIT} or {@code STATS}
 *     below it would run per node;</li>
 *     <li>the left side reads local indices, any number of them, and the right side a single local index;</li>
 *     <li>the right index routes documents by {@code _routing} alone: no routing partitions, no {@code routing_path}, no
 *     resharding, and {@code _routing} required;</li>
 *     <li>the right join key is the right index's {@code index.routing_field}, so the server guarantees that all documents
 *     with a key value are on the shard that value hashes to.</li>
 * </ul>
 * Nothing is required of the left index's routing, shard count or placement: on the data nodes each left row is looked up
 * on the right shard that holds its key, locally when that shard has a copy on the node and remotely otherwise. A left index
 * routed by its key the same way as the right index only makes those lookups cheaper (see {@code ColocatedRouting}).
 * <p>
 * There is no other way to execute the command yet, so any failed condition fails the query, with every reason listed.
 */
final class SelectJoinStrategy {

    private SelectJoinStrategy() {}

    static LogicalPlan apply(LogicalPlan plan, ProjectState project) {
        Failures failures = new Failures();
        LogicalPlan result = plan.transformUp(
            Join.class,
            join -> join.isCandicateColocatedJoin() && join instanceof ColocatedJoin == false ? select(join, project, failures) : join
        );
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
        return result;
    }

    private static Join select(Join join, ProjectState project, Failures allFailures) {
        // Failures dedupes by plan node, so the reasons are collected here and reported as one failure of the join.
        List<Failure> failures = new ArrayList<>();
        EsRelation left = scan(join, join.left(), "left", p -> p instanceof Filter || p instanceof Eval || p instanceof Project, failures);
        EsRelation right = scan(join, join.right(), "right", p -> p instanceof Filter || p instanceof Project, failures);
        if (left != null) {
            checkLocalOnly(join, left, failures);
        }
        IndexMetadata rightIndex = right == null ? null : singleLocalIndex(join, right, project, failures);
        if (rightIndex != null) {
            checkRouting(join, rightIndex, failures);
            checkRoutingField(join, join.config().rightFields().get(0), rightIndex, failures);
        }
        if (failures.isEmpty() == false) {
            allFailures.add(fail(join, "{}", failures.stream().map(Failure::message).collect(Collectors.joining("; "))));
            return join;
        }
        return new ColocatedJoin(join);
    }

    /**
     * The relation feeding one side, if every command between it and the join is allowed there.
     */
    private static EsRelation scan(Join join, LogicalPlan side, String which, Predicate<LogicalPlan> allowed, List<Failure> failures) {
        LogicalPlan plan = side;
        while (plan instanceof UnaryPlan unary && allowed.test(unary)) {
            plan = unary.child();
        }
        if (plan instanceof EsRelation relation) {
            return relation;
        }
        failures.add(
            fail(join, "JOIN can't be executed after [{}] on the {} side, only after a scan of an index", plan.source().text(), which)
        );
        return null;
    }

    /**
     * The left side runs where its shards are, and looks up the right index from there; shards of a remote cluster can't.
     */
    private static void checkLocalOnly(Join join, EsRelation relation, List<Failure> failures) {
        if (relation.concreteIndices().keySet().equals(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) == false) {
            failures.add(
                fail(
                    join,
                    "JOIN requires [{}] to resolve to local indices only, found {}",
                    relation.indexPattern(),
                    relation.concreteIndices()
                )
            );
        }
    }

    private static IndexMetadata singleLocalIndex(Join join, EsRelation relation, ProjectState project, List<Failure> failures) {
        List<String> local = relation.concreteIndices().get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        if (relation.concreteIndices().size() != 1 || local == null || local.size() != 1) {
            failures.add(
                fail(
                    join,
                    "JOIN requires [{}] to resolve to a single local index, found {}",
                    relation.indexPattern(),
                    relation.concreteIndices()
                )
            );
            return null;
        }
        IndexMetadata index = project.metadata().index(local.get(0));
        if (index == null) {
            failures.add(fail(join, "JOIN could not find index [{}]", local.get(0)));
        }
        return index;
    }

    /**
     * Rejects routing schemes that do not send all documents with the same {@code _routing} to one shard by its hash alone.
     */
    private static void checkRouting(Join join, IndexMetadata index, List<Failure> failures) {
        String name = index.getIndex().getName();
        if (index.isRoutingPartitionedIndex()) {
            failures.add(fail(join, "JOIN does not support [{}]: it uses [index.routing_partition_size]", name));
        }
        if (index.getRoutingPaths().isEmpty() == false || index.getTimeSeriesDimensions().isEmpty() == false) {
            failures.add(fail(join, "JOIN does not support [{}]: it routes on document fields, not on [_routing]", name));
        }
        if (index.getReshardingMetadata() != null) {
            failures.add(fail(join, "JOIN does not support [{}]: it is being resharded", name));
        }
        if (index.mapping() == null || index.mapping().routingRequired() == false) {
            failures.add(fail(join, "JOIN requires [{}] to have [_routing] required in its mapping", name));
        }
    }

    /**
     * The join key must be the field the right index routes by. Only then does the server guarantee where each key value
     * lives, which is what lets each left row be looked up on a single right shard.
     */
    private static void checkRoutingField(Join join, Attribute key, IndexMetadata index, List<Failure> failures) {
        String name = index.getIndex().getName();
        String routingField = IndexSettings.ROUTING_FIELD.get(index.getSettings());
        String keyField = key instanceof FieldAttribute field ? field.fieldName().string() : null;
        if (routingField.isEmpty()) {
            failures.add(
                fail(join, "JOIN requires [{}] to set [{}] to the join key [{}]", name, IndexSettings.ROUTING_FIELD.getKey(), key.name())
            );
        } else if (routingField.equals(keyField) == false) {
            failures.add(
                fail(
                    join,
                    "JOIN requires the join key [{}] to be the routing field of [{}], found [{}: {}]",
                    key.name(),
                    name,
                    IndexSettings.ROUTING_FIELD.getKey(),
                    routingField
                )
            );
        }
    }
}
