/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;

/**
 * The execution strategy for the {@code JOIN} command (see {@link Join#isCandicateColocatedJoin()}) when both indices are partitioned
 * the same way on the join key: same routing function, documents routed by the key, and shard {@code i} of both indices
 * hosted on the same node. Under that layout a key can only match within the same shard number, so the join executes on
 * the data nodes, each node joining its local left shards against the matching local right shards. Nothing is exchanged
 * between data nodes.
 * <p>
 * The parser never builds this node. {@code SelectJoinStrategy} replaces the plain {@link Join} with it on the
 * coordinator, after logical optimization, once it has checked the index metadata and the shard placement. It is
 * serialized because the whole join ships to the data nodes inside a {@code FragmentExec}.
 */
public class ColocatedJoin extends Join {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "ColocatedJoin",
        ColocatedJoin::new
    );

    /**
     * Picks the co-located strategy for a {@code JOIN} command.
     */
    public ColocatedJoin(Join join) {
        this(join.source(), join.left(), join.right(), join.config());
        assert join.isCandicateColocatedJoin() : "not a JOIN command: " + join;
    }

    public ColocatedJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Expression joinOnConditions
    ) {
        this(source, left, right, new JoinConfig(type, leftFields, rightFields, joinOnConditions));
    }

    public ColocatedJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config, ExecuteLocation.REMOTE);
    }

    private ColocatedJoin(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(LogicalPlan.class),
            new JoinConfig(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(left());
        out.writeNamedWriteable(right());
        config().writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Join> info() {
        // Same as Join: list the config's parts so expressions and references are registered.
        return NodeInfo.create(
            this,
            ColocatedJoin::new,
            left(),
            right(),
            config().type(),
            config().leftFields(),
            config().rightFields(),
            config().joinOnConditions()
        );
    }

    @Override
    public Join withConfig(JoinConfig config) {
        return new ColocatedJoin(source(), left(), right(), config);
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new ColocatedJoin(source(), left, right, config());
    }

    @Override
    public boolean isCandicateColocatedJoin() {
        return true;
    }

    /**
     * Walks down unary nodes (filters, projections, evals) to the relation feeding one side of the join, or
     * {@code null} if the side is not a plain index scan.
     */
    public static EsRelation findRelation(LogicalPlan plan) {
        while (plan instanceof UnaryPlan unary) {
            plan = unary.child();
        }
        return plan instanceof EsRelation relation ? relation : null;
    }
}
