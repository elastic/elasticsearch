/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.SurrogateLogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.UsingJoinType;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;

/**
 * Lookup join - specialized LEFT (OUTER) JOIN between the main left side and a lookup index (index_mode = lookup) on the right.
 */
public class LookupJoin extends Join implements SurrogateLogicalPlan {

    private final List<Attribute> output;

    public LookupJoin(Source source, LogicalPlan left, LogicalPlan right, List<Attribute> joinFields) {
        this(source, left, right, new UsingJoinType(LEFT, joinFields), emptyList(), emptyList(), emptyList(), emptyList());
    }

    public LookupJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> joinFields,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> output
    ) {
        this(source, left, right, new JoinConfig(type, joinFields, leftFields, rightFields), output);
    }

    public LookupJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig joinConfig, List<Attribute> output) {
        super(source, left, right, joinConfig);
        this.output = output;
    }

    /**
     * Translate the expression into a regular join with a Projection on top, to deal with serialization &amp; co.
     */
    @Override
    public LogicalPlan surrogate() {
        JoinConfig cfg = config();
        JoinConfig newConfig = new JoinConfig(LEFT, cfg.matchFields(), cfg.leftFields(), cfg.rightFields());
        Join normalized = new Join(source(), left(), right(), newConfig);
        // TODO: decide whether to introduce USING or just basic ON semantics - keep the ordering out for now
        return new Project(source(), normalized, output);
    }

    public List<Attribute> output() {
        return output;
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new LookupJoin(source(), left, right, config(), output);
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(
            this,
            LookupJoin::new,
            left(),
            right(),
            config().type(),
            config().matchFields(),
            config().leftFields(),
            config().rightFields(),
            output
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), output);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }

        LookupJoin other = (LookupJoin) obj;
        return Objects.equals(output, other.output);
    }
}
