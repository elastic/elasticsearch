/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.SurrogateLogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.UsingJoinType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;

/**
 * Lookup join - specialized LEFT (OUTER) JOIN between the main left side and a lookup index (index_mode = lookup) on the right.
 */
public class LookupJoin extends Join implements SurrogateLogicalPlan, PostAnalysisVerificationAware, TelemetryAware {

    public LookupJoin(Source source, LogicalPlan left, LogicalPlan right, List<Attribute> joinFields, boolean isRemote) {
        this(source, left, right, new UsingJoinType(LEFT, joinFields), emptyList(), emptyList(), emptyList(), isRemote);
    }

    public LookupJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> joinFields,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        boolean isRemote
    ) {
        this(source, left, right, new JoinConfig(type, joinFields, leftFields, rightFields), isRemote);
    }

    public LookupJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig joinConfig, boolean isRemote) {
        super(source, left, right, joinConfig, isRemote);
    }

    /**
     * Translate the expression into a regular join with a Projection on top, to deal with serialization &amp; co.
     */
    @Override
    public LogicalPlan surrogate() {
        // TODO: decide whether to introduce USING or just basic ON semantics - keep the ordering out for now
        return new Join(source(), left(), right(), config(), isRemote());
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new LookupJoin(source(), left, right, config(), isRemote());
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
            isRemote()
        );
    }

    @Override
    public String telemetryLabel() {
        return "LOOKUP JOIN";
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        super.postAnalysisVerification(failures);
        if (isRemote()) {
            checkRemoteJoin(failures);
        }
    }

    private void checkRemoteJoin(Failures failures) {
        Set<String> fails = new HashSet<>();

        this.forEachUp(UnaryPlan.class, u -> {
            if (u instanceof PipelineBreaker) {
                fails.add(u.nodeName());
            }
            if (u instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR) {
                fails.add("ENRICH with coordinator policy");
            }
        });

        fails.forEach(f -> failures.add(fail(this, "LOOKUP JOIN with remote indices can't be executed after " + f)));
    }

}
