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
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.SurrogateLogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.UsingJoinType;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;

/**
 * Lookup join - specialized LEFT (OUTER) JOIN between the main left side and a lookup index (index_mode = lookup) on the right.
 */
public class LookupJoin extends Join implements SurrogateLogicalPlan, PostAnalysisVerificationAware, TelemetryAware {

    private boolean isRemote = false;

    public LookupJoin(Source source, LogicalPlan left, LogicalPlan right, List<Attribute> joinFields) {
        this(source, left, right, new UsingJoinType(LEFT, joinFields), emptyList(), emptyList(), emptyList());
    }

    public LookupJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> joinFields,
        List<Attribute> leftFields,
        List<Attribute> rightFields
    ) {
        this(source, left, right, new JoinConfig(type, joinFields, leftFields, rightFields));
    }

    public LookupJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig joinConfig) {
        super(source, left, right, joinConfig);
    }

    /**
     * Translate the expression into a regular join with a Projection on top, to deal with serialization &amp; co.
     */
    @Override
    public LogicalPlan surrogate() {
        // TODO: decide whether to introduce USING or just basic ON semantics - keep the ordering out for now
        return new Join(source(), left(), right(), config());
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new LookupJoin(source(), left, right, config());
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
            config().rightFields()
        );
    }

    @Override
    public String telemetryLabel() {
        return "LOOKUP JOIN";
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        super.postAnalysisVerification(failures);
        if (isRemote) {
            checkRemoteJoin(failures);
        }
    }

    private void checkRemoteJoin(Failures failures) {
        boolean[] agg = { false };
        boolean[] enrichCoord = { false };
        boolean[] sort = { false };

        this.forEachUp(UnaryPlan.class, u -> {
            if (u instanceof Aggregate) {
                agg[0] = true;
            } else if (u instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR) {
                enrichCoord[0] = true;
            } else if (u instanceof OrderBy) {
                sort[0] = true;
            }
        });
        if (agg[0]) {
            failures.add(fail(this, "LOOKUP JOIN with remote indices can't be executed after STATS"));
        }
        if (enrichCoord[0]) {
            failures.add(fail(this, "LOOKUP JOIN with remote indices can't be executed after ENRICH with coordinator policy"));
        }
        if (sort[0]) {
            failures.add(fail(this, "LOOKUP JOIN with remote indices can't be executed after SORT"));
        }
    }

    public boolean isRemote() {
        return isRemote;
    }

    public LookupJoin setRemote(boolean remote) {
        isRemote = remote;
        return this;
    }
}
