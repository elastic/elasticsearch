/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SurrogateLogicalPlan;

import java.util.List;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;

/**
 * Lookup join - specialized LEFT (OUTER) JOIN between the main left side and a lookup index (index_mode = lookup) on the right.
 * This is only used during parsing and substituted to a regular {@link Join} during analysis.
 */
public class LookupJoin extends Join implements SurrogateLogicalPlan, TelemetryAware, PostAnalysisVerificationAware {

    public LookupJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        List<Attribute> joinFields,
        @Nullable Expression joinOnConditions,
        ExecuteLocation mode
    ) {
        this(source, left, right, new JoinConfig(LEFT, joinFields, joinFields, joinOnConditions), mode);
    }

    public LookupJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Expression joinOnConditions,
        ExecuteLocation mode
    ) {
        this(source, left, right, new JoinConfig(type, leftFields, rightFields, joinOnConditions), mode);
    }

    public LookupJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig joinConfig) {
        this(source, left, right, joinConfig, ExecuteLocation.ANY);
    }

    public LookupJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig joinConfig, ExecuteLocation mode) {
        super(source, left, right, joinConfig, mode);
    }

    /**
     * Translate the expression into a regular join with a Projection on top, to deal with serialization &amp; co.
     */
    @Override
    public LogicalPlan surrogate() {
        // TODO: decide whether to introduce USING or just basic ON semantics - keep the ordering out for now
        return new Join(source(), left(), right(), config(), executesOn());
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new LookupJoin(source(), left, right, config(), executesOn());
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(
            this,
            LookupJoin::new,
            left(),
            right(),
            config().type(),
            config().leftFields(),
            config().rightFields(),
            config().joinOnConditions(),
            executesOn()
        );
    }

    @Override
    public String telemetryLabel() {
        var label = new StringBuilder();
        if (executesOn() == ExecuteLocation.COORDINATOR) {
            label.append("COORDINATOR ");
        }
        label.append("LOOKUP JOIN");
        if (config().joinOnConditions() != null) {
            label.append(" ON EXPRESSION");
        }
        return label.toString();
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        super.postAnalysisVerification(failures);
        checkNotUnderNested(failures);
        if (executesOn() == ExecuteLocation.REMOTE) {
            checkRemoteJoin(failures);
        }
    }

    /**
     * The lookup index is searched for the join keys, but under a nested object the parent's index holds none of their values.
     */
    private void checkNotUnderNested(Failures failures) {
        AttributeSet.Builder keys = AttributeSet.builder().addAll(config().rightFields());
        if (config().joinOnConditions() != null) {
            AttributeSet rightOutput = right().outputSet();
            config().joinOnConditions().references().forEach(a -> {
                if (rightOutput.contains(a)) {
                    keys.add(a);
                }
            });
        }
        for (Attribute key : keys.build()) {
            if (key instanceof FieldAttribute fa && fa.field().isUnderNested()) {
                failures.add(
                    fail(key, "LOOKUP JOIN cannot match on [{}]: matching on nested child fields is not supported yet", key.name())
                );
            }
        }
    }

    private void checkRemoteJoin(Failures failures) {
        // Check only for LIMITs, Join will check the rest post-optimization
        checkForLimits(this, failures);
    }

    private void checkForLimits(LogicalPlan plan, Failures failures) {
        if (plan instanceof AbstractSubqueryJoin subqueryJoin) {
            // The right side is an independent subquery; do not traverse into it.
            checkForLimits(subqueryJoin.left(), failures);
        } else {
            if (plan instanceof Limit f) {
                failures.add(
                    fail(this, "LOOKUP JOIN with remote indices can't be executed after [" + f.source().text() + "]" + f.source().source())
                );
            }
            for (LogicalPlan child : plan.children()) {
                checkForLimits(child, failures);
            }
        }
    }
}
