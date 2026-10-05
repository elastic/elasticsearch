/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plugin.ReductionPlan;

import java.util.List;
import java.util.Optional;

/**
 * Splits a data node plan whose node reduce stage the coordinator already planned.
 * <p>
 * Usually the data node derives the node reduce driver from the fragment it receives. When the coordinator plans the
 * stage itself, the data node plan carries an {@link ExchangeExec} with {@link ExchangeExec.Scope#NODE} between the
 * node reduce stage and the fragment. This split is purely structural, the same as the coordinator split at the
 * cluster exchange: the node reduce plan is everything above the exchange, ending in an {@link ExchangeSourceExec},
 * and the data plan is the fragment below it, feeding an {@link ExchangeSinkExec}. Nothing is planned here and nothing
 * is recomputed. The exchange output, declared once by the coordinator, is the schema contract between the two.
 */
public final class NodeReduceSplit {

    private NodeReduceSplit() {}

    /**
     * @return the two plans, or empty when the plan carries no planned node reduce stage
     */
    public static Optional<ReductionPlan> split(ExchangeSinkExec plan) {
        Holder<ExchangeExec> nodeExchange = new Holder<>();
        PhysicalPlan nodeReducePlan = plan.transformDownSkipBranch((node, skipBranch) -> {
            if (node instanceof ExchangeExec exchange && exchange.scope() == ExchangeExec.Scope.NODE) {
                if (nodeExchange.get() != null) {
                    throw new EsqlIllegalArgumentException("expected a single NODE exchange in a data node plan but found two");
                }
                nodeExchange.set(exchange);
                skipBranch.set(true);
                return new ExchangeSourceExec(exchange.source(), exchange.output(), exchange.inBetweenAggs());
            }
            return node;
        });
        ExchangeExec exchange = nodeExchange.get();
        if (exchange == null) {
            return Optional.empty();
        }
        if (exchange.child() instanceof FragmentExec == false) {
            throw new EsqlIllegalArgumentException("a NODE exchange must wrap a fragment, found [{}]", exchange.child().nodeName());
        }
        if (sameIdsAndTypes(exchange.output(), exchange.child().output()) == false) {
            throw new EsqlIllegalArgumentException(
                "NODE exchange output {} does not match its fragment output {}",
                exchange.output(),
                exchange.child().output()
            );
        }
        ExchangeSinkExec dataNodePlan = new ExchangeSinkExec(plan.source(), exchange.output(), false, exchange.child());
        return Optional.of(new ReductionPlan((ExchangeSinkExec) nodeReducePlan, dataNodePlan));
    }

    private static boolean sameIdsAndTypes(List<Attribute> left, List<Attribute> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (left.get(i).id().equals(right.get(i).id()) == false || left.get(i).dataType() != right.get(i).dataType()) {
                return false;
            }
        }
        return true;
    }
}
