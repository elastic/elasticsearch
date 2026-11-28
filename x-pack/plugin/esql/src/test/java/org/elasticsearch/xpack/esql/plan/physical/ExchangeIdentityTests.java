/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * A rewrite of an exchange node is the same hop, so it keeps its exchange id however it is built. The plan logs show the plan at
 * several stages, and an exchange that got a new id between two of them could not be followed from one to the next.
 */
public class ExchangeIdentityTests extends ESTestCase {

    public void testExchangeExecKeepsItsIdentityWhenItsChildIsReplaced() {
        ExchangeExec exchange = new ExchangeExec(Source.EMPTY, child());
        assertNotNull(exchange.exchange());

        // replaceChild is called directly by rules, which then do not go through the transforms that copy identity
        ExchangeExec replaced = (ExchangeExec) exchange.replaceChild(child());
        assertThat(replaced, not(sameInstance(exchange)));
        assertThat(replaced.exchange(), equalTo(exchange.exchange()));
        assertThat(replaced.planId(), equalTo(exchange.planId()));
    }

    public void testExchangeExecKeepsItsIdentityWhenItsChildrenAreReplaced() {
        ExchangeExec exchange = new ExchangeExec(Source.EMPTY, child());

        PhysicalPlan replaced = exchange.replaceChildren(List.of(child()));
        assertThat(replaced.exchange(), equalTo(exchange.exchange()));
        assertThat(replaced.planId(), equalTo(exchange.planId()));
    }

    public void testExchangeSinkExecKeepsItsExchangeWhenItsChildIsReplaced() {
        PhysicalPlan child = child();
        ExchangeExec exchange = new ExchangeExec(Source.EMPTY, child);
        ExchangeSinkExec sink = new ExchangeSinkExec(Source.EMPTY, child.output(), false, child);
        sink.withExchange(exchange.exchange());

        assertThat(sink.replaceChild(child()).exchange(), equalTo(exchange.exchange()));
        assertThat(sink.replaceChildAndUpdateOutput(child()).exchange(), equalTo(exchange.exchange()));
    }

    public void testEveryExchangeExecIsANewHop() {
        assertThat(
            new ExchangeExec(Source.EMPTY, child()).exchange().id(),
            not(equalTo(new ExchangeExec(Source.EMPTY, child()).exchange().id()))
        );
    }

    private static PhysicalPlan child() {
        List<Attribute> output = List.of();
        return new LocalSourceExec(Source.EMPTY, output, EmptyLocalSupplier.EMPTY);
    }
}
