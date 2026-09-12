/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.SourceFanInExec;

import java.util.List;

public class PlannerUtilsTests extends ESTestCase {

    public void testSplitSourceFanInUsesProducerOutputsForOrdinaryExchange() {
        Attribute common = new ReferenceAttribute(Source.EMPTY, "value", DataType.INTEGER);
        LocalSourceExec first = source("value");
        LocalSourceExec second = source("value");
        SourceFanInExec fanIn = new SourceFanInExec(Source.EMPTY, List.of(first, second), List.of(common), false);

        PlannerUtils.SourceFanInPlan split = PlannerUtils.breakPlanIntoSourceProducers(fanIn);

        assertEquals(List.of(common), ((ExchangeSourceExec) split.coordinatorPlan()).output());
        assertEquals(2, split.producers().size());
        assertEquals(first.output(), split.producers().get(0).output());
        assertEquals(second.output(), split.producers().get(1).output());
        assertFalse(split.producers().get(0).isIntermediateAgg());
    }

    public void testSplitSourceFanInUsesCommonIntermediateOutput() {
        Attribute intermediate = new ReferenceAttribute(Source.EMPTY, "intermediate", DataType.LONG);
        SourceFanInExec fanIn = new SourceFanInExec(Source.EMPTY, List.of(source("first"), source("second")), List.of(intermediate), true);

        PlannerUtils.SourceFanInPlan split = PlannerUtils.breakPlanIntoSourceProducers(fanIn);

        assertTrue(((ExchangeSourceExec) split.coordinatorPlan()).isIntermediateAgg());
        for (ExchangeSinkExec producer : split.producers()) {
            assertEquals(List.of(intermediate), producer.output());
            assertTrue(producer.isIntermediateAgg());
        }
    }

    private static LocalSourceExec source(String name) {
        List<Attribute> output = List.of(new ReferenceAttribute(Source.EMPTY, name, DataType.INTEGER));
        return new LocalSourceExec(Source.EMPTY, output, EmptyLocalSupplier.EMPTY);
    }
}
