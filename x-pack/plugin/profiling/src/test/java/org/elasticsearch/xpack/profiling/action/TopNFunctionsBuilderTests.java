/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.test.ESTestCase;

public class TopNFunctionsBuilderTests extends ESTestCase {
    public void testBuildFunctions() {
        TransportGetTopNFunctionsAction.TopNFunctionsBuilder builder = new TransportGetTopNFunctionsAction.TopNFunctionsBuilder(null);
        TopNFunction foo = foo();
        TopNFunction bar = bar();
        builder.addTopNFunction(foo);
        builder.addTopNFunction(bar);

        GetTopNFunctionsResponse response = builder.build();

        assertEquals(7L, response.getSelfCount());
        assertEquals(14L, response.getTotalCount());
        assertEquals(1.5d, response.getAnnualCo2Tons(), 0.001d);
        assertEquals(48.2d, response.getAnnualCostsUsd(), 0.001d);

        assertEquals(2, response.getTopN().size());
        assertEquals(foo, response.getTopN().get(0));
        assertEquals(bar, response.getTopN().get(1));
    }

    public void testBuildFunctionsWithLimitSmallerThanAvailableFunctionCount() {
        TransportGetTopNFunctionsAction.TopNFunctionsBuilder builder = new TransportGetTopNFunctionsAction.TopNFunctionsBuilder(1);
        TopNFunction foo = foo();
        TopNFunction bar = bar();
        builder.addTopNFunction(foo);
        builder.addTopNFunction(bar);

        GetTopNFunctionsResponse response = builder.build();

        // total values are independent of the limit
        assertEquals(7L, response.getSelfCount());
        assertEquals(14L, response.getTotalCount());
        assertEquals(1.5d, response.getAnnualCo2Tons(), 0.001d);
        assertEquals(48.2d, response.getAnnualCostsUsd(), 0.001d);

        assertEquals(1, response.getTopN().size());
        assertEquals(foo, response.getTopN().get(0));
    }

    public void testBuildFunctionsWithLimitHigherThanAvailableFunctionCount() {
        TransportGetTopNFunctionsAction.TopNFunctionsBuilder builder = new TransportGetTopNFunctionsAction.TopNFunctionsBuilder(5);
        TopNFunction foo = foo();
        TopNFunction bar = bar();
        builder.addTopNFunction(foo);
        builder.addTopNFunction(bar);

        GetTopNFunctionsResponse response = builder.build();

        assertEquals(7L, response.getSelfCount());
        assertEquals(14L, response.getTotalCount());
        assertEquals(1.5d, response.getAnnualCo2Tons(), 0.001d);
        assertEquals(48.2d, response.getAnnualCostsUsd(), 0.001d);
        // still limited to the available two functions
        assertEquals(2, response.getTopN().size());
        assertEquals(foo, response.getTopN().get(0));
        assertEquals(bar, response.getTopN().get(1));
    }

    private TopNFunction foo() {
        TopNFunction foo = function("foo");
        foo.addSelfCount(5L);
        foo.addTotalCount(10L);
        foo.addSelfAnnualCO2Tons(1.0d);
        foo.addTotalAnnualCO2Tons(2.0d);
        foo.addSelfAnnualCostsUSD(32.2d);
        foo.addTotalAnnualCostsUSD(64.4d);
        return foo;
    }

    private TopNFunction bar() {
        TopNFunction bar = function("bar");
        bar.addSelfCount(2L);
        bar.addTotalCount(4L);
        bar.addSelfAnnualCO2Tons(0.5d);
        bar.addTotalAnnualCO2Tons(1.0d);
        bar.addSelfAnnualCostsUSD(16.0d);
        bar.addTotalAnnualCostsUSD(32.0d);
        return bar;
    }

    private TopNFunction function(String name) {
        return new TopNFunction(name, 3, false, 0, name, "main.c", 1, "demo");
    }
}
