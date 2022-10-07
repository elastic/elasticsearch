/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.FunctionalBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ValueBoost;

public class ScriptScoreBoostsTests extends ESTestCase {
    public void testValueSource() {
        ValueBoost val1 = new ValueBoost("5", "add", 2f);
        String actualVal1 = val1.getSource("foo");
        assertEquals("(((doc['foo'].size() > 0) && (doc['foo'].value.toString() == '5')) ? 2 : 0)", actualVal1);

        ValueBoost val2 = new ValueBoost("5", "multiply", 2f);
        String actualVal2 = val2.getSource("foo");
        assertEquals("(((doc['foo'].size() > 0) && (doc['foo'].value.toString() == '5')) ? 2 : 1)", actualVal2);
    }

    public void testFunctionalSource() {
        FunctionalBoost linear1 = new FunctionalBoost("linear", "add", 3f);
        String actualLin1 = linear1.getSource("foo");
        assertEquals("3 * ((doc['foo'].size() > 0) ? doc['foo'].value : 0)", actualLin1);

        FunctionalBoost linear2 = new FunctionalBoost("linear", "multiply", 3f);
        String actualLin2 = linear2.getSource("foo");
        assertEquals("3 * ((doc['foo'].size() > 0) ? doc['foo'].value : 1)", actualLin2);

        FunctionalBoost exp1 = new FunctionalBoost("exponential", "add", 3f);
        String actualExp1 = exp1.getSource("foo");
        assertEquals("3 * Math.exp((doc['foo'].size() > 0) ? doc['foo'].value : 0)", actualExp1);

        FunctionalBoost exp2 = new FunctionalBoost("exponential", "multiply", 3f);
        String actualExp2 = exp2.getSource("foo");
        assertEquals("3 * Math.exp((doc['foo'].size() > 0) ? doc['foo'].value : 1)", actualExp2);

        FunctionalBoost log1 = new FunctionalBoost("logarithmic", "add", 3f);
        String actualLog1 = log1.getSource("foo");
        assertEquals("3 * Math.max(0.0001, Math.log(Math.max(0.0001, (doc['foo'].size() > 0) ? (doc['foo'].value + 1) : 0)))", actualLog1);

        FunctionalBoost log2 = new FunctionalBoost("logarithmic", "multiply", 3f);
        String actualLog2 = log2.getSource("foo");
        assertEquals("3 * Math.max(0.0001, Math.log(Math.max(0.0001, (doc['foo'].size() > 0) ? (doc['foo'].value + 1) : 1)))", actualLog2);
    }
}
