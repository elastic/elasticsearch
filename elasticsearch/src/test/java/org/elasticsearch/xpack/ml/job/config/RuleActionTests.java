/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.test.ESTestCase;

public class RuleActionTests extends ESTestCase {

    public void testForString() {
        assertEquals(RuleAction.FILTER_RESULTS, RuleAction.forString("filter_results"));
        assertEquals(RuleAction.FILTER_RESULTS, RuleAction.forString("FILTER_RESULTS"));
        assertEquals(RuleAction.FILTER_RESULTS, RuleAction.forString("fiLTer_Results"));
    }
}
