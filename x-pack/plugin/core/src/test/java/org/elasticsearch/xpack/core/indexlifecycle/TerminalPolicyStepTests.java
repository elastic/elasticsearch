/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;


import org.elasticsearch.test.ESTestCase;

public class TerminalPolicyStepTests extends ESTestCase {

    public void testKeys() {
        assertEquals(new Step.StepKey("completed", "completed", "completed"), TerminalPolicyStep.INSTANCE.getKey());
        assertEquals(null, TerminalPolicyStep.INSTANCE.getNextStepKey());
    }
}
