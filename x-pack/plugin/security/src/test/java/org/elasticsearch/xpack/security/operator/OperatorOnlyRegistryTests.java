/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;

public class OperatorOnlyRegistryTests extends ESTestCase {

    private OperatorOnlyRegistry operatorOnlyRegistry;

    @Before
    public void init() {
        operatorOnlyRegistry = new OperatorOnlyRegistry();
    }

    public void testSimpleOperatorOnlyApi() {
        for (final String actionName : OperatorOnlyRegistry.SIMPLE_ACTIONS) {
            final OperatorOnlyRegistry.OperatorPrivilegesViolation violation = operatorOnlyRegistry.check(actionName, null);
            assertNotNull(violation);
            assertThat(violation.message(), containsString("action [" + actionName + "]"));
        }
    }

    public void testNonOperatorOnlyApi() {
        final String actionName = randomValueOtherThanMany(
            OperatorOnlyRegistry.SIMPLE_ACTIONS::contains, () -> randomAlphaOfLengthBetween(10, 40));
        assertNull(operatorOnlyRegistry.check(actionName, null));
    }

    // TODO: not tests for settings yet since it's not settled whether it will be part of phase 1

}
