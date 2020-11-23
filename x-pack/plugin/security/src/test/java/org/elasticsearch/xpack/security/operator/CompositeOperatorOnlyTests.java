/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.test.ESTestCase;

public class CompositeOperatorOnlyTests extends ESTestCase {

    public void testSimpleOperatorOnlyApi() {
        final CompositeOperatorOnly compositeOperatorOnly = new CompositeOperatorOnly();
        for (final String actionName : CompositeOperatorOnly.ActionOperatorOnly.SIMPLE_ACTIONS) {
            final OperatorOnly.Result result = compositeOperatorOnly.check(actionName, null);
            assertEquals(OperatorOnly.Status.YES, result.getStatus());
            assertNotNull(result.getMessage());
        }
    }

    public void testNonOperatorOnlyApi() {
        final CompositeOperatorOnly compositeOperatorOnly = new CompositeOperatorOnly();
        final OperatorOnly.Result result = compositeOperatorOnly.check(MainAction.NAME, null);
        assertEquals(result, OperatorOnly.RESULT_NO);
    }

}
