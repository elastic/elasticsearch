/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Supplier;

public class OperatorOnlyTests extends ESTestCase {

    public void testSimpleOperatorOnlyApi() {
        final OperatorOnly operatorOnly = new OperatorOnly();
        for (final String actionName : OperatorOnly.SIMPLE_ACTIONS) {
            final Supplier<String> messageSupplier = operatorOnly.check(actionName, null);
            assertNotNull(messageSupplier);
            assertNotNull(messageSupplier.get());
        }
    }

    public void testNonOperatorOnlyApi() {
        final OperatorOnly operatorOnly = new OperatorOnly();
        assertNull(operatorOnly.check(MainAction.NAME, null));
    }

}
