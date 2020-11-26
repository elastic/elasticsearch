/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Supplier;

public class OperatorOnlyRegistryTests extends ESTestCase {

    public void testSimpleOperatorOnlyApi() {
        final OperatorOnlyRegistry operatorOnlyRegistry = new OperatorOnlyRegistry();
        for (final String actionName : OperatorOnlyRegistry.SIMPLE_ACTIONS) {
            final Supplier<String> messageSupplier = operatorOnlyRegistry.check(actionName, null);
            assertNotNull(messageSupplier);
            assertNotNull(messageSupplier.get());
        }
    }

    public void testNonOperatorOnlyApi() {
        final OperatorOnlyRegistry operatorOnlyRegistry = new OperatorOnlyRegistry();
        assertNull(operatorOnlyRegistry.check(MainAction.NAME, null));
    }

}
