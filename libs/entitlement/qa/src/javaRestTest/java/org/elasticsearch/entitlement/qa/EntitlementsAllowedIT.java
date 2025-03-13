/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.entitlement.qa.test.RestEntitlementsCheckAction;
import org.junit.ClassRule;

public class EntitlementsAllowedIT extends AbstractEntitlementsIT {

    @ClassRule
    public static EntitlementsTestRule testRule = new EntitlementsTestRule(true, ALLOWED_TEST_ENTITLEMENTS);

    public EntitlementsAllowedIT(@Name("actionName") String actionName) {
        super(actionName, true);
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return RestEntitlementsCheckAction.getCheckActionsAllowedInPlugins().stream().map(action -> new Object[] { action }).toList();
    }

    @Override
    protected String getTestRestCluster() {
        return testRule.cluster.getHttpAddresses();
    }
}
