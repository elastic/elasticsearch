/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.UpdateForV9;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

@UpdateForV9
public class NodesCapabilitiesUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static Boolean upgradingBeforeCapabilities;

    public NodesCapabilitiesUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Before
    public void checkBeforeHasNoCapabilities() throws IOException {
        if (upgradingBeforeCapabilities == null) {
            // try to do a _capabilities query on a node before we upgrade
            try {
                clusterHasCapability("GET", "_capabilities", List.of(), List.of());
                upgradingBeforeCapabilities = false;
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() == 400) {
                    upgradingBeforeCapabilities = true;
                } else {
                    throw e;
                }
            }
        }

        assumeTrue("Only valid when upgrading from versions without capabilities API", upgradingBeforeCapabilities);
    }

    public void testCapabilitiesReturnsFalsePartiallyUpgraded() {
        if (isMixedCluster()) {

        }
    }
}
