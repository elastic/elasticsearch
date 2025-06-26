/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.apache.logging.log4j.Logger;

public class TVBackportRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    public TVBackportRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }
    // We'd be most interested to see this failing on the patch issue where 9.0 thinks it's ahead of 8.x but it's not.
    /*
        v2  TV1,TV3,
        v3  TV1,TV2,TV3,TV4

     */
    public void testTVBackport() {
        if (isOldCluster()) {
            // TODO: Implement the test for old cluster
        } else if (isMixedCluster()) {
            // TODO: Test mixed cluster behavior
        } else if (isUpgradedCluster()) {
            // TODO: Test upgraded cluster behavior
        } else {
            fail("Unknown cluster state");
        }
    }
}
