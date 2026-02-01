/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

public class ExampleSystemIndexIT extends AbstractSystemIndicesTestCase {

    @Override
    void onOldCluster() {
        // this is 7.x
        System.out.println("onOldCluster");
    }

    @Override
    void afterFirstUpgrade() {
        // this is 8.x
        System.out.println("afterFirstUpgrade");
    }

    @Override
    void afterSecondUpgrade() {
        // this is 9.x
        System.out.println("afterSecondUpgrade");
    }
}
