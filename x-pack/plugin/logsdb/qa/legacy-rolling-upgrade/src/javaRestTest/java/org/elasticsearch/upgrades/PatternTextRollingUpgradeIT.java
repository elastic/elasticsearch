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

public class PatternTextRollingUpgradeIT extends AbstractStringTypeRollingUpgradeIT {

    public PatternTextRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    public String stringType() {
        return "pattern_text";
    }

    @Override
    protected void testIndexing(boolean shouldIncludeKeywordMultiField) throws Exception {
        assumeTrue("pattern_text only available from 9.2.0 onward", oldClusterHasFeature("gte_v9.2.0"));
        super.testIndexing(shouldIncludeKeywordMultiField);
    }
}
