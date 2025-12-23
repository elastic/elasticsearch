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

import org.elasticsearch.index.mapper.MapperFeatures;
import org.elasticsearch.index.mapper.TextFieldMapper;

public class MatchOnlyTextRollingUpgradeIT extends AbstractStringTypeRollingUpgradeIT {

    public MatchOnlyTextRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    public String stringType() {
        return "match_only_text";
    }

    @Override
    protected void testIndexing(boolean shouldIncludeKeywordMultiField) throws Exception {
        assumeTrue(
            "Match only text block loader bug is present and fix is not present in this cluster",
            TextFieldMapper.multiFieldsNotStoredByDefaultIndexVersionCheck(getOldClusterIndexVersion()) == oldClusterHasFeature(
                MapperFeatures.MATCH_ONLY_TEXT_BLOCK_LOADER_FIX
            )
        );
        super.testIndexing(shouldIncludeKeywordMultiField);
    }
}
