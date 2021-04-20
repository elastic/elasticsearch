/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;

public class BaseFrozenSearchableSnapshotsIntegTestCase extends BaseSearchableSnapshotsIntegTestCase {
    @Override
    protected boolean forceSingleDataPath() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (DiscoveryNode.canContainData(otherSettings)) {
            builder.put(
                FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(),
                rarely()
                    ? randomBoolean()
                        ? new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.KB)
                        : new ByteSizeValue(randomIntBetween(1, 1000), ByteSizeUnit.BYTES)
                    : new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB)
            );
        }

        return builder.build();
    }
}
