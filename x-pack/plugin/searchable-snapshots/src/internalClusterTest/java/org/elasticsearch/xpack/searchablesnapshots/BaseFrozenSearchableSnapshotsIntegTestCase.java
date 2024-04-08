/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;

import static org.elasticsearch.core.IOUtils.WINDOWS;

public abstract class BaseFrozenSearchableSnapshotsIntegTestCase extends BaseSearchableSnapshotsIntegTestCase {
    @Override
    protected boolean forceSingleDataPath() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (DiscoveryNode.canContainData(otherSettings)) {
            builder.put(
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                rarely()
                    ? randomBoolean()
                        ? new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.KB).getStringRep()
                        : new ByteSizeValue(randomIntBetween(1, 1000), ByteSizeUnit.BYTES).getStringRep()
                    : randomBoolean() ? new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB).getStringRep()
                    : new RatioValue(randomDoubleBetween(0.0d, 0.1d, false)).toString() // only use up to 0.1% disk to be friendly.
                // don't test mmap on Windows since we don't have code to unmap the shared cache file which trips assertions after tests
            ).put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), WINDOWS == false && randomBoolean());
        }

        return builder.build();
    }
}
