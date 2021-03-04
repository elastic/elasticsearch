/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SMALL_REGION_SIZE;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_TINY_REGION_SIZE;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_TINY_REGION_SIZE_SHARE;

public final class SharedCacheConfiguration {

    private final long tinyRegionSize;
    private final long smallRegionSize;
    private final long regionSize;

    private final int numRegions;
    private final int numSmallRegions;
    private final int numTinyRegions;

    public SharedCacheConfiguration(Settings settings) {
        final long cacheSize = SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes();
        this.regionSize = SNAPSHOT_CACHE_REGION_SIZE_SETTING.get(settings).getBytes();
        this.smallRegionSize = Math.min(SNAPSHOT_CACHE_SMALL_REGION_SIZE.get(settings).getBytes(), regionSize / 2);
        this.tinyRegionSize = SNAPSHOT_CACHE_TINY_REGION_SIZE.get(settings).getBytes();
        final float smallRegionShare = SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.get(settings);
        final float tinyRegionShare = SNAPSHOT_CACHE_TINY_REGION_SIZE_SHARE.get(settings);
        this.numRegions = Math.round(Math.toIntExact(cacheSize / regionSize) * (1 - smallRegionShare - tinyRegionShare));
        this.numSmallRegions = Math.round(Math.toIntExact(cacheSize / smallRegionSize) * smallRegionShare);
        this.numTinyRegions = Math.round(Math.toIntExact(cacheSize / tinyRegionSize) * tinyRegionShare);

        if (smallRegionSize * 2 > regionSize || tinyRegionSize * 2> smallRegionSize) {
            throw new IllegalArgumentException("region sizes are not consistent");
        }
    }

    public long totalSize() {
        return tinyRegionSize * numTinyRegions + smallRegionSize * numSmallRegions + regionSize * numRegions;
    }

    public long getPhysicalOffset(long chunkPosition) {
        long physicalOffset;
        if (chunkPosition > numRegions + numSmallRegions) {
            physicalOffset = numRegions * regionSize + numSmallRegions * smallRegionSize
                    + (chunkPosition - numSmallRegions - numRegions) * tinyRegionSize;
            assert physicalOffset <= numRegions * regionSize + numSmallRegions * smallRegionSize + numTinyRegions * tinyRegionSize;
        } else if (chunkPosition > numRegions) {
            physicalOffset = numRegions * regionSize + (chunkPosition - numRegions) * smallRegionSize;
            assert physicalOffset <= numRegions * regionSize + numSmallRegions * smallRegionSize;
        } else {
            physicalOffset = chunkPosition * regionSize;
            assert physicalOffset <= numRegions * regionSize;
        }
        return physicalOffset;
    }

    public int numRegions() {
        return numRegions;
    }

    public int numTinyRegions() {
        return numTinyRegions;
    }

    public int numSmallRegions() {
        return numSmallRegions;
    }

    public long regionSize(int pageNum) {
        if (pageNum >= numRegions) {
            if (pageNum >= numRegions + numSmallRegions) {
                return tinyRegionSize;
            }
            return smallRegionSize;
        }
        return regionSize;
    }

    public long tinyRegionSize() {
        return tinyRegionSize;
    }

    public long smallRegionSize() {
        return smallRegionSize;
    }

    public long standardRegionSize() {
        return regionSize;
    }
}
