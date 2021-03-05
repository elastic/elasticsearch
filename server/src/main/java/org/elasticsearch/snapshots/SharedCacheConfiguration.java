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

    // get the number of bytes between the beginning of a file of the given size and the start of the given region
    public long getRegionStart(int region, long fileSize, long cachedHeaderSize) {
        if (region == 0) {
            return 0L;
        }
        final int largeRegions = largeRegions(fileSize, cachedHeaderSize);
        final long offset;
        final int effectiveRegion;
        if (cachedHeaderSize > 0) {
            effectiveRegion = region - 1;
            offset = cachedHeaderSize;
        } else {
            effectiveRegion = region;
            offset = 0L;
        }

        if (effectiveRegion < largeRegions) {
            return (long) effectiveRegion * regionSize + offset;
        }
        return largeRegions * regionSize + (effectiveRegion - largeRegions) * smallRegionSize + offset;
    }

    public SharedCacheConfiguration.RegionSize regionType(int region, long fileSize, long cacheHeaderLength) {
        if (region == 0 && cacheHeaderLength > 0) {
            if (cacheHeaderLength <= tinyRegionSize) {
                return SharedCacheConfiguration.RegionSize.TINY;
            }
            if (cacheHeaderLength <= smallRegionSize) {
                return SharedCacheConfiguration.RegionSize.SMALL;
            }
        }
        final int largeRegionCount = largeRegions(fileSize, cacheHeaderLength);
        if (region < (largeRegionCount + (cacheHeaderLength > 0 ? 1 : 0))) {
            return SharedCacheConfiguration.RegionSize.STANDARD;
        }
        return SharedCacheConfiguration.RegionSize.SMALL;
    }

    public long regionSize(int region, long fileSize, long cacheHeaderLength) {
        switch (regionType(region, fileSize, cacheHeaderLength)) {
            case TINY:
                return tinyRegionSize;
            case SMALL:
                return smallRegionSize;
            default:
                return regionSize;
        }
    }

    // get the region of a file of the given size that the given position belongs to
    public int getRegion(long position, long fileSize, long cacheHeaderLength) {
        if (position < cacheHeaderLength || fileSize == cacheHeaderLength) {
            return 0;
        }
        assert cacheHeaderLength == 0
                || cacheHeaderLength == tinyRegionSize
                || cacheHeaderLength == smallRegionSize;
        final long positionAfterHeader = position - cacheHeaderLength;
        final int numberOfLargeRegions = largeRegions(fileSize, cacheHeaderLength);
        final int largeRegionIndex = Math.toIntExact(positionAfterHeader / regionSize);

        final int lastLargeRegionIndex = largeRegionIndex + (cacheHeaderLength > 0 ? 1 : 0);
        if (largeRegionIndex < numberOfLargeRegions) {
            return lastLargeRegionIndex;
        }
        final long remainder = positionAfterHeader % regionSize;
        return lastLargeRegionIndex + Math.toIntExact(remainder / smallRegionSize) + (remainder > 0 && remainder % smallRegionSize == 0
                ? -1
                : 0);
    }

    // get the relative position from the start of its region for the given position in a file of given size
    public long getRegionRelativePosition(long position, long fileSize, long cacheHeaderLength) {
        final int region = getRegion(position, fileSize, cacheHeaderLength);
        if (region == 0) {
            return position;
        }
        final long relativeToLargePage = (position - cacheHeaderLength) % regionSize;
        final SharedCacheConfiguration.RegionSize size = regionType(region, fileSize, cacheHeaderLength);
        if (size == SharedCacheConfiguration.RegionSize.SMALL) {
            return relativeToLargePage % smallRegionSize;
        } else {
            assert size == SharedCacheConfiguration.RegionSize.STANDARD : "tiny regions only for region 0";
            return relativeToLargePage;
        }
    }

    public SharedCacheConfiguration.RegionSize sharedRegionSize(int sharedBytesPos) {
        final long rsize = regionSize(sharedBytesPos);
        if (rsize == smallRegionSize) {
            return SharedCacheConfiguration.RegionSize.SMALL;
        } else if (rsize == regionSize) {
            return SharedCacheConfiguration.RegionSize.STANDARD;
        }
        assert rsize == tinyRegionSize;
        return SharedCacheConfiguration.RegionSize.TINY;
    }

    public long getRegionSize(long fileLength, int region, long cachedHeaderLength) {
        final long currentRegionSize = regionSize(region, fileLength, cachedHeaderLength);
        assert fileLength > 0;
        final int maxRegion = getRegion(fileLength, fileLength, cachedHeaderLength);
        assert region >= 0 && region <= maxRegion : region + " - " + maxRegion;
        final long effectiveRegionSize;
        final long regionStart = getRegionStart(region, fileLength, cachedHeaderLength);
        if (region == maxRegion && regionStart + currentRegionSize != fileLength) {
            assert getRegionRelativePosition(fileLength, fileLength, cachedHeaderLength) != 0L;
            effectiveRegionSize = getRegionRelativePosition(fileLength, fileLength, cachedHeaderLength);
        } else {
            effectiveRegionSize = currentRegionSize;
        }
        assert regionStart + effectiveRegionSize <= fileLength;
        return effectiveRegionSize;
    }

    public long effectiveHeaderCacheRange(long headerCacheRequested) {
        return headerCacheRequested > smallRegionSize || headerCacheRequested == 0 ? 0 :
                (headerCacheRequested > tinyRegionSize ? smallRegionSize : tinyRegionSize);
    }

    // Number of large regions used when caching a file of the given length
    private int largeRegions(long fileLength, long cacheHeaderLength) {
        final long nonHeaderLength = fileLength - cacheHeaderLength;
        final int largeRegions = Math.toIntExact(nonHeaderLength / regionSize);
        final long remainder = nonHeaderLength % regionSize;
        if (remainder == 0) {
            return largeRegions;
        }
        // if we fill up the next region more than 50%, add another region
        if (remainder > regionSize / 2) {
            return largeRegions + 1;
        }
        final int smallRegionsNeeded = Math.toIntExact(remainder / smallRegionSize);
        // arbitrary heuristic: don't create more than twice the value of (large regions + 1) to strike a balance between not wasting too
        // much space to fragmentation and not having to manage too many regions.
        // TODO: this would be nicer if we had a fixed or power of 2 ratio between the region sizes?
        if (smallRegionsNeeded <= 2 * (largeRegions + 1)) {
            return largeRegions;
        }
        // It would have taken too many small regions to cache the last partial large page so we just use another large one at the cost of
        // disk space over number of regions
        return largeRegions + 1;
    }

    public enum RegionSize {
        STANDARD,
        SMALL,
        TINY
    }
}
