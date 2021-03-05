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
    private final long largeRegionSize;

    private final int numLargeRegions;
    private final int numSmallRegions;
    private final int numTinyRegions;

    public SharedCacheConfiguration(Settings settings) {
        long cacheSize = SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes();
        this.largeRegionSize = Math.min(SNAPSHOT_CACHE_REGION_SIZE_SETTING.get(settings).getBytes(), cacheSize / 4 + 4);
        this.smallRegionSize = Math.min(SNAPSHOT_CACHE_SMALL_REGION_SIZE.get(settings).getBytes(), largeRegionSize / 2);
        this.tinyRegionSize = Math.min(SNAPSHOT_CACHE_TINY_REGION_SIZE.get(settings).getBytes(), smallRegionSize  / 2);
        final float smallRegionShare = SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.get(settings);
        final float tinyRegionShare = SNAPSHOT_CACHE_TINY_REGION_SIZE_SHARE.get(settings);
        this.numLargeRegions = Math.round(Math.toIntExact(cacheSize / largeRegionSize) * (1 - smallRegionShare - tinyRegionShare));
        this.numSmallRegions = Math.round(Math.toIntExact(cacheSize / smallRegionSize) * smallRegionShare);
        this.numTinyRegions = Math.round(Math.toIntExact(cacheSize / tinyRegionSize) * tinyRegionShare);

        if (smallRegionSize > largeRegionSize || tinyRegionSize > smallRegionSize) {
            throw new IllegalArgumentException("region sizes are not consistent");
        }
        if (cacheSize > 0 && numLargeRegions == 0) {
            throw new IllegalArgumentException("No large regions available for the given settings");
        }
        if (numLargeRegions > 0 && numTinyRegions == 0) {
            throw new IllegalArgumentException("No tiny regions available for the given settings");
        }
        if (numLargeRegions > 0 && numSmallRegions == 0) {
            throw new IllegalArgumentException("No small regions available for the given settings");
        }
    }

    public long totalSize() {
        return tinyRegionSize * numTinyRegions + smallRegionSize * numSmallRegions + largeRegionSize * numLargeRegions;
    }

    public long getPhysicalOffset(long chunkPosition) {
        long physicalOffset;
        if (chunkPosition > numLargeRegions + numSmallRegions) {
            physicalOffset = numLargeRegions * largeRegionSize + numSmallRegions * smallRegionSize
                    + (chunkPosition - numSmallRegions - numLargeRegions) * tinyRegionSize;
            assert physicalOffset <= numLargeRegions * largeRegionSize + numSmallRegions * smallRegionSize
                    + numTinyRegions * tinyRegionSize;
        } else if (chunkPosition > numLargeRegions) {
            physicalOffset = numLargeRegions * largeRegionSize + (chunkPosition - numLargeRegions) * smallRegionSize;
            assert physicalOffset <= numLargeRegions * largeRegionSize + numSmallRegions * smallRegionSize;
        } else {
            physicalOffset = chunkPosition * largeRegionSize;
            assert physicalOffset <= numLargeRegions * largeRegionSize;
        }
        return physicalOffset;
    }

    public int numRegions() {
        return numLargeRegions;
    }

    public int numTinyRegions() {
        return numTinyRegions;
    }

    public int numSmallRegions() {
        return numSmallRegions;
    }

    public long regionSize(int pageNum) {
        if (pageNum >= numLargeRegions) {
            if (pageNum >= numLargeRegions + numSmallRegions) {
                return tinyRegionSize;
            }
            return smallRegionSize;
        }
        return largeRegionSize;
    }

    // get the number of bytes between the beginning of a file of the given size and the start of the given region
    public long getRegionStart(int region, long fileSize, long cachedHeaderSize, long footerCacheLength) {
        if (region == 0) {
            return 0L;
        }
        final int largeRegions = largeRegions(fileSize, cachedHeaderSize, footerCacheLength);
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
            return (long) effectiveRegion * largeRegionSize + offset;
        }

        final long withoutFooter = largeRegions * largeRegionSize + offset;
        if (footerCacheLength > 0 && withoutFooter > fileSize) {
            return fileSize - footerCacheLength;
        }

        return withoutFooter;
    }

    public RegionType regionType(int region, long fileSize, long cacheHeaderLength, long footerCacheLength) {
        if (region == 0 && cacheHeaderLength > 0) {
            if (cacheHeaderLength <= tinyRegionSize) {
                return RegionType.TINY;
            }
            if (cacheHeaderLength <= smallRegionSize) {
                return RegionType.SMALL;
            }
        }
        if (footerCacheLength > 0 && region == getRegion(fileSize, fileSize, cacheHeaderLength, footerCacheLength)) {
            return RegionType.TINY;
        }
        return RegionType.LARGE;
    }

    public long regionSize(int region, long fileSize, long cacheHeaderLength, long footerCacheLength) {
        switch (regionType(region, fileSize, cacheHeaderLength, footerCacheLength)) {
            case TINY:
                return tinyRegionSize;
            case SMALL:
                return smallRegionSize;
            default:
                return largeRegionSize;
        }
    }

    // get the region of a file of the given size that the given position belongs to
    public int getRegion(long position, long fileSize, long cacheHeaderLength, long cacheFooterLength) {
        if (position < cacheHeaderLength || fileSize <= cacheHeaderLength) {
            return 0;
        }
        assert cacheFooterLength == 0 || cacheFooterLength == tinyRegionSize;
        assert cacheHeaderLength == 0
                || cacheHeaderLength == tinyRegionSize
                || cacheHeaderLength == smallRegionSize;
        final long positionAfterHeader = position - cacheHeaderLength;
        final int numberOfLargeRegions = largeRegions(fileSize, cacheHeaderLength, cacheFooterLength);
        final int largeRegionIndex = Math.toIntExact(positionAfterHeader / largeRegionSize)
                - (positionAfterHeader > 0 && positionAfterHeader % largeRegionSize == 0 ? 1 : 0);

        boolean inFooter = cacheFooterLength > 0 && fileSize - position <= cacheFooterLength;
        if (largeRegionIndex == numberOfLargeRegions && inFooter) {
            return numberOfLargeRegions + (cacheHeaderLength > 0 ? 1 : 0);
        }

        final int lastLargeRegionIndex = largeRegionIndex + (cacheHeaderLength > 0 ? 1 : 0);

        // add one if we are in the footer region
        return lastLargeRegionIndex + (inFooter ? 1 : 0);
    }

    // get the relative position from the start footerCacheLength of its region for the given position in a file of given size
    public long getRegionRelativePosition(long position, long fileSize, long cacheHeaderLength, long footerCacheLength) {
        final int region = getRegion(position, fileSize, cacheHeaderLength, footerCacheLength);
        if (region == 0) {
            return position;
        }
        return position - getRegionStart(region, fileSize, cacheHeaderLength, footerCacheLength);
    }

    public RegionType sharedRegionType(int sharedBytesPos) {
        final long rsize = regionSize(sharedBytesPos);
        if (rsize == smallRegionSize) {
            return RegionType.SMALL;
        } else if (rsize == largeRegionSize) {
            return RegionType.LARGE;
        }
        assert rsize == tinyRegionSize;
        return RegionType.TINY;
    }

    public long getRegionSize(long fileLength, int region, long cachedHeaderLength, long footerCacheLength) {
        final long currentRegionSize = regionSize(region, fileLength, cachedHeaderLength, footerCacheLength);
        assert fileLength > 0;
        final int maxRegion = getRegion(fileLength, fileLength, cachedHeaderLength, footerCacheLength);
        assert region >= 0 && region <= maxRegion : region + " - " + maxRegion;
        final long effectiveRegionSize;
        final long regionStart = getRegionStart(region, fileLength, cachedHeaderLength, footerCacheLength);
        if (region == maxRegion && regionStart + currentRegionSize != fileLength) {
            final long relativePos = getRegionRelativePosition(fileLength, fileLength, cachedHeaderLength, footerCacheLength);
            assert relativePos != 0L;
            effectiveRegionSize = relativePos;
        } else if (region > 0 && footerCacheLength > 0 && region == maxRegion - 1) {
            effectiveRegionSize = (fileLength - footerCacheLength) - regionStart;
        } else {
            effectiveRegionSize = currentRegionSize;
        }
        assert regionStart + effectiveRegionSize <= fileLength;
        assert effectiveRegionSize > 0;
        return effectiveRegionSize;
    }

    public long effectiveHeaderCacheRange(long headerCacheRequested) {
        return headerCacheRequested > smallRegionSize || headerCacheRequested == 0 ? 0 :
                (headerCacheRequested > tinyRegionSize ? smallRegionSize : tinyRegionSize);
    }

    public long effectiveFooterCacheRange(long footerCacheRequested) {
        return footerCacheRequested > 0 && footerCacheRequested <= tinyRegionSize ? tinyRegionSize : 0;
    }

    // Number of large regions used when caching a file of the given length
    private int largeRegions(long fileLength, long cacheHeaderLength, long cacheFooterLength) {
        final long bodyLength = fileLength - cacheHeaderLength - cacheFooterLength;
        final int largeRegions = Math.toIntExact(bodyLength / largeRegionSize);
        if (largeRegions == 0 && fileLength <= cacheFooterLength + cacheFooterLength) {
            return 0;
        }
        final long remainder = bodyLength % largeRegionSize;
        return remainder == 0 ? largeRegions : largeRegions + 1;
    }

    public enum RegionType {
        LARGE,
        SMALL,
        TINY
    }
}
