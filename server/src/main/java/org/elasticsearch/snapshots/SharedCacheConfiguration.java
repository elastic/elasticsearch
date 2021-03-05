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

/**
 * Configuration for the shared cache. The shared cache is made up of 3 sizes of pages and are used as the separate cache regions of a
 * cached file.
 * A file's regions start and index 0 with an optional header region that can be either {@link RegionType#SMALL} or
 * {@link RegionType#TINY}, depending on the requested header cache size.
 * After (starting from either index 0 or 1 depending on whether or not there is a header region) that come as many
 * {@link RegionType#LARGE} regions as necessary to cache all bytes up to the start of the optional footer region.
 * The last region of the file is then optionally {@link RegionType#TINY} if a separate footer cache region that fits it was requested.
 *
 * The shared cache file itself contains the configured number of each page type and is split into 3 sections. First come the
 * {@link RegionType#LARGE} pages, then the {@link RegionType#SMALL} and then the {@link RegionType#TINY} pages.
 */
public final class SharedCacheConfiguration {

    private final long tinyRegionSize;
    private final long smallRegionSize;
    private final long largeRegionSize;

    private final int numLargeRegions;
    private final int numSmallRegions;
    private final int numTinyRegions;

    public SharedCacheConfiguration(Settings settings) {
        long cacheSize = SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes();
        // TODO: just forcing defaults for conflicting cache- and page sizes seems wrong
        this.largeRegionSize = Math.min(SNAPSHOT_CACHE_REGION_SIZE_SETTING.get(settings).getBytes(), cacheSize / 4 + 4);
        this.smallRegionSize = Math.min(SNAPSHOT_CACHE_SMALL_REGION_SIZE.get(settings).getBytes(), largeRegionSize / 2);
        this.tinyRegionSize = Math.min(SNAPSHOT_CACHE_TINY_REGION_SIZE.get(settings).getBytes(), smallRegionSize  / 2);
        final float smallRegionShare = SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.get(settings);
        final float tinyRegionShare = SNAPSHOT_CACHE_TINY_REGION_SIZE_SHARE.get(settings);
        this.numLargeRegions = Math.round(Math.toIntExact(cacheSize / largeRegionSize) * (1 - smallRegionShare - tinyRegionShare));
        this.numSmallRegions = Math.max(Math.round(Math.toIntExact(cacheSize / smallRegionSize) * smallRegionShare), 1);
        this.numTinyRegions = Math.max(Math.round(Math.toIntExact(cacheSize / tinyRegionSize) * tinyRegionShare), 1);

        if (cacheSize > 0) {
            if (smallRegionSize > largeRegionSize || tinyRegionSize > smallRegionSize) {
                throw new IllegalArgumentException("region sizes are not consistent");
            }
            if (numLargeRegions == 0) {
                throw new IllegalArgumentException("No large regions available for the given settings");
            }
        }
    }

    public long totalSize() {
        return tinyRegionSize * numTinyRegions + smallRegionSize * numSmallRegions + largeRegionSize * numLargeRegions;
    }

    /**
     * Physical offset in the shared file by page number.
     */
    public long getPhysicalOffset(long pageNum) {
        long physicalOffset;
        if (pageNum > numLargeRegions + numSmallRegions) {
            physicalOffset = numLargeRegions * largeRegionSize + numSmallRegions * smallRegionSize
                    + (pageNum - numSmallRegions - numLargeRegions) * tinyRegionSize;
            assert physicalOffset <= numLargeRegions * largeRegionSize + numSmallRegions * smallRegionSize
                    + numTinyRegions * tinyRegionSize;
        } else if (pageNum > numLargeRegions) {
            physicalOffset = numLargeRegions * largeRegionSize + (pageNum - numLargeRegions) * smallRegionSize;
            assert physicalOffset <= numLargeRegions * largeRegionSize + numSmallRegions * smallRegionSize;
        } else {
            physicalOffset = pageNum * largeRegionSize;
            assert physicalOffset <= numLargeRegions * largeRegionSize;
        }
        return physicalOffset;
    }

    public int numLargeRegions() {
        return numLargeRegions;
    }

    public int numTinyRegions() {
        return numTinyRegions;
    }

    public int numSmallRegions() {
        return numSmallRegions;
    }

    public long regionSizeBySharedPageNumber(int pageNum) {
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
        final long headerPageSize;
        final int largeRegionIndex; // index relative to the first large region
        if (cachedHeaderSize > 0) {
            largeRegionIndex = region - 1; // we have a cached header region
            headerPageSize = cachedHeaderSize;
        } else {
            largeRegionIndex = region;
            headerPageSize = 0L;
        }

        if (largeRegionIndex < largeRegions) {
            // this is a large region so we can compute its starting offset as header page length + large regions
            return (long) largeRegionIndex * largeRegionSize + headerPageSize;
        }

        // last page can be either a complete or partial large page depending on whether or not we have a separate footer cache page
        final long largePagesAndHeader = largeRegions * largeRegionSize + headerPageSize;
        if (footerCacheLength > 0 && largePagesAndHeader > fileSize) {
            // this is the last page and we have a footer so it's just the footer cache length from the file's end here
            return fileSize - footerCacheLength;
        }
        return largePagesAndHeader;
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

    // get the region of a file of the given size that the given position belongs to
    public int getRegion(long position, long fileSize, long cacheHeaderLength, long cacheFooterLength) {
        if (position < cacheHeaderLength || fileSize <= cacheHeaderLength) {
            return 0;
        }
        assert cacheFooterLength == 0 || cacheFooterLength == tinyRegionSize;
        assert cacheHeaderLength == 0 || cacheHeaderLength == tinyRegionSize || cacheHeaderLength == smallRegionSize;
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

    /**
     * Get the relative position of the given absolute file position in its region.
     *
     * @param position           position in the file
     * @param fileSize           size of the file
     * @param cachedHeaderLength length of the header region if the header is to be cached separately or 0 if the header
     *                           is not separately cached
     * @param footerCacheLength  length of the footer region if it is separately cached or 0 of not
     * @return relative position in region
     */
    public long getRegionRelativePosition(long position, long fileSize, long cachedHeaderLength, long footerCacheLength) {
        final int region = getRegion(position, fileSize, cachedHeaderLength, footerCacheLength);
        if (region == 0) {
            return position;
        }
        return position - getRegionStart(region, fileSize, cachedHeaderLength, footerCacheLength);
    }

    /**
     * Type of the shared page by shared page number.
     */
    public RegionType sharedRegionType(int pageNum) {
        final long rsize = regionSizeBySharedPageNumber(pageNum);
        if (rsize == smallRegionSize) {
            return RegionType.SMALL;
        } else if (rsize == largeRegionSize) {
            return RegionType.LARGE;
        }
        assert rsize == tinyRegionSize;
        return RegionType.TINY;
    }

    /**
     * Compute the size of the given region of a file.
     *
     * @param fileLength         size of file overall
     * @param region             index of the file region
     * @param cachedHeaderLength length of the header region if the header is to be cached separately or 0 if the header
     *                           is not separately cached
     * @param footerCacheLength  length of the footer region if it is separately cached or 0 of not
     *
     * @return size of the file region
     */
    public long getRegionSize(long fileLength, int region, long cachedHeaderLength, long footerCacheLength) {
        final long currentRegionSize = regionMaxSize(region, fileLength, cachedHeaderLength, footerCacheLength);
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

    // maximum size that the given region may have
    private long regionMaxSize(int region, long fileSize, long cacheHeaderLength, long footerCacheLength) {
        switch (regionType(region, fileSize, cacheHeaderLength, footerCacheLength)) {
            case TINY:
                return tinyRegionSize;
            case SMALL:
                return smallRegionSize;
            default:
                return largeRegionSize;
        }
    }

    /**
     * Size of the first region to use for a file that has a header to be separately cached of the given size.
     */
    public long effectiveHeaderCacheRange(long headerCacheRequested) {
        return headerCacheRequested > smallRegionSize || headerCacheRequested == 0 ? 0 :
                (headerCacheRequested > tinyRegionSize ? smallRegionSize : tinyRegionSize);
    }

    /**
     * Size of the last region to use for a file that has a footer to be separately cached of the given size.
     */
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
