/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE;
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

    public static final long TINY_REGION_SIZE = ByteSizeValue.ofKb(1).getBytes();
    public static final long SMALL_REGION_SIZE = ByteSizeValue.ofKb(64).getBytes();

    private final long largeRegionSize;

    private final int numLargeRegions;
    private final int numSmallRegions;
    private final int numTinyRegions;

    public SharedCacheConfiguration(Settings settings) {
        long cacheSize = SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes();
        this.largeRegionSize = SNAPSHOT_CACHE_REGION_SIZE_SETTING.get(settings).getBytes();
        if (cacheSize > 0) {
            // TODO: just forcing defaults for conflicting cache- and page sizes seems wrong
            if (largeRegionSize <= SMALL_REGION_SIZE) {
                throw new IllegalArgumentException("Large region size must be larger than small region size");
            }
            final float smallRegionShare = SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.get(settings);
            final float tinyRegionShare = SNAPSHOT_CACHE_TINY_REGION_SIZE_SHARE.get(settings);
            this.numLargeRegions = Math.round(Math.toIntExact(cacheSize / largeRegionSize) * (1 - smallRegionShare - tinyRegionShare));
            this.numSmallRegions = Math.max(Math.round(Math.toIntExact(cacheSize / SMALL_REGION_SIZE) * smallRegionShare), 1);
            this.numTinyRegions = Math.max(Math.round(Math.toIntExact(cacheSize / TINY_REGION_SIZE) * tinyRegionShare), 1);
            if (numLargeRegions == 0) {
                throw new IllegalArgumentException("No large regions available for the given settings");
            }
        } else {
            numLargeRegions = 0;
            numSmallRegions = 0;
            numTinyRegions = 0;
        }
    }

    public long totalSize() {
        return TINY_REGION_SIZE * numTinyRegions + SMALL_REGION_SIZE * numSmallRegions + largeRegionSize * numLargeRegions;
    }

    /**
     * Physical offset in the shared file by page number.
     */
    public long getPhysicalOffset(long pageNum) {
        long physicalOffset;
        if (pageNum > numLargeRegions + numSmallRegions) {
            physicalOffset = numLargeRegions * largeRegionSize + numSmallRegions * SMALL_REGION_SIZE
                    + (pageNum - numSmallRegions - numLargeRegions) * TINY_REGION_SIZE;
            assert physicalOffset <= numLargeRegions * largeRegionSize + numSmallRegions * SMALL_REGION_SIZE
                    + numTinyRegions * TINY_REGION_SIZE;
        } else if (pageNum > numLargeRegions) {
            physicalOffset = numLargeRegions * largeRegionSize + (pageNum - numLargeRegions) * SMALL_REGION_SIZE;
            assert physicalOffset <= numLargeRegions * largeRegionSize + numSmallRegions * SMALL_REGION_SIZE;
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
                return TINY_REGION_SIZE;
            }
            return SMALL_REGION_SIZE;
        }
        return largeRegionSize;
    }

    /**
     * Computes the offset in the file at which the given region starts.
     *
     * @param region            region index
     * @param fileSize          size of the fill overall
     * @param cachedHeaderSize  number of bytes that should be cached to a separate header region
     * @param footerCacheLength number of bytes that should be cached to a separate footer regino
     * @return                  offset at which the requested region starts
     */
    public long getRegionStart(int region, long fileSize, long cachedHeaderSize, long footerCacheLength) {
        if (region == 0) {
            // first region starts at the beginning of the file
            return 0L;
        }
        // since we are not in the first page the region starts at least at the size of the separately cached header page
        final boolean hasHeaderPage = cachedHeaderSize > 0;
        if (hasHeaderPage && region == 1) {
            return cachedHeaderSize;
        }

        // if the region is not the first region it can either be a large region or the separate footer cache page
        final int largeRegions = largeRegions(fileSize, cachedHeaderSize, footerCacheLength);
        // The number of large regions determines the highest region index at which a large region starts.
        // If there is a header page then its simply equal to the number of large regions, otherwise we must deduct one to go from count to
        // region index
        final int largeRegionMaxIndex = hasHeaderPage ? largeRegions : largeRegions - 1;
        if (region <= largeRegionMaxIndex) {
            return cachedHeaderSize + (region - (hasHeaderPage ? 1 : 0)) * largeRegionSize;
        }

        // the given region is the last region in the file but not a large region, it must be the footer cache region
        assert region == endingRegion(fileSize, cachedHeaderSize, footerCacheLength);
        assert footerCacheLength > 0;
        return fileSize - footerCacheLength;
    }

    // returns the index of the last region in the file
    private int endingRegion(long fileSize, long cachedHeaderSize, long footerCacheLength) {
        return getRegion(fileSize - 1, fileSize, cachedHeaderSize, footerCacheLength);
    }

    public RegionType regionType(int region, long fileSize, long cacheHeaderLength, long footerCacheLength) {
        if (region == 0 && cacheHeaderLength > 0) {
            if (cacheHeaderLength <= TINY_REGION_SIZE) {
                return RegionType.TINY;
            }
            if (cacheHeaderLength <= SMALL_REGION_SIZE) {
                return RegionType.SMALL;
            }
        }
        if (footerCacheLength > 0 && region == endingRegion(fileSize, cacheHeaderLength, footerCacheLength)) {
            return RegionType.TINY;
        }
        return RegionType.LARGE;
    }

    // get the region of a file of the given size that the given position belongs to
    public int getRegion(long position, long fileSize, long cacheHeaderLength, long cacheFooterLength) {
        assert assertRegionParameters(fileSize, position, cacheHeaderLength, cacheFooterLength);

        // position is within the separately cached header region's length so its the first region
        if (position < cacheHeaderLength) {
            return 0;
        }

        // the position is not inside a separately cached header region so its either inside the footer region or inside a large region

        // first determine the number of large regions in this file
        final int largeRegions = largeRegions(fileSize, cacheHeaderLength, cacheFooterLength);
        final int headerRegions = cacheHeaderLength > 0 ? 1 : 0;
        if (fileSize - position < cacheFooterLength) {
            // N large regions and one or no header regions
            return largeRegions + headerRegions;
        }

        // we are in a large region, the number of full large regions from the end of the header region plus the header region number
        // is the region index
        return Math.toIntExact((position - cacheHeaderLength) / largeRegionSize) + headerRegions;
    }

    private static boolean assertRegionParameters(long fileSize, long position, long cacheHeaderLength, long cacheFooterLength) {
        assert fileSize > 0 : "zero length file has no cache regions";
        assert position < fileSize : "Position index must be less than file size but saw [" + position + "][" + fileSize + "]";
        assert cacheFooterLength == 0 || cacheFooterLength == TINY_REGION_SIZE;
        assert cacheHeaderLength == 0 || cacheHeaderLength == TINY_REGION_SIZE || cacheHeaderLength == SMALL_REGION_SIZE;
        return true;
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
        // find this file region the position belongs to
        final int region = getRegion(position, fileSize, cachedHeaderLength, footerCacheLength);
        // relative position is the distance from the region start
        return position - getRegionStart(region, fileSize, cachedHeaderLength, footerCacheLength);
    }

    /**
     * Type of the shared page by shared page number.
     */
    public RegionType sharedRegionType(int pageNum) {
        final long rsize = regionSizeBySharedPageNumber(pageNum);
        if (rsize == SMALL_REGION_SIZE) {
            return RegionType.SMALL;
        } else if (rsize == largeRegionSize) {
            return RegionType.LARGE;
        }
        assert rsize == TINY_REGION_SIZE;
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
        final int maxRegion = endingRegion(fileLength, cachedHeaderLength, footerCacheLength);
        if (region < maxRegion) {
            // this is not the last region so the distance between the start of this region and the next region is the size of this region
            return getRegionStart(region + 1, fileLength, cachedHeaderLength, footerCacheLength) -
                    getRegionStart(region, fileLength, cachedHeaderLength, footerCacheLength);
        } else {
            // this is the last region in the file
            if (region == 0) {
                // if the file only has a single region then the size of the file is the region size
                return fileLength;
            } else {
                // the distance from the start of this region to the end of this file is the size of the region
                return fileLength - getRegionStart(region, fileLength, cachedHeaderLength, footerCacheLength);
            }
        }
    }

    /**
     * Size of the first region to use for a file that has a header to be separately cached of the given size.
     */
    public static long effectiveHeaderCacheRange(long headerCacheRequested) {
        return headerCacheRequested > SMALL_REGION_SIZE || headerCacheRequested == 0 ? 0 :
                (headerCacheRequested > TINY_REGION_SIZE ? SMALL_REGION_SIZE : TINY_REGION_SIZE);
    }

    /**
     * Size of the last region to use for a file that has a footer to be separately cached of the given size.
     */
    public long effectiveFooterCacheRange(long footerCacheRequested) {
        return footerCacheRequested > 0 && footerCacheRequested <= TINY_REGION_SIZE ? TINY_REGION_SIZE : 0;
    }

    // calculates the number of large regions required to cache a file
    private int largeRegions(long fileLength, long cacheHeaderLength, long cacheFooterLength) {

        // large regions are only used to cache the body bytes that are not covered by the optional separate header and footer regions
        final long bodyLength = fileLength - cacheHeaderLength - cacheFooterLength;
        if (bodyLength <= 0) {
            // We do not have any large pages because header and footer cover the full file
            return 0;
        }

        // number of full large regions that are needed for the body bytes
        final int fullLargeRegions = Math.toIntExact(bodyLength / largeRegionSize);
        // large regions either align exactly and all of them are fully used or we need to add another partially used one for the remainder
        // of the body bytes
        return fullLargeRegions + (bodyLength % largeRegionSize == 0 ? 0 : 1);
    }

    public enum RegionType {
        LARGE,
        SMALL,
        TINY
    }
}
