/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

/**
 * Format-specific configuration that varies per codec version. Groups related parameters
 * into sub-records and provides delegation methods for convenient flat access.
 *
 * @param version                    version identifiers for header validation and feature gating
 * @param termsDict                  terms dictionary block layout parameters
 * @param skipIndex                  skip index geometry parameters
 * @param numeric                    numeric encoding parameters
 * @param binary                     binary doc values compression parameters
 * @param directMonotonicBlockShift  block shift for DirectMonotonicWriter used across all field types
 * @param writePrefixPartitions      whether to write prefix-based partition metadata for the primary sort field
 */
public record TSDBDocValuesFormatConfig(
    int version,
    TermsDictConfig termsDict,
    SkipIndexConfig skipIndex,
    NumericConfig numeric,
    BinaryConfig binary,
    int directMonotonicBlockShift,
    boolean writePrefixPartitions
) {
    /** @return terms dict block mask */
    public int termsBlockLz4Mask() {
        return termsDict.blockLz4Mask();
    }

    /** @return terms dict block shift */
    public int termsBlockLz4Shift() {
        return termsDict.blockLz4Shift();
    }

    /** @return terms dict reverse index shift */
    public int termsReverseIndexShift() {
        return termsDict.reverseIndexShift();
    }

    /** @return terms dict reverse index mask */
    public int termsReverseIndexMask() {
        return termsDict.reverseIndexMask();
    }

    /** @return number of intervals per level, expressed as a shift */
    public int skipIndexLevelShift() {
        return skipIndex.levelShift();
    }

    /** @return maximum number of skip index levels */
    public int skipIndexMaxLevel() {
        return skipIndex.maxLevel();
    }

    /** @return number of documents per skip index interval */
    public int skipIndexIntervalSize() {
        return skipIndex.intervalSize();
    }

    /** @return block shift for numeric encoding */
    public int numericBlockShift() {
        return numeric.numericBlockShift();
    }

    /** @return block shift for ordinal range encoding */
    public int ordinalRangeBlockShift() {
        return numeric.ordinalRangeBlockShift();
    }

    /** @return minimum docs per ordinal for range encoding */
    public int minDocsPerOrdinalForRangeEncoding() {
        return numeric.minDocsPerOrdinalForRangeEncoding();
    }

    /** @return threshold for binary block size in bytes */
    public int blockBytesThreshold() {
        return binary.blockBytesThreshold();
    }

    /** @return threshold for binary block value count */
    public int blockCountThreshold() {
        return binary.blockCountThreshold();
    }

    /** @return whether per-block compression is enabled */
    public boolean enablePerBlockCompression() {
        return binary.enablePerBlockCompression();
    }

    /** @return compression algorithm for binary doc values */
    public BinaryDVCompressionMode binaryCompressionMode() {
        return binary.compressionMode();
    }

    /**
     * @param blockLz4Mask      terms dict block mask
     * @param blockLz4Shift     terms dict block shift
     * @param reverseIndexShift terms dict reverse index shift
     * @param reverseIndexMask  terms dict reverse index mask
     */
    public record TermsDictConfig(int blockLz4Mask, int blockLz4Shift, int reverseIndexShift, int reverseIndexMask) {}

    /**
     * @param levelShift   number of intervals per level, expressed as a shift
     * @param maxLevel     maximum number of skip index levels
     * @param intervalSize number of documents per skip index interval
     */
    public record SkipIndexConfig(int levelShift, int maxLevel, int intervalSize) {}

    /**
     * @param numericBlockShift                 block shift for numeric value encoding
     * @param ordinalRangeBlockShift            block shift for ordinal range encoding
     * @param minDocsPerOrdinalForRangeEncoding minimum docs per ordinal for range encoding
     */
    public record NumericConfig(int numericBlockShift, int ordinalRangeBlockShift, int minDocsPerOrdinalForRangeEncoding) {}

    /**
     * @param blockBytesThreshold       threshold for binary block size in bytes
     * @param blockCountThreshold       threshold for binary block value count
     * @param enablePerBlockCompression whether per-block compression is enabled
     * @param compressionMode           compression algorithm for binary doc values
     */
    public record BinaryConfig(
        int blockBytesThreshold,
        int blockCountThreshold,
        boolean enablePerBlockCompression,
        BinaryDVCompressionMode compressionMode
    ) {}

    public static final int VERSION_START = 0;
    public static final int VERSION_BINARY_DV_COMPRESSION = 1;
    public static final int VERSION_NUMERIC_LARGE_BLOCKS = 2;
    public static final int VERSION_PREFIX_PARTITIONS = 4;
    public static final int VERSION_SEPARATE_SKIPLIST = 5;
    public static final int VERSION_ORDINAL_BLOCK_SHIFT = 6;
    public static final int VERSION_SKIPPER_MAX_VALUE_COUNT = 7;
    public static final int VERSION_REMOVE_ORDINAL_BLOCK_SHIFT = 8;
    /**
     * First version whose ES95 segments may carry the run-table ordinal layout for dimension fields.
     * Unused on this POC branch: {@link #VERSION_CURRENT} is not bumped, so run-table segments are
     * written at {@link #VERSION_CURRENT} and distinguished per field by a layout discriminator byte
     * (the {@code LAYOUT_DEFAULT}/{@code LAYOUT_RUN_TABLE} bytes defined in
     * {@code org.elasticsearch.index.codec.tsdb.es95.RunTableLayout}), and the addresses table is
     * dropped for run-table SortedSet fields. That mutates the on-disk ES95 format in place, so this
     * branch cannot read pre-POC ES95 segments and pre-POC ES95 cannot read these segments.
     *
     * <p>TODO: production would preserve backward compatibility one of two ways.
     *
     * <p>Option 1, an internal version gate that stays within ES95. Bump {@link #VERSION_CURRENT} to
     * {@code VERSION_RUN_TABLE} and write the layout discriminator and run-table layout (and drop the
     * SortedSet addresses table) only for segments at the new version. The reader branches on the
     * persisted format version: segments below {@code VERSION_RUN_TABLE} read the old way (no
     * discriminator byte, addresses table present), segments at or above it read the run-table layout.
     * {@code ES95OrdinalFieldReader} already branches on {@code segmentVersion} across
     * {@link #VERSION_ORDINAL_BLOCK_SHIFT} and {@link #VERSION_REMOVE_ORDINAL_BLOCK_SHIFT} for a
     * removed per-field {@code blockShift} byte, so the version-gated read mechanism is in place.
     *
     * <p>Option 2, a distinct codec, which is the ES96 plan. Ship the run-table as a new SPI-named
     * doc values format gated by {@code IndexVersion}, alongside ES95, exactly as ES95 shipped
     * alongside ES819. Old ES95 segments keep reading through an unchanged ES95 format.
     *
     * <p>Per-segment read resolution is automatic in Lucene under either option: the format SPI name
     * and format version are persisted per segment, so a reader always resolves the writer that
     * produced each segment. New-index write selection is gated by {@code IndexVersion}. The POC
     * skips all of this because Rally uses fresh indices, so no old segment is ever read.
     */
    public static final int VERSION_RUN_TABLE = 9;
    public static final int VERSION_CURRENT = VERSION_REMOVE_ORDINAL_BLOCK_SHIFT;
}
