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
    VersionConfig version,
    TermsDictConfig termsDict,
    SkipIndexConfig skipIndex,
    NumericConfig numeric,
    BinaryConfig binary,
    int directMonotonicBlockShift,
    boolean writePrefixPartitions
) {

    /** @return minimum format version for header validation */
    public int versionStart() {
        return version.start();
    }

    /** @return format version to write in file headers */
    public int versionCurrent() {
        return version.current();
    }

    /** @return version at which large numeric blocks were introduced */
    public int versionLargeBlocks() {
        return version.largeBlocks();
    }

    /** @return version at which binary DV compression was introduced */
    public int versionBinaryCompression() {
        return version.binaryCompression();
    }

    /** @return version at which prefix partitions were introduced */
    public int versionPrefixPartitions() {
        return version.prefixPartitions();
    }

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
     * @param start              minimum format version for header validation
     * @param current            format version to write in file headers
     * @param largeBlocks        version at which large numeric blocks were introduced
     * @param binaryCompression  version at which binary DV compression was introduced
     * @param prefixPartitions   version at which prefix partitions were introduced
     */
    public record VersionConfig(int start, int current, int largeBlocks, int binaryCompression, int prefixPartitions) {}

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
}
