/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene40.blocktree;

import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Locale;

/**
 * This is a copy of {@link Stats} shipped with Lucene, which has though package protected constructor and methods.
 * We need to copy it because we have our own fork of {@link FieldReader}.
 */
public class Stats {
    /** Byte size of the index. */
    public long indexNumBytes;

    /** Total number of terms in the field. */
    public long totalTermCount;

    /** Total number of bytes (sum of term lengths) across all terms in the field. */
    public long totalTermBytes;

    /** The number of normal (non-floor) blocks in the terms file. */
    public int nonFloorBlockCount;

    /**
     * The number of floor blocks (meta-blocks larger than the allowed {@code maxItemsPerBlock}) in
     * the terms file.
     */
    public int floorBlockCount;

    /** The number of sub-blocks within the floor blocks. */
    public int floorSubBlockCount;

    /** The number of "internal" blocks (that have both terms and sub-blocks). */
    public int mixedBlockCount;

    /** The number of "leaf" blocks (blocks that have only terms). */
    public int termsOnlyBlockCount;

    /** The number of "internal" blocks that do not contain terms (have only sub-blocks). */
    public int subBlocksOnlyBlockCount;

    /** Total number of blocks. */
    public int totalBlockCount;

    /** Number of blocks at each prefix depth. */
    public int[] blockCountByPrefixLen = new int[10];

    private int startBlockCount;
    private int endBlockCount;

    /** Total number of bytes used to store term suffixes. */
    public long totalBlockSuffixBytes;

    /**
     * Number of times each compression method has been used. 0 = uncompressed 1 = lowercase_ascii 2 =
     * LZ4
     */
    public final long[] compressionAlgorithms = new long[3];

    /** Total number of suffix bytes before compression. */
    public long totalUncompressedBlockSuffixBytes;

    /**
     * Total number of bytes used to store term stats (not including what the {@link
     * PostingsReaderBase} stores.
     */
    public long totalBlockStatsBytes;

    /**
     * Total bytes stored by the {@link PostingsReaderBase}, plus the other few vInts stored in the
     * frame.
     */
    public long totalBlockOtherBytes;

    /** Segment name. */
    public final String segment;

    /** Field name. */
    public final String field;

    Stats(String segment, String field) {
        this.segment = segment;
        this.field = field;
    }

    void startBlock(SegmentTermsEnumFrame frame, boolean isFloor) {
        totalBlockCount++;
        if (isFloor) {
            if (frame.fp == frame.fpOrig) {
                floorBlockCount++;
            }
            floorSubBlockCount++;
        } else {
            nonFloorBlockCount++;
        }

        if (blockCountByPrefixLen.length <= frame.prefix) {
            blockCountByPrefixLen = ArrayUtil.grow(blockCountByPrefixLen, 1 + frame.prefix);
        }
        blockCountByPrefixLen[frame.prefix]++;
        startBlockCount++;
        totalBlockSuffixBytes += frame.totalSuffixBytes;
        totalUncompressedBlockSuffixBytes += frame.suffixesReader.length();
        if (frame.suffixesReader != frame.suffixLengthsReader) {
            totalUncompressedBlockSuffixBytes += frame.suffixLengthsReader.length();
        }
        totalBlockStatsBytes += frame.statsReader.length();
        compressionAlgorithms[frame.compressionAlg.code]++;
    }

    void endBlock(SegmentTermsEnumFrame frame) {
        final int termCount = frame.isLeafBlock ? frame.entCount : frame.state.termBlockOrd;
        final int subBlockCount = frame.entCount - termCount;
        totalTermCount += termCount;
        if (termCount != 0 && subBlockCount != 0) {
            mixedBlockCount++;
        } else if (termCount != 0) {
            termsOnlyBlockCount++;
        } else if (subBlockCount != 0) {
            subBlocksOnlyBlockCount++;
        } else {
            throw new IllegalStateException();
        }
        endBlockCount++;
        final long otherBytes = frame.fpEnd - frame.fp - frame.totalSuffixBytes - frame.statsReader.length();
        assert otherBytes > 0 : "otherBytes=" + otherBytes + " frame.fp=" + frame.fp + " frame.fpEnd=" + frame.fpEnd;
        totalBlockOtherBytes += otherBytes;
    }

    void term(BytesRef term) {
        totalTermBytes += term.length;
    }

    void finish() {
        assert startBlockCount == endBlockCount : "startBlockCount=" + startBlockCount + " endBlockCount=" + endBlockCount;
        assert totalBlockCount == floorSubBlockCount + nonFloorBlockCount
            : "floorSubBlockCount="
                + floorSubBlockCount
                + " nonFloorBlockCount="
                + nonFloorBlockCount
                + " totalBlockCount="
                + totalBlockCount;
        assert totalBlockCount == mixedBlockCount + termsOnlyBlockCount + subBlocksOnlyBlockCount
            : "totalBlockCount="
                + totalBlockCount
                + " mixedBlockCount="
                + mixedBlockCount
                + " subBlocksOnlyBlockCount="
                + subBlocksOnlyBlockCount
                + " termsOnlyBlockCount="
                + termsOnlyBlockCount;
    }

    @Override
    public String toString() {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        PrintStream out;
        try {
            out = new PrintStream(bos, false, IOUtils.UTF_8);
        } catch (UnsupportedEncodingException bogus) {
            throw new RuntimeException(bogus);
        }

        out.println("  index FST:");
        out.println("    " + indexNumBytes + " bytes");
        out.println("  terms:");
        out.println("    " + totalTermCount + " terms");
        out.println(
            "    "
                + totalTermBytes
                + " bytes"
                + (totalTermCount != 0
                    ? " (" + String.format(Locale.ROOT, "%.1f", ((double) totalTermBytes) / totalTermCount) + " bytes/term)"
                    : "")
        );
        out.println("  blocks:");
        out.println("    " + totalBlockCount + " blocks");
        out.println("    " + termsOnlyBlockCount + " terms-only blocks");
        out.println("    " + subBlocksOnlyBlockCount + " sub-block-only blocks");
        out.println("    " + mixedBlockCount + " mixed blocks");
        out.println("    " + floorBlockCount + " floor blocks");
        out.println("    " + (totalBlockCount - floorSubBlockCount) + " non-floor blocks");
        out.println("    " + floorSubBlockCount + " floor sub-blocks");
        out.println(
            "    "
                + totalUncompressedBlockSuffixBytes
                + " term suffix bytes before compression"
                + (totalBlockCount != 0
                    ? " (" + String.format(Locale.ROOT, "%.1f", ((double) totalBlockSuffixBytes) / totalBlockCount) + " suffix-bytes/block)"
                    : "")
        );
        StringBuilder compressionCounts = new StringBuilder();
        for (int code = 0; code < compressionAlgorithms.length; ++code) {
            if (compressionAlgorithms[code] == 0) {
                continue;
            }
            if (compressionCounts.length() > 0) {
                compressionCounts.append(", ");
            }
            compressionCounts.append(CompressionAlgorithm.byCode(code));
            compressionCounts.append(": ");
            compressionCounts.append(compressionAlgorithms[code]);
        }
        out.println(
            "    "
                + totalBlockSuffixBytes
                + " compressed term suffix bytes"
                + (totalBlockCount != 0
                    ? " ("
                        + String.format(Locale.ROOT, "%.2f", ((double) totalBlockSuffixBytes) / totalUncompressedBlockSuffixBytes)
                        + " compression ratio - compression count by algorithm: "
                        + compressionCounts
                    : "")
                + ")"
        );
        out.println(
            "    "
                + totalBlockStatsBytes
                + " term stats bytes "
                + (totalBlockCount != 0
                    ? " (" + String.format(Locale.ROOT, "%.1f", ((double) totalBlockStatsBytes) / totalBlockCount) + " stats-bytes/block)"
                    : "")
        );
        out.println(
            "    "
                + totalBlockOtherBytes
                + " other bytes"
                + (totalBlockCount != 0
                    ? " (" + String.format(Locale.ROOT, "%.1f", ((double) totalBlockOtherBytes) / totalBlockCount) + " other-bytes/block)"
                    : "")
        );
        if (totalBlockCount != 0) {
            out.println("    by prefix length:");
            int total = 0;
            for (int prefix = 0; prefix < blockCountByPrefixLen.length; prefix++) {
                final int blockCount = blockCountByPrefixLen[prefix];
                total += blockCount;
                if (blockCount != 0) {
                    out.println("      " + String.format(Locale.ROOT, "%2d", prefix) + ": " + blockCount);
                }
            }
            assert totalBlockCount == total;
        }

        try {
            return bos.toString(IOUtils.UTF_8);
        } catch (UnsupportedEncodingException bogus) {
            throw new RuntimeException(bogus);
        }
    }
}
