/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */
package co.elastic.elasticsearch.stateless.utils;

import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;

import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.index.store.LuceneFilesExtensions;

import java.util.Comparator;

/**
 * Order commit files in an optimized order for indexing shard recoveries
 */
public class IndexingShardRecoveryComparator implements Comparator<String> {

    @Override
    public int compare(String fileName1, String fileName2) {
        // The segment_N file is usually the first file Lucene reads, so it is always prewarmed first.
        boolean segments1 = fileName1.startsWith(IndexFileNames.SEGMENTS);
        boolean segments2 = fileName2.startsWith(IndexFileNames.SEGMENTS);
        var compare = Boolean.compare(segments2, segments1);
        if (compare != 0) {
            return compare;
        }

        // Lucene then usually reads segment core info files (.si), so we prioritize them over other type of files.
        var si = LuceneFilesExtensions.SI.getExtension();
        boolean segmentInfo1 = IndexFileNames.matchesExtension(fileName1, si);
        boolean segmentInfo2 = IndexFileNames.matchesExtension(fileName2, si);
        compare = Boolean.compare(segmentInfo2, segmentInfo1);
        if (compare != 0) {
            return compare;
        }
        // Special case of two .si files: we sort them by segment names
        if (segmentInfo1 && segmentInfo2) {
            return IndexFileNames.parseSegmentName(fileName1).compareTo(IndexFileNames.parseSegmentName(fileName2));
        }

        // Lucene usually reads generational files when opening the IndexWriter
        var isGenerationalFile1 = StatelessCommitService.isGenerationalFile(fileName1);
        var isGenerationalFile2 = StatelessCommitService.isGenerationalFile(fileName2);
        compare = Boolean.compare(isGenerationalFile2, isGenerationalFile1);
        if (compare != 0) {
            return compare;
        }

        // Lucene loads a global field map when initializing the IndexWriter, so we want to prewarm .fnm files before other type of
        // files.
        var fnm = LuceneFilesExtensions.FNM.getExtension();
        boolean fields1 = IndexFileNames.matchesExtension(fileName1, fnm);
        boolean fields2 = IndexFileNames.matchesExtension(fileName2, fnm);
        compare = Boolean.compare(fields2, fields1);
        if (compare != 0) {
            return compare;
        }

        // Special case of two .fnm files: we sort them by generation in the next step

        // Lucene usually parses segment core info files (.si) in the order they are serialized in the segment_N file. We don't have
        // this exact order today (but we could add it this information in the compound commit blob in the future) so we use the segment
        // names (parsed as longs) to order them.
        var segmentName1 = isGenerationalFile1 ? IndexFileNames.parseGeneration(fileName1) : Long.MAX_VALUE;
        var segmentName2 = isGenerationalFile2 ? IndexFileNames.parseGeneration(fileName2) : Long.MAX_VALUE;
        compare = Long.compare(segmentName1, segmentName2);
        if (compare != 0) {
            return compare;
        }

        // Sort files belonging to the same segment core are sorted in a pre-defined order (see #getExtensionOrder)
        var extension1 = getExtensionOrder(fileName1);
        var extension2 = getExtensionOrder(fileName2);
        compare = Integer.compare(extension1, extension2);
        if (compare != 0) {
            return compare;
        }
        // Natural ordering as last resort
        return fileName1.compareTo(fileName2);
    }

    private static int getExtensionOrder(String fileName) {
        var ext = LuceneFilesExtensions.fromFile(fileName);
        assert ext != null || fileName.startsWith(IndexFileNames.SEGMENTS) : fileName;
        if (ext == null) {
            return 0;
        }
        // basically the order in which files are accessed when SegmentCoreReaders and SegmentReader are instantiated
        return switch (ext) {
            case SI -> 0;
            case FNM -> 1;
            case CFE, CFS -> 2;
            case BFM, BFI, DOC, POS, PAY, CMP, LKP, TMD, TIM, TIP -> 3;
            case NVM, NVD -> 4;
            case FDM, FDT, FDX -> 5;
            case TVM, TVD, TVX, TVF -> 6;
            case KDM, KDI, KDD, DIM, DII -> 7;
            case VEC, VEX, VEM, VEMF, VEMQ, VEQ -> 8;
            case LIV -> 9;
            case DVM, DVD -> 10;
            default -> Integer.MAX_VALUE;
        };
    }

}
