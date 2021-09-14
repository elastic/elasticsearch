/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeUnit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;

public class SearchableSnapshotsUtils {

    static final Comparator<String> SEGMENT_FILENAME_COMPARATOR = Comparator.comparingLong(SegmentInfos::generationFromSegmentsFileName);

    public static IndexCommit emptyIndexCommit(Directory directory) {
        try {
            // We have to use a generation number that corresponds to a real segments_N file since we read this file from
            // the directory during the snapshotting process. The oldest segments_N file is the one in the snapshot
            // (recalling that we may perform some no-op commits which make newer segments_N files too). The good thing
            // about using the oldest segments_N file is that a restore will find that we already have this file "locally",
            // avoid overwriting the real one with the bogus one, and then use the real one for the rest of the recovery.
            final String oldestSegmentsFile = Arrays.stream(directory.listAll())
                .filter(s -> s.startsWith(IndexFileNames.SEGMENTS + "_"))
                .min(SEGMENT_FILENAME_COMPARATOR)
                .orElseThrow(() -> new IOException("segments_N file not found"));
            final SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
            segmentInfos.updateGeneration(SegmentInfos.readCommit(directory, oldestSegmentsFile));
            return Lucene.getIndexCommit(segmentInfos, directory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * We use {@code long} to represent offsets and lengths of files since they may be larger than 2GB, but {@code int} to represent
     * offsets and lengths of arrays in memory which are limited to 2GB in size. We quite often need to convert from the file-based world
     * of {@code long}s into the memory-based world of {@code int}s, knowing for certain that the result will not overflow. This method
     * should be used to clarify that we're doing this.
     */
    public static int toIntBytes(long l) {
        return ByteSizeUnit.BYTES.toIntBytes(l);
    }
}
