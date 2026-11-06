/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.engine.CommitStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Everything a {@link HollowIndexEngine} needs to know about its hollow commit, so that it can be opened without reading the
 * commit's segment infos from the store.
 */
public record HollowCommitMetadata(
    long generation,
    Map<String, String> userData,
    byte[] id,
    Version luceneVersion,
    int numDocs,
    int maxDoc,
    int segmentCount,
    Set<String> files
) {

    public HollowCommitMetadata {
        userData = Map.copyOf(userData);
        files = Set.copyOf(files);
        Objects.requireNonNull(id);
        Objects.requireNonNull(luceneVersion);
        if (IndexEngine.isLastCommitHollow(userData) == false) {
            throw new IllegalArgumentException("commit generation [" + generation + "] is not hollow");
        }
    }

    public static HollowCommitMetadata of(SegmentInfos segmentInfos) {
        try {
            return new HollowCommitMetadata(
                segmentInfos.getGeneration(),
                segmentInfos.getUserData(),
                segmentInfos.getId(),
                segmentInfos.getCommitLuceneVersion(),
                Lucene.getNumDocs(segmentInfos),
                segmentInfos.totalMaxDoc(),
                segmentInfos.size(),
                Set.copyOf(segmentInfos.files(true))
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public CommitStats commitStats() {
        return new CommitStats(userData, generation, id, numDocs, segmentCount);
    }

    public String segmentsFileName() {
        return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", generation);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof HollowCommitMetadata that
            && generation == that.generation
            && userData.equals(that.userData)
            && Arrays.equals(id, that.id)
            && luceneVersion.equals(that.luceneVersion)
            && numDocs == that.numDocs
            && maxDoc == that.maxDoc
            && segmentCount == that.segmentCount
            && files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generation, userData, Arrays.hashCode(id), luceneVersion, numDocs, maxDoc, segmentCount, files);
    }

    @Override
    public String toString() {
        return "HollowCommitMetadata[generation=" + generation + ", segmentCount=" + segmentCount + ", files=" + files.size() + ']';
    }
}
