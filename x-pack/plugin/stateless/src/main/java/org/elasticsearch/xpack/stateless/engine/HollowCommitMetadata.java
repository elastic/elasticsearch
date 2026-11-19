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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.engine.CommitStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.ParseException;
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

    /**
     * Reads the metadata written by {@link #writeTo}. The files of the commit are not part of the serialized form, since they are the
     * files of the hollow compound commit that is always transferred along with this metadata, and must be given instead.
     */
    public HollowCommitMetadata(StreamInput in, Set<String> files) throws IOException {
        this(
            in.readVLong(),
            in.readImmutableMap(StreamInput::readString),
            in.readByteArray(),
            parseLuceneVersion(in.readString()),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            files
        );
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

    private static Version parseLuceneVersion(String version) throws IOException {
        try {
            return Version.parse(version);
        } catch (ParseException e) {
            throw new IOException("invalid Lucene version [" + version + "]", e);
        }
    }

    public CommitStats commitStats() {
        return new CommitStats(userData, generation, id, numDocs, segmentCount);
    }

    public String segmentsFileName() {
        return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", generation);
    }

    /**
     * Writes the metadata except for the files of the commit. They are the files of the hollow compound commit that is always
     * transferred along with this metadata, so writing them would only duplicate them.
     */
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(generation);
        out.writeMap(userData, StreamOutput::writeString);
        out.writeByteArray(id);
        // Lucene versions can only be fully round-tripped, including their prerelease part, through their string representation
        out.writeString(luceneVersion.toString());
        out.writeVInt(numDocs);
        out.writeVInt(maxDoc);
        out.writeVInt(segmentCount);
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
