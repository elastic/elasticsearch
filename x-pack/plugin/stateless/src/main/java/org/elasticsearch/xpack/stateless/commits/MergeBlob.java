/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HexFormat;

/// A blob holding a single large merged segment, uploaded on its own and independently of the batched compound commits (BCCs), before
/// the merge is published in a commit.
///
/// The blob name is `stateless_merge_<segmentId>`, where `segmentId` is a unique id that Lucene assigns to the merged segment when the
/// merge is initialized (`SegmentInfo#getId()`), formatted as 32 lowercase hexadecimal characters.
///
/// Using the segment id makes the name identify the content of the blob rather than the instance that wrote it: the id is written in the
/// header of every file of the segment, and every merge gets a new one, even when it merges the same input segments and picks the same
/// segment name (for example after an engine reset). Two different merges can therefore not write to the same blob.
///
/// @param blobName    the name of the blob, from which the segment id is parsed
/// @param primaryTerm the primary term of the container the blob is stored in
public record MergeBlob(String blobName, long primaryTerm) implements Blob {

    /// Prefix of the names of merge blobs.
    public static final String PREFIX = "stateless_merge_";

    private static final int SEGMENT_ID_LENGTH = StringHelper.ID_LENGTH * 2;

    /// @throws IllegalArgumentException if the blob name is not of the form `stateless_merge_<segmentId>`, with a segment id of 32
    ///                                  lowercase hexadecimal characters
    public MergeBlob {
        if (startsWithPrefix(blobName) == false) {
            throw new IllegalArgumentException("not a merge blob name [" + blobName + ']');
        }
        if (isValidSegmentId(blobName.substring(PREFIX.length())) == false) {
            throw new IllegalArgumentException("invalid segment id in merge blob name [" + blobName + ']');
        }
    }

    /// @return the id of the merged segment, as 32 lowercase hexadecimal characters
    public String segmentId() {
        return blobName.substring(PREFIX.length());
    }

    /// Writes the blob name and the primary term. Unlike a [BlobFile], a merge blob has no generation.
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // WARNING: the blob name must stay the first thing written, as is. Blob#readFrom reads it first to find the type of the blob from
        // its prefix. BlobTests checks that every implementation of Blob does it.
        out.writeString(blobName);
        out.writeVLong(primaryTerm);
    }

    /// Reads the primary term of a merge blob whose name was already read by [Blob#readFrom(StreamInput, Class)].
    ///
    /// @throws IOException if the blob name is not well-formed
    static MergeBlob readFrom(String blobName, StreamInput in) throws IOException {
        final long primaryTerm = in.readVLong();
        try {
            return new MergeBlob(blobName, primaryTerm);
        } catch (IllegalArgumentException e) {
            throw new IOException("malformed blob name [" + blobName + ']', e);
        }
    }

    @Override
    public String toString() {
        return "MergeBlob[term=" + primaryTerm() + ", name=" + blobName() + ']';
    }

    /// @return `true` if the given blob name starts with the prefix of merge blobs. It does not check that the rest of the name is
    /// well-formed.
    public static boolean startsWithPrefix(String name) {
        return name.startsWith(PREFIX);
    }

    /// Builds the name of a merge blob.
    ///
    /// @param segmentId the id of the merged segment, as returned by `SegmentInfo#getId()`
    /// @throws IllegalArgumentException if the segment id does not have the length of Lucene segment ids
    public static String blobName(byte[] segmentId) {
        if (segmentId.length != StringHelper.ID_LENGTH) {
            throw new IllegalArgumentException("invalid segment id length [" + segmentId.length + ']');
        }
        return PREFIX + HexFormat.of().formatHex(segmentId);
    }

    /// A segment id is formatted as 32 lowercase hexadecimal characters.
    private static boolean isValidSegmentId(String segmentId) {
        if (segmentId.length() != SEGMENT_ID_LENGTH) {
            return false;
        }
        for (int i = 0; i < segmentId.length(); i++) {
            char c = segmentId.charAt(i);
            if ((c >= '0' && c <= '9') == false && (c >= 'a' && c <= 'f') == false) {
                return false;
            }
        }
        return true;
    }
}
