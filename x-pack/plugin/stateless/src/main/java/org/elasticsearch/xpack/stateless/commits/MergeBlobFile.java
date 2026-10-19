/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

/// A blob holding a single large merged segment, uploaded on its own and independently of the batched compound commits (BCCs), before
/// the merge is published in a commit.
///
/// The blob name is `stateless_merge_<gen>_<segment>_<discriminator>`, where:
/// - `gen` is the generation of the last commit when the merge started. Together with the primary term of the container the blob is
///   stored in, it only approximately links the merge blob to BCCs: it is **not** the generation of the BCC under which the merged
///   segment was first committed.
/// - `segment` is the name of the merged segment without its leading `_`.
/// - `discriminator` is a fixed width string of 16 lowercase hexadecimal characters, unique per engine instance.
///
/// @param primaryTerm the primary term of the container the blob is stored in
/// @param blobName    the name of the blob, from which the merge start generation, the segment and the discriminator are deduced
public record MergeBlobFile(String blobName, long primaryTerm) implements BlobFile {

    public static final String PREFIX = "stateless_merge_";

    private static final int DISCRIMINATOR_LENGTH = 16;

    /// @throws IllegalArgumentException if the blob name is not of the form `stateless_merge_<gen>_<segment>_<discriminator>`, with a
    ///                                  strictly positive generation, a valid segment name and a valid discriminator
    public MergeBlobFile {
        if (startsWithBlobPrefix(blobName) == false) {
            throw new IllegalArgumentException("not a merge blob name [" + blobName + ']');
        }
        if (parseGenerationFromBlobName(blobName) <= 0) {
            throw new IllegalArgumentException("invalid merge start generation in merge blob name [" + blobName + ']');
        }
        if (isValidSegment(segmentFromBlobName(blobName)) == false) {
            throw new IllegalArgumentException("invalid segment in merge blob name [" + blobName + ']');
        }
        if (isValidDiscriminator(discriminatorFromBlobName(blobName)) == false) {
            throw new IllegalArgumentException("invalid discriminator in merge blob name [" + blobName + ']');
        }
    }

    /// @return the generation of the last commit when the merge started
    public long mergeStartGeneration() {
        return parseGenerationFromBlobName(blobName);
    }

    /// @return the primary term of the container and the generation of the last commit when the merge started. This is **not** the
    ///         identity of a batched compound commit, nor the generation of the BCC under which the merged segment was first committed,
    ///         and must not be used as such.
    @Override
    public PrimaryTermAndGeneration termAndGeneration() {
        return new PrimaryTermAndGeneration(primaryTerm, mergeStartGeneration());
    }

    /// @return the name of the merged segment, without its leading `_`
    public String segment() {
        return segmentFromBlobName(blobName);
    }

    /// @return the discriminator of the engine instance that uploaded the blob
    public String discriminator() {
        return discriminatorFromBlobName(blobName);
    }

    public String toString() {
        return "MergeBlobFile[term=" + primaryTerm() + ", name=" + blobName() + ']';
    }

    /// @return `true` if the given blob name starts with the prefix of merge blobs. It does not check that the rest of the name is
    ///         well-formed.
    public static boolean startsWithBlobPrefix(String name) {
        return name.startsWith(PREFIX);
    }

    /// Builds the name of a merge blob.
    ///
    /// @param generation    the generation of the last commit when the merge started
    /// @param segment       the name of the merged segment, without its leading `_`
    /// @param discriminator the discriminator of the engine instance uploading the blob
    /// @throws IllegalArgumentException if the segment or the discriminator is not valid
    public static String blobName(long generation, String segment, String discriminator) {
        if (isValidSegment(segment) == false) {
            throw new IllegalArgumentException("invalid segment name [" + segment + ']');
        }
        if (isValidDiscriminator(discriminator) == false) {
            throw new IllegalArgumentException("invalid discriminator [" + discriminator + ']');
        }
        return PREFIX + generation + '_' + segment + '_' + discriminator;
    }

    /// Parses the generation out of the name of a merge blob.
    ///
    /// @throws IllegalArgumentException if the name does not have the `stateless_merge_<gen>_<segment>_<discriminator>` structure, or if
    ///                                  the generation is not a number
    public static long parseGenerationFromBlobName(String name) {
        assert startsWithBlobPrefix(name) : name;
        return Long.parseLong(name, PREFIX.length(), generationSeparatorIndex(name), 10);
    }

    // The following helpers are static so that the constructor can use them before the blobName field is assigned.

    /// Extracts the segment from a merge blob name, assuming that the name has the expected structure.
    private static String segmentFromBlobName(String name) {
        return name.substring(generationSeparatorIndex(name) + 1, name.lastIndexOf('_'));
    }

    /// Extracts the discriminator from a merge blob name, assuming that the name has the expected structure.
    private static String discriminatorFromBlobName(String name) {
        return name.substring(name.lastIndexOf('_') + 1);
    }

    /// Finds the `_` that separates the generation from the segment, which is the first `_` after the prefix. It also checks that the name
    /// contains one more `_`, the one that separates the segment from the discriminator.
    ///
    /// @return the index of the `_` that follows the generation
    /// @throws IllegalArgumentException if the name has no segment or no discriminator part
    private static int generationSeparatorIndex(String name) {
        int separator = name.indexOf('_', PREFIX.length());
        if (separator == -1 || separator == name.lastIndexOf('_')) {
            throw new IllegalArgumentException("invalid merge blob name [" + name + ']');
        }
        return separator;
    }

    /// A segment name must not be empty and must not contain a `_`, which separates the parts of the blob name.
    private static boolean isValidSegment(String segment) {
        return segment.isEmpty() == false && segment.indexOf('_') == -1;
    }

    /// A discriminator is 8 random bytes formatted as 16 lowercase hexadecimal characters.
    private static boolean isValidDiscriminator(String discriminator) {
        if (discriminator.length() != DISCRIMINATOR_LENGTH) {
            return false;
        }
        for (int i = 0; i < discriminator.length(); i++) {
            char c = discriminator.charAt(i);
            if ((c >= '0' && c <= '9') == false && (c >= 'a' && c <= 'f') == false) {
                return false;
            }
        }
        return true;
    }
}
