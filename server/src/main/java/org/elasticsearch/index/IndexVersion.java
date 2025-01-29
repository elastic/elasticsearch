/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.internal.VersionExtension;
import org.elasticsearch.plugins.ExtensionLoader;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ServiceLoader;

/**
 * The index version.
 * <p>
 * Prior to 8.8.0, the node {@link Version} was used everywhere. This class separates the index format version
 * from the running node version.
 * <p>
 * Each index version constant has an id number, which for versions prior to 8.9.0 is the same as the release version
 * for backwards compatibility. In 8.9.0 this is changed to an incrementing number, disconnected from the release version.
 * <p>
 * Each version constant has a unique id string. This is not actually stored in the index, but is there to ensure
 * each index version is only added to the source file once. This string needs to be unique (normally a UUID,
 * but can be any other unique nonempty string).
 * If two concurrent PRs add the same index version, the different unique ids cause a git conflict, ensuring the second PR to be merged
 * must be updated with the next free version first. Without the unique id string, git will happily merge the two versions together,
 * resulting in the same index version being used across multiple commits,
 * causing problems when you try to upgrade between those two merged commits.
 * <h2>Version compatibility</h2>
 * The earliest compatible version is hardcoded in the {@link IndexVersions#MINIMUM_COMPATIBLE} field. Previously, this was dynamically
 * calculated from the major/minor versions of {@link Version}, but {@code IndexVersion} does not have separate major/minor version
 * numbers. So the minimum compatible version is hard-coded as the index version used by the first version of the previous major release.
 * {@link IndexVersions#MINIMUM_COMPATIBLE} should be updated appropriately whenever a major release happens.
 * <h2>Adding a new version</h2>
 * A new index version should be added <em>every time</em> a change is made to the serialization protocol of one or more classes.
 * Each index version should only be used in a single merged commit (apart from BwC versions copied from {@link Version}).
 * <p>
 * To add a new index version, add a new constant at the bottom of the list that is one greater than the current highest version,
 * ensure it has a unique id, and update the {@link #current()} constant to point to the new version.
 * <h2>Reverting an index version</h2>
 * If you revert a commit with an index version change, you <em>must</em> ensure there is a <em>new</em> index version
 * representing the reverted change. <em>Do not</em> let the index version go backwards, it must <em>always</em> be incremented.
 */
public record IndexVersion(int id, Version luceneVersion) implements VersionId<IndexVersion>, ToXContentFragment {

    private static class CurrentHolder {
        private static final IndexVersion CURRENT = findCurrent();

        // finds the pluggable current version
        private static IndexVersion findCurrent() {
            var version = ExtensionLoader.loadSingleton(ServiceLoader.load(VersionExtension.class))
                .map(e -> e.getCurrentIndexVersion(IndexVersions.LATEST_DEFINED))
                .orElse(IndexVersions.LATEST_DEFINED);

            assert version.onOrAfter(IndexVersions.LATEST_DEFINED);
            assert version.luceneVersion.equals(Version.LATEST)
                : "IndexVersion must be upgraded to ["
                + Version.LATEST
                + "] is still set to ["
                + version.luceneVersion
                + "]";
            return version;
        }
    }

    public static IndexVersion readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static IndexVersion fromId(int id) {
        IndexVersion known = IndexVersions.VERSION_IDS.get(id);
        if (known != null) {
            return known;
        }

        // this is a version we don't otherwise know about
        // We need to guess the lucene version.
        // Our best guess is to use the same lucene version as the previous
        // version in the list, assuming that it didn't change.
        // if it's older than any known version use the previous major to the oldest known lucene version
        var prev = IndexVersions.VERSION_IDS.floorEntry(id);
        Version luceneVersion = prev != null
            ? prev.getValue().luceneVersion
            : Version.fromBits(IndexVersions.VERSION_IDS.firstEntry().getValue().luceneVersion.major - 1, 0, 0);

        return new IndexVersion(id, luceneVersion);
    }

    public static void writeVersion(IndexVersion version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
    }

    /**
     * Returns the minimum version of {@code version1} and {@code version2}
     */
    public static IndexVersion min(IndexVersion version1, IndexVersion version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the maximum version of {@code version1} and {@code version2}
     */
    public static IndexVersion max(IndexVersion version1, IndexVersion version2) {
        return version1.id > version2.id ? version1 : version2;
    }

    /**
     * Returns the most recent index version.
     * This should be the index version with the highest id.
     */
    public static IndexVersion current() {
        return CurrentHolder.CURRENT;
    }

    /**
     * Returns whether this index version is supported by this node version out-of-the-box.
     * This is used to distinguish between ordinary indices and archive indices that may be
     * imported into the cluster in read-only mode, and with limited functionality.
     */
    public boolean isLegacyIndexVersion() {
        return before(IndexVersions.MINIMUM_READONLY_COMPATIBLE);
    }

    public static IndexVersion getMinimumCompatibleIndexVersion(int versionId) {
        int major = versionId / 1_000_000;
        if (major == IndexVersion.current().id() / 1_000_000) {
            // same compatibility version as current
            return IndexVersions.MINIMUM_COMPATIBLE;
        } else {
            int compatId = (major-1) * 1_000_000;
            if (major <= 8) compatId += 99;
            return IndexVersion.fromId(compatId);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(id);
    }

    /**
     * Returns a string representing the Elasticsearch release version of this index version,
     * if applicable for this deployment, otherwise the raw version number.
     */
    public String toReleaseVersion() {
        return IndexVersions.VERSION_LOOKUP.apply(id);
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexVersion version = (IndexVersion) o;

        if (id != version.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
