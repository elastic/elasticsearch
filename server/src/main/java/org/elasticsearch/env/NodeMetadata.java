/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.env;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Metadata associated with this node: its persistent node ID and its version.
 * The metadata is persisted in the data folder of this node and is reused across restarts.
 */
public final class NodeMetadata {

    static final String NODE_ID_KEY = "node_id";
    static final String NODE_VERSION_KEY = "node_version";
    static final String OLDEST_INDEX_VERSION_KEY = "oldest_index_version";

    private final String nodeId;

    private final IndexVersion nodeVersionAsIndexVersion;

    private final IndexVersion previousNodeVersionAsIndexVersion;

    private final IndexVersion oldestIndexVersion;

    private NodeMetadata(
        final String nodeId,
        final IndexVersion nodeVersionAsIndexVersion,
        final IndexVersion previousNodeVersionAsIndexVersion,
        final IndexVersion oldestIndexVersion
    ) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.nodeVersionAsIndexVersion = Objects.requireNonNull(nodeVersionAsIndexVersion);
        this.previousNodeVersionAsIndexVersion = Objects.requireNonNull(previousNodeVersionAsIndexVersion);
        this.oldestIndexVersion = Objects.requireNonNull(oldestIndexVersion);
    }

    private NodeMetadata(final String nodeId, final IndexVersion nodeVersionAsIndexVersion, final IndexVersion oldestIndexVersion) {
        this(nodeId, nodeVersionAsIndexVersion, nodeVersionAsIndexVersion, oldestIndexVersion);
    }

    public static NodeMetadata createWithIndexVersion(
        final String nodeId,
        final IndexVersion nodeVersion,
        final IndexVersion oldestIndexVersion
    ) {
        return new NodeMetadata(nodeId, nodeVersion, oldestIndexVersion);
    }

    static Version indexVersionToVersion(IndexVersion indexVersion) {
        // index version id and Version id match
        if (indexVersion.before(IndexVersions.FIRST_DETACHED_INDEX_VERSION)) {
            return Version.fromId(indexVersion.id());
        }

        String releaseVersion = indexVersion.toReleaseVersion();
        // snapshot version
        if (releaseVersion.contains("snapshot")) {
            Pattern snapshotPattern = Pattern.compile("^(\\d+\\.\\d+\\.\\d+)-snapshot\\[(\\d+)]$");
            Matcher matcher = snapshotPattern.matcher(releaseVersion);
            if (matcher.matches() == false) {
                throw new AssertionError("Unexpected regex failure for [" + releaseVersion + "]");
            }
            // snapshot at or before current version
            if (Integer.parseInt(matcher.group(2)) <= IndexVersion.current().id()) {
                return Version.fromString(matcher.group(1));
            }
            // snapshot after current version
            return Version.fromId(indexVersion.id());
        } else if (indexVersion.between(IndexVersions.FIRST_DETACHED_INDEX_VERSION, IndexVersion.current())) {
            // two cases: single version (easy) or range of versions
            if (releaseVersion.contains("-") == false) {
                return Version.fromString(releaseVersion);
            }

            Pattern rangePattern = Pattern.compile("^(\\d+\\.\\d+\\.\\d+)-(\\d+\\.\\d+\\.\\d+)$");
            Matcher matcher = rangePattern.matcher(releaseVersion);
            if (matcher.matches() == false) {
                throw new AssertionError("Unexpected regex failure for [" + releaseVersion + "]");
            }
            return Version.fromString(matcher.group(2));
        } else {
            throw new AssertionError(
                "Unexpected case of index version id ["
                    + indexVersion.id()
                    + "] with release version ["
                    + indexVersion.toReleaseVersion()
                    + "]"
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeMetadata that = (NodeMetadata) o;
        return nodeId.equals(that.nodeId)
            && nodeVersionAsIndexVersion.equals(that.nodeVersionAsIndexVersion)
            && oldestIndexVersion.equals(that.oldestIndexVersion)
            && Objects.equals(previousNodeVersionAsIndexVersion, that.previousNodeVersionAsIndexVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeVersionAsIndexVersion, previousNodeVersionAsIndexVersion, oldestIndexVersion);
    }

    @Override
    public String toString() {
        return "NodeMetadata{"
            + "nodeId='"
            + nodeId
            + '\''
            + ", nodeVersion="
            + nodeVersionAsIndexVersion
            + ", previousNodeVersion="
            + previousNodeVersionAsIndexVersion
            + ", oldestIndexVersion="
            + oldestIndexVersion
            + '}';
    }

    public String nodeId() {
        return nodeId;
    }

    public Version nodeVersion() {
        return indexVersionToVersion(nodeVersionAsIndexVersion);
    }

    public IndexVersion nodeVersionAsIndexVersion() {
        return this.nodeVersionAsIndexVersion;
    }

    static IndexVersion versionToIndexVersion(Version version) {
        // case -- ids match
        if (version.before(Version.V_8_11_0)) {
            return IndexVersion.fromId(version.id());
        }

        // case 2: version ID that's diverged from indexVersion ID
        // I think this is just the 8.11 and 8.12 lines
        if (version.between(Version.V_8_11_0, Version.V_8_12_0)) {
            return IndexVersions.UPGRADE_LUCENE_9_8;
        }
        if (version.equals(Version.V_8_12_0)) {
            return IndexVersions.ES_VERSION_8_12;
        }
        if (version.between(Version.V_8_12_1, Version.V_8_13_0)) {
            return IndexVersions.ES_VERSION_8_12_1;
        }
        if (version.equals(Version.CURRENT)) {
            return IndexVersion.current();
        }
        // case 3: indexVersion ID from new code
        return IndexVersion.fromId(version.id());
    }

    /**
     * When a node starts we read the existing node metadata from disk (see NodeEnvironment@loadNodeMetadata), store a reference to the
     * node version that we read from there in {@code previousNodeVersion} and then proceed to upgrade the version to
     * the current version of the node ({@link NodeMetadata#upgradeToCurrentVersion()} before storing the node metadata again on disk.
     * In doing so, {@code previousNodeVersion} refers to the previously last known version that this node was started on.
     */
    public Version previousNodeVersion() {
        return indexVersionToVersion(previousNodeVersionAsIndexVersion);
    }

    public IndexVersion previousNodeVersionAsIndexVersion() {
        return this.previousNodeVersionAsIndexVersion;
    }

    public IndexVersion oldestIndexVersion() {
        return oldestIndexVersion;
    }

    // TODO[wrb] remove poison pill
    public void verifyUpgradeToCurrentVersion() {
        assert (nodeVersionAsIndexVersion.equals(IndexVersions.ZERO) == false) || (Version.CURRENT.major <= Version.V_7_0_0.major + 1)
            : "version is required in the node metadata from v9 onwards";

        if (nodeVersionAsIndexVersion.before(IndexVersions.V_7_17_0)) {
            throw new IllegalStateException(
                "cannot upgrade a node from version ["
                    + nodeVersionAsIndexVersion.toReleaseVersion()
                    + "] directly to version ["
                    + Build.current().version()
                    + "], "
                    + "upgrade to version ["
                    + Build.current().minWireCompatVersion()
                    + "] first."
            );
        }

        if (nodeVersionAsIndexVersion.after(IndexVersion.current())) {
            throw new IllegalStateException(
                "cannot downgrade a node from version ["
                    + nodeVersionAsIndexVersion.toReleaseVersion()
                    + "] to version ["
                    + Build.current().version()
                    + "]"
            );
        }
    }

    public NodeMetadata upgradeToCurrentVersion() {
        verifyUpgradeToCurrentVersion();

        return nodeVersionAsIndexVersion.equals(IndexVersion.current())
            ? this
            : new NodeMetadata(nodeId, IndexVersion.current(), nodeVersionAsIndexVersion, oldestIndexVersion);
    }

    private static class Builder {
        String nodeId;
        IndexVersion nodeVersion;
        IndexVersion previousNodeVersion;
        IndexVersion oldestIndexVersion;

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public void setNodeVersionId(int nodeVersionId) {
            this.nodeVersion = versionToIndexVersion(Version.fromId(nodeVersionId));
        }

        public void setOldestIndexVersion(int oldestIndexVersion) {
            this.oldestIndexVersion = IndexVersion.fromId(oldestIndexVersion);
        }

        private IndexVersion getVersionOrFallbackToEmpty() {
            return Objects.requireNonNullElse(this.nodeVersion, IndexVersions.ZERO);
        }

        public NodeMetadata build() {
            @UpdateForV9 // version is required in the node metadata from v9 onwards
            final IndexVersion nodeVersion = getVersionOrFallbackToEmpty();
            final IndexVersion oldestIndexVersion;

            if (this.previousNodeVersion == null) {
                previousNodeVersion = nodeVersion;
            }
            if (this.oldestIndexVersion == null) {
                oldestIndexVersion = IndexVersions.ZERO;
            } else {
                oldestIndexVersion = this.oldestIndexVersion;
            }

            return new NodeMetadata(nodeId, nodeVersion, previousNodeVersion, oldestIndexVersion);
        }
    }

    static class NodeMetadataStateFormat extends MetadataStateFormat<NodeMetadata> {

        private ObjectParser<Builder, Void> objectParser;

        /**
         * @param ignoreUnknownFields whether to ignore unknown fields or not. Normally we are strict about this, but
         *                            {@link OverrideNodeVersionCommand} is lenient.
         */
        NodeMetadataStateFormat(boolean ignoreUnknownFields) {
            super("node-");
            objectParser = new ObjectParser<>("node_meta_data", ignoreUnknownFields, Builder::new);
            objectParser.declareString(Builder::setNodeId, new ParseField(NODE_ID_KEY));
            objectParser.declareInt(Builder::setNodeVersionId, new ParseField(NODE_VERSION_KEY));
            objectParser.declareInt(Builder::setOldestIndexVersion, new ParseField(OLDEST_INDEX_VERSION_KEY));
        }

        @Override
        protected XContentBuilder newXContentBuilder(XContentType type, OutputStream stream) throws IOException {
            XContentBuilder xContentBuilder = super.newXContentBuilder(type, stream);
            xContentBuilder.prettyPrint();
            return xContentBuilder;
        }

        @Override
        public void toXContent(XContentBuilder builder, NodeMetadata nodeMetadata) throws IOException {
            builder.field(NODE_ID_KEY, nodeMetadata.nodeId);
            builder.field(NODE_VERSION_KEY, nodeMetadata.nodeVersionAsIndexVersion.id());
            builder.field(OLDEST_INDEX_VERSION_KEY, nodeMetadata.oldestIndexVersion.id());
        }

        @Override
        public NodeMetadata fromXContent(XContentParser parser) throws IOException {
            return objectParser.apply(parser, null).build();
        }
    }

    public static final MetadataStateFormat<NodeMetadata> FORMAT = new NodeMetadataStateFormat(false);

}
