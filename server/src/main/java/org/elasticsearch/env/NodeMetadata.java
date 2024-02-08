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

/**
 * Metadata associated with this node: its persistent node ID, the most recent IndexVersion it was aware of,
 * and version of the oldest index it stores.
 * The metadata is persisted in the data folder of this node and is reused across restarts.
 */
public record NodeMetadata(
    String nodeId,
    IndexVersion indexVersionCheckpoint,
    IndexVersion previousIndexVersionCheckpoint,
    IndexVersion oldestIndexVersion
) {

    static final String NODE_ID_KEY = "node_id";
    static final String NODE_VERSION_KEY = "node_version";
    static final String OLDEST_INDEX_VERSION_KEY = "oldest_index_version";

    public NodeMetadata(
        final String nodeId,
        final IndexVersion indexVersionCheckpoint,
        final IndexVersion previousIndexVersionCheckpoint,
        final IndexVersion oldestIndexVersion
    ) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.indexVersionCheckpoint = Objects.requireNonNull(indexVersionCheckpoint);
        this.previousIndexVersionCheckpoint = Objects.requireNonNull(previousIndexVersionCheckpoint);
        this.oldestIndexVersion = Objects.requireNonNull(oldestIndexVersion);
    }

    private NodeMetadata(final String nodeId, final IndexVersion indexVersionCheckpoint, final IndexVersion oldestIndexVersion) {
        this(nodeId, indexVersionCheckpoint, indexVersionCheckpoint, oldestIndexVersion);
    }

    public static NodeMetadata create(
        final String nodeId,
        final IndexVersion indexVersionCheckpoint,
        final IndexVersion oldestIndexVersion
    ) {
        return new NodeMetadata(nodeId, indexVersionCheckpoint, oldestIndexVersion);
    }

    public static NodeMetadata create(final String nodeId, final int indexVersion, final IndexVersion oldestIndexVersion) {
        return new NodeMetadata(nodeId, idToIndexVersionCheckpoint(indexVersion), oldestIndexVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeMetadata that = (NodeMetadata) o;
        return nodeId.equals(that.nodeId)
            && indexVersionCheckpoint.equals(that.indexVersionCheckpoint)
            && oldestIndexVersion.equals(that.oldestIndexVersion)
            && Objects.equals(previousIndexVersionCheckpoint, that.previousIndexVersionCheckpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, indexVersionCheckpoint, previousIndexVersionCheckpoint, oldestIndexVersion);
    }

    @Override
    public String toString() {
        return "NodeMetadata{"
            + "nodeId='"
            + nodeId
            + '\''
            + ", indexVersionCheckpoint="
            + indexVersionCheckpoint
            + ", previousIndexVersionCheckpoint="
            + previousIndexVersionCheckpoint
            + ", oldestIndexVersion="
            + oldestIndexVersion
            + '}';
    }

    public String nodeId() {
        return nodeId;
    }

    public IndexVersion indexVersionCheckpoint() {
        return this.indexVersionCheckpoint;
    }

    /**
     * When a node starts we read the existing node metadata from disk (see NodeEnvironment@loadNodeMetadata), store a reference to the
     * index version checkpoint that we read from there in {@code previousIndexVersionCheckpoint} and then proceed to upgrade the version to
     * the current version of the node ({@link NodeMetadata#upgradeToCurrentVersion()} before storing the node metadata again on disk.
     * In doing so, {@code previousIndexVersionCheckpoint} refers to the previously last known version that this node was started on.
     * Because different releases may share index versions, we can only use index versions as checkpoints.
     */
    public IndexVersion previousIndexVersionCheckpoint() {
        return this.previousIndexVersionCheckpoint;
    }

    public IndexVersion oldestIndexVersion() {
        return oldestIndexVersion;
    }

    @UpdateForV9
    public void verifyUpgradeToCurrentVersion() {
        assert (indexVersionCheckpoint.equals(IndexVersions.ZERO) == false) || (Version.CURRENT.major <= Version.V_7_0_0.major + 1)
            : "version is required in the node metadata from v9 onwards";

        if (indexVersionCheckpoint.before(IndexVersions.V_7_17_0)) {
            throw new IllegalStateException(
                "cannot upgrade a node from version ["
                    + indexVersionCheckpoint.toReleaseVersion()
                    + "] directly to version ["
                    + Build.current().version()
                    + "], "
                    + "upgrade to version ["
                    + Build.current().minWireCompatVersion()
                    + "] first."
            );
        }

        if (indexVersionCheckpoint.after(IndexVersion.current())) {
            throw new IllegalStateException(
                "cannot downgrade a node from version ["
                    + indexVersionCheckpoint.toReleaseVersion()
                    + "] to version ["
                    + Build.current().version()
                    + "]"
            );
        }
    }

    public NodeMetadata upgradeToCurrentVersion() {
        verifyUpgradeToCurrentVersion();

        return indexVersionCheckpoint.equals(IndexVersion.current())
            ? this
            : new NodeMetadata(nodeId, IndexVersion.current(), indexVersionCheckpoint, oldestIndexVersion);
    }

    private static class Builder {
        String nodeId;
        IndexVersion indexVersionCheckpoint;
        IndexVersion previousIndexVersionCheckpoint;
        IndexVersion oldestIndexVersion;

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public void setIndexVersionCheckpoint(int nodeVersionId) {
            this.indexVersionCheckpoint = idToIndexVersionCheckpoint(nodeVersionId);
        }

        public void setOldestIndexVersion(int oldestIndexVersion) {
            this.oldestIndexVersion = IndexVersion.fromId(oldestIndexVersion);
        }

        private IndexVersion getVersionOrFallbackToEmpty() {
            return Objects.requireNonNullElse(this.indexVersionCheckpoint, IndexVersions.ZERO);
        }

        public NodeMetadata build() {
            @UpdateForV9 // version is required in the node metadata from v9 onwards
            final IndexVersion indexVersionCheckpoint = getVersionOrFallbackToEmpty();
            final IndexVersion oldestIndexVersion;

            if (this.previousIndexVersionCheckpoint == null) {
                previousIndexVersionCheckpoint = indexVersionCheckpoint;
            }
            if (this.oldestIndexVersion == null) {
                oldestIndexVersion = IndexVersions.ZERO;
            } else {
                oldestIndexVersion = this.oldestIndexVersion;
            }

            return new NodeMetadata(nodeId, indexVersionCheckpoint, previousIndexVersionCheckpoint, oldestIndexVersion);
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
            objectParser.declareInt(Builder::setIndexVersionCheckpoint, new ParseField(NODE_VERSION_KEY));
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
            builder.field(NODE_VERSION_KEY, nodeMetadata.indexVersionCheckpoint.id());
            builder.field(OLDEST_INDEX_VERSION_KEY, nodeMetadata.oldestIndexVersion.id());
        }

        @Override
        public NodeMetadata fromXContent(XContentParser parser) throws IOException {
            return objectParser.apply(parser, null).build();
        }
    }

    /**
     * Determine an index version checkpoint from a version ID
     *
     * <p>IndexVersion IDs diverged from Version IDs in 8.11.0. The 8.11 and 8.12 lines will
     * write Version IDs in their node metadata, so we have to convert those IDs to specific
     * IndexVersions. In 8.10 and earlier, IndexVersion and Version IDs are aligned. In 8.13 and
     * later, NodeMetadata should write an IndexVersion to disk as a checkpoint.
     *
     * @param versionId a version ID as defined in {@link Version} and {@link IndexVersion}
     * @return IndexVersion
     */
    private static IndexVersion idToIndexVersionCheckpoint(int versionId) {
        // case -- ids match
        Version version = Version.fromId(versionId);
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

    public static final MetadataStateFormat<NodeMetadata> FORMAT = new NodeMetadataStateFormat(false);

}
