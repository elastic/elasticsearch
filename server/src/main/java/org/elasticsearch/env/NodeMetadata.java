/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.env;

import org.elasticsearch.Version;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Metadata associated with this node: its persistent node ID and its version.
 * The metadata is persisted in the data folder of this node and is reused across restarts.
 */
public final class NodeMetadata {

    static final String NODE_ID_KEY = "node_id";
    static final String NODE_VERSION_KEY = "node_version";
    static final String OLDEST_INDEX_VERSION_KEY = "oldest_index_version";

    private final String nodeId;

    private final Version nodeVersion;

    private final Version previousNodeVersion;

    private final IndexVersion oldestIndexVersion;

    private NodeMetadata(
        final String nodeId,
        final Version nodeVersion,
        final Version previousNodeVersion,
        final IndexVersion oldestIndexVersion
    ) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.nodeVersion = Objects.requireNonNull(nodeVersion);
        this.previousNodeVersion = Objects.requireNonNull(previousNodeVersion);
        this.oldestIndexVersion = Objects.requireNonNull(oldestIndexVersion);
    }

    public NodeMetadata(final String nodeId, final Version nodeVersion, final IndexVersion oldestIndexVersion) {
        this(nodeId, nodeVersion, nodeVersion, oldestIndexVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeMetadata that = (NodeMetadata) o;
        return nodeId.equals(that.nodeId)
            && nodeVersion.equals(that.nodeVersion)
            && oldestIndexVersion.equals(that.oldestIndexVersion)
            && Objects.equals(previousNodeVersion, that.previousNodeVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeVersion, previousNodeVersion, oldestIndexVersion);
    }

    @Override
    public String toString() {
        return "NodeMetadata{"
            + "nodeId='"
            + nodeId
            + '\''
            + ", nodeVersion="
            + nodeVersion
            + ", previousNodeVersion="
            + previousNodeVersion
            + ", oldestIndexVersion="
            + oldestIndexVersion
            + '}';
    }

    public String nodeId() {
        return nodeId;
    }

    public Version nodeVersion() {
        return nodeVersion;
    }

    /**
     * When a node starts we read the existing node metadata from disk (see NodeEnvironment@loadNodeMetadata), store a reference to the
     * node version that we read from there in {@code previousNodeVersion} and then proceed to upgrade the version to
     * the current version of the node ({@link NodeMetadata#upgradeToCurrentVersion()} before storing the node metadata again on disk.
     * In doing so, {@code previousNodeVersion} refers to the previously last known version that this node was started on.
     */
    public Version previousNodeVersion() {
        return previousNodeVersion;
    }

    public IndexVersion oldestIndexVersion() {
        return oldestIndexVersion;
    }

    public void verifyUpgradeToCurrentVersion() {
        assert (nodeVersion.equals(Version.V_EMPTY) == false) || (Version.CURRENT.major <= Version.V_7_0_0.major + 1)
            : "version is required in the node metadata from v9 onwards";

        if (nodeVersion.before(Version.CURRENT.minimumCompatibilityVersion())) {
            throw new IllegalStateException(
                "cannot upgrade a node from version ["
                    + nodeVersion
                    + "] directly to version ["
                    + Version.CURRENT
                    + "], "
                    + "upgrade to version ["
                    + Version.CURRENT.minimumCompatibilityVersion()
                    + "] first."
            );
        }

        if (nodeVersion.after(Version.CURRENT)) {
            throw new IllegalStateException(
                "cannot downgrade a node from version [" + nodeVersion + "] to version [" + Version.CURRENT + "]"
            );
        }
    }

    public NodeMetadata upgradeToCurrentVersion() {
        verifyUpgradeToCurrentVersion();

        return nodeVersion.equals(Version.CURRENT) ? this : new NodeMetadata(nodeId, Version.CURRENT, nodeVersion, oldestIndexVersion);
    }

    private static class Builder {
        String nodeId;
        Version nodeVersion;
        Version previousNodeVersion;
        IndexVersion oldestIndexVersion;

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public void setNodeVersionId(int nodeVersionId) {
            this.nodeVersion = Version.fromId(nodeVersionId);
        }

        public void setPreviousNodeVersionId(int previousNodeVersionId) {
            this.previousNodeVersion = Version.fromId(previousNodeVersionId);
        }

        public void setOldestIndexVersion(int oldestIndexVersion) {
            this.oldestIndexVersion = IndexVersion.fromId(oldestIndexVersion);
        }

        public NodeMetadata build() {
            final Version nodeVersion;
            final IndexVersion oldestIndexVersion;
            if (this.nodeVersion == null) {
                assert Version.CURRENT.major <= Version.V_7_0_0.major + 1 : "version is required in the node metadata from v9 onwards";
                nodeVersion = Version.V_EMPTY;
            } else {
                nodeVersion = this.nodeVersion;
            }
            if (this.previousNodeVersion == null) {
                previousNodeVersion = nodeVersion;
            }
            if (this.oldestIndexVersion == null) {
                oldestIndexVersion = IndexVersion.ZERO;
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
            builder.field(NODE_VERSION_KEY, nodeMetadata.nodeVersion.id);
            builder.field(OLDEST_INDEX_VERSION_KEY, nodeMetadata.oldestIndexVersion.id());
        }

        @Override
        public NodeMetadata fromXContent(XContentParser parser) throws IOException {
            return objectParser.apply(parser, null).build();
        }
    }

    public static final MetadataStateFormat<NodeMetadata> FORMAT = new NodeMetadataStateFormat(false);
}
