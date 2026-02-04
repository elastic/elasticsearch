/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.reload;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * The response for the reload secure settings action
 */
public class NodesReloadSecureSettingsResponse extends BaseNodesResponse<NodesReloadSecureSettingsResponse.NodeResponse>
    implements
        ToXContentFragment {

    public NodesReloadSecureSettingsResponse(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodesReloadSecureSettingsResponse.NodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return TransportAction.localOnly();
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodesReloadSecureSettingsResponse.NodeResponse> nodes) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (final NodesReloadSecureSettingsResponse.NodeResponse node : getNodes()) {
            builder.startObject(node.getNode().getId());
            builder.field("name", node.getNode().getName());
            final Exception e = node.reloadException();
            if (e != null) {
                builder.startObject("reload_exception");
                ElasticsearchException.generateThrowableXContent(builder, params, e);
                builder.endObject();
            }
            if (node.secureSettingNames() != null) {
                builder.array("secure_setting_names", b -> {
                    for (String settingName : Stream.of(node.secureSettingNames()).sorted().toList()) {
                        b.value(settingName);
                    }
                });
            }
            if (node.keystorePath() != null) {
                builder.field("keystore_path", node.keystorePath());
            }
            if (node.keystoreDigest() != null) {
                builder.field("keystore_digest", node.keystoreDigest());
            }
            if (node.keystoreLastModifiedTime() != null) {
                builder.field("keystore_last_modified_time", Instant.ofEpochMilli(node.keystoreLastModifiedTime()));
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static class NodeResponse extends BaseNodeResponse {

        public static final TransportVersion KEYSTORE_DETAILS = TransportVersion.fromName(
            "keystore_details_in_reload_secure_settings_response"
        );

        private final Exception reloadException;
        private final String[] secureSettingNames;
        private final String keystorePath;
        private final String keystoreDigest;
        private final Long keystoreLastModifiedTime;

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            reloadException = in.readOptionalException();
            if (in.getTransportVersion().supports(KEYSTORE_DETAILS)) {
                secureSettingNames = in.readOptionalStringArray();
                keystorePath = in.readOptionalString();
                keystoreDigest = in.readOptionalString();
                keystoreLastModifiedTime = in.readOptionalLong();
            } else {
                secureSettingNames = null;
                keystorePath = null;
                keystoreDigest = null;
                keystoreLastModifiedTime = null;
            }
        }

        public NodeResponse(
            DiscoveryNode node,
            Exception reloadException,
            String[] secureSettingNames,
            String keystorePath,
            String keystoreDigest,
            Long keystoreLastModifiedTime
        ) {
            super(node);
            this.reloadException = reloadException;
            this.secureSettingNames = secureSettingNames;
            this.keystorePath = keystorePath;
            this.keystoreDigest = keystoreDigest;
            this.keystoreLastModifiedTime = keystoreLastModifiedTime;
        }

        public Exception reloadException() {
            return this.reloadException;
        }

        public String[] secureSettingNames() {
            return this.secureSettingNames;
        }

        public String keystorePath() {
            return this.keystorePath;
        }

        public String keystoreDigest() {
            return this.keystoreDigest;
        }

        public Long keystoreLastModifiedTime() {
            return this.keystoreLastModifiedTime;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalException(reloadException);
            if (out.getTransportVersion().supports(KEYSTORE_DETAILS)) {
                out.writeOptionalStringArray(secureSettingNames);
                out.writeOptionalString(keystorePath);
                out.writeOptionalString(keystoreDigest);
                out.writeOptionalLong(keystoreLastModifiedTime);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final NodesReloadSecureSettingsResponse.NodeResponse that = (NodesReloadSecureSettingsResponse.NodeResponse) o;
            return Objects.equals(reloadException, that.reloadException)
                && Arrays.equals(secureSettingNames, that.secureSettingNames)
                && Objects.equals(keystorePath, that.keystorePath)
                && Objects.equals(keystoreDigest, that.keystoreDigest)
                && Objects.equals(keystoreLastModifiedTime, that.keystoreLastModifiedTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                reloadException,
                Arrays.hashCode(secureSettingNames),
                keystorePath,
                keystoreDigest,
                keystoreLastModifiedTime
            );
        }
    }
}
