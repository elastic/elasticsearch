/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class RemoteClusterFeatureSetUsage extends XPackFeatureUsage {

    private final List<RemoteConnectionInfo> remoteConnectionInfos;

    public RemoteClusterFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.remoteConnectionInfos = in.readCollectionAsImmutableList(RemoteConnectionInfo::new);
    }

    public RemoteClusterFeatureSetUsage(List<RemoteConnectionInfo> remoteConnectionInfos) {
        super(XPackField.REMOTE_CLUSTERS, true, true);
        this.remoteConnectionInfos = remoteConnectionInfos;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(remoteConnectionInfos);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        final int size = remoteConnectionInfos.size();
        builder.field("size", size);

        int numberOfSniffModes = 0;
        int numberOfApiKeySecured = 0;
        for (var info : remoteConnectionInfos) {
            if ("sniff".equals(info.getModeInfo().modeName())) {
                numberOfSniffModes += 1;
            } else {
                assert "proxy".equals(info.getModeInfo().modeName());
            }
            if (info.hasClusterCredentials()) {
                numberOfApiKeySecured += 1;
            }
        }

        builder.startObject("mode");
        builder.field("proxy", size - numberOfSniffModes);
        builder.field("sniff", numberOfSniffModes);
        builder.endObject();

        builder.startObject("security");
        builder.field("cert", size - numberOfApiKeySecured);
        builder.field("api_key", numberOfApiKeySecured);
        builder.endObject();
    }
}
