/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ReservedClusterSecretsAction implements ReservedClusterStateHandler<SecureClusterStateSettings> {

    @Override
    public String name() {
        return ClusterSecrets.NAME;
    }

    @Override
    public TransformState transform(SecureClusterStateSettings secureSettings, TransformState prevState) {
        ClusterState clusterState = prevState.state();
        ClusterSecrets existingSecrets = clusterState.custom(ClusterSecrets.TYPE);

        if (existingSecrets != null && existingSecrets.containsSecureSettings(secureSettings)) {
            return prevState;
        }

        long version = existingSecrets != null ? existingSecrets.getVersion() + 1 : 1L;
        ClusterSecrets newSecrets = new ClusterSecrets(version, secureSettings);

        ClusterState.Builder builder = ClusterState.builder(clusterState);
        builder.putCustom(ClusterSecrets.TYPE, newSecrets);

        return new TransformState(builder.build(), newSecrets.getSettings().getSettingNames());
    }

    @Override
    public ClusterState remove(TransformState prevState) {
        ClusterState clusterState = prevState.state();
        if (clusterState.custom(ClusterSecrets.TYPE) == null) {
            return clusterState;
        }
        return ClusterState.builder(clusterState).removeCustom(ClusterSecrets.TYPE).build();
    }

    @Override
    public SecureClusterStateSettings fromXContent(XContentParser parser) throws IOException {
        return SecureClusterStateSettings.fromXContent(parser);
    }
}
