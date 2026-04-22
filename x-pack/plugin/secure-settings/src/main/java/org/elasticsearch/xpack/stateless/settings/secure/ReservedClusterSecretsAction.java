/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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

        // Actively remove legacy metadata from cluster state if present.
        builder.removeCustom(ClusterStateSecretsMetadata.TYPE);

        return new TransformState(builder.build(), newSecrets.getSettings().getSettingNames());
    }

    @Override
    public ClusterState remove(TransformState prevState) {
        ClusterState clusterState = prevState.state();
        if (clusterState.custom(ClusterSecrets.TYPE) == null && clusterState.custom(ClusterStateSecretsMetadata.TYPE) == null) {
            return clusterState;
        }
        return ClusterState.builder(clusterState)
            .removeCustom(ClusterSecrets.TYPE)
            // Actively remove legacy metadata from cluster state if present.
            .removeCustom(ClusterStateSecretsMetadata.TYPE)
            .build();
    }

    @Override
    public SecureClusterStateSettings fromXContent(XContentParser parser) throws IOException {
        return SecureClusterStateSettings.fromXContent(parser);
    }
}
