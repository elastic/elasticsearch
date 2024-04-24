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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;

class ReplicasUpdater {

    private static final Logger logger = LogManager.getLogger(ReplicasUpdater.class);

    private final NodeClient client;

    ReplicasUpdater(NodeClient client) {
        this.client = client;
    }

    private void publishUpdate(Map<Integer, List<String>> replicaUpdates) {
        for (Map.Entry<Integer, List<String>> entry : replicaUpdates.entrySet()) {
            int numReplicas = entry.getKey();
            List<String> indices = entry.getValue();
            Settings settings = Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), numReplicas).build();
            UpdateSettingsRequest request = new UpdateSettingsRequest(settings, indices.toArray(new String[0]));
            client.executeLocally(TransportUpdateSettingsAction.TYPE, request, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    logger.info("Updated replicas for " + indices + " to " + numReplicas);
                    // TODO should we clear the counter for involved indices or can we rely on
                    // not getting any updates for those indices once the replica setting switch is propagated?
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Error updating replicas for " + indices + " to " + numReplicas, e);
                    // TODO maybe log?
                }
            });
        }
    }
}
