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

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public class RecoveryCommitRegistrationHandler {
    public static final TransportVersion REQUIRED_MIN_VERSION = TransportVersion.V_8_500_066;
    private static final Logger logger = LogManager.getLogger(RecoveryCommitRegistrationHandler.class);

    private final Client client;
    private final ClusterService clusterService;

    public RecoveryCommitRegistrationHandler(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    public void register(PrimaryTermAndGeneration commit, ShardId shardId, ActionListener<PrimaryTermAndGeneration> listener) {
        var clusterMinVerion = clusterService.state().getMinTransportVersion();
        if (clusterMinVerion.before(REQUIRED_MIN_VERSION)) {
            logger.debug(
                () -> Strings.format(
                    "Skipping recovery commit registration since it requires version {} and cluster is at version {}.",
                    REQUIRED_MIN_VERSION,
                    clusterMinVerion
                )
            );
            listener.onResponse(commit);
        } else {
            client.execute(
                TransportSendRecoveryCommitRegistrationAction.TYPE,
                new RegisterCommitRequest(commit, shardId, clusterService.localNode().getId()),
                listener.map(RegisterCommitResponse::getCommit)
            );
        }
    }
}
