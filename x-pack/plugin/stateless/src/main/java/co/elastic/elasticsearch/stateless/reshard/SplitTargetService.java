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

package co.elastic.elasticsearch.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.index.shard.ShardId;

public class SplitTargetService {

    private final Client client;

    public SplitTargetService(Client client) {
        this.client = client;
    }

    public void startSplitRecovery(
        ShardId shardId,
        IndexMetadata indexMetadata,
        IndexReshardingMetadata reshardingMetadata,
        ActionListener<Void> listener
    ) {
        long sourcePrimaryTerm = indexMetadata.primaryTerm(reshardingMetadata.getSplit().sourceShard(shardId.id()));
        client.execute(TransportSplitHandoffStateAction.TYPE, new SplitStateRequest(shardId, sourcePrimaryTerm), listener.map(r -> null));
    }
}
