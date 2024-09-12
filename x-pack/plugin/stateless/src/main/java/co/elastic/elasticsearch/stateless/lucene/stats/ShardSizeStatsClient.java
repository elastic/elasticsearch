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

package co.elastic.elasticsearch.stateless.lucene.stats;

import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader.ShardSize;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

public class ShardSizeStatsClient {

    private final NodeClient client;

    public ShardSizeStatsClient(Client client) {
        this.client = (NodeClient) client;
    }

    public void getAllShardSizes(TimeValue boostWindowInterval, ActionListener<Map<ShardId, ShardSize>> listener) {
        client.executeLocally(
            GetAllShardSizesAction.INSTANCE,
            new GetAllShardSizesAction.Request(boostWindowInterval),
            listener.map(GetAllShardSizesAction.Response::getShardSizes)
        );
    }

    public void getShardSize(ShardId shardId, TimeValue boostWindowInterval, ActionListener<ShardSize> listener) {
        client.executeLocally(
            GetShardSizeAction.INSTANCE,
            new GetShardSizeAction.Request(shardId, boostWindowInterval),
            listener.map(GetShardSizeAction.Response::getShardSize)
        );
    }
}
