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

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;

public class StatelessShardRoutingRoleStrategy implements ShardRoutingRoleStrategy {

    @Override
    public ShardRouting.Role newEmptyRole(int copyIndex) {
        return copyIndex == 0 ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY;
    }

    @Override
    public ShardRouting.Role newReplicaRole() {
        return ShardRouting.Role.SEARCH_ONLY;
    }
}
