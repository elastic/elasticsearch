/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.Nullable;

public interface ShardRoutingRoleStrategy {

    /**
     * @return the role for a copy of a new empty shard, where {@code copyIndex} is the index of the copy ({@code 0} for the primary and
     * {@code 1..N} for replicas).
     */
    ShardRouting.Role newEmptyRole(int copyIndex);

    /**
     * @return the role for a new replica copy of an existing shard.
     */
    ShardRouting.Role newReplicaRole();

    /**
     * @return the role for a copy of a new shard being restored from snapshot, where {@code copyIndex} is the index of the copy ({@code 0}
     * for the primary and {@code 1..N} for replicas).
     */
    default ShardRouting.Role newRestoredRole(int copyIndex) {
        return newEmptyRole(copyIndex);
    }

    /**
     * @return the role for a copy of a new empty shard of the given index. By default the role does not depend on the index; a strategy
     * that gives the copies of some indices other roles than the copies of the others overrides this.
     *
     * @param indexMetadata the index of the shard
     * @param copyIndex     the index of the copy ({@code 0} for the primary and {@code 1..N} for replicas)
     */
    default ShardRouting.Role newEmptyRole(IndexMetadata indexMetadata, int copyIndex) {
        return newEmptyRole(copyIndex);
    }

    /**
     * @return the role for a new replica copy of an existing shard of the given index, see {@link #newEmptyRole(IndexMetadata, int)}.
     *
     * @param indexMetadata the index of the shard, or {@code null} if the caller does not know it
     */
    default ShardRouting.Role newReplicaRole(@Nullable IndexMetadata indexMetadata) {
        return newReplicaRole();
    }

    /**
     * @return the role for a copy of a new shard of the given index being restored from snapshot, see
     * {@link #newEmptyRole(IndexMetadata, int)}.
     */
    default ShardRouting.Role newRestoredRole(IndexMetadata indexMetadata, int copyIndex) {
        return newRestoredRole(copyIndex);
    }

    /**
     * A strategy that refuses to create any new shard copies, which is used (for instance) when reading shard copies from a remote node.
     */
    ShardRoutingRoleStrategy NO_SHARD_CREATION = new ShardRoutingRoleStrategy() {
        @Override
        public ShardRouting.Role newEmptyRole(int copyIndex) {
            return newReplicaRole();
        }

        @Override
        public ShardRouting.Role newReplicaRole() {
            assert false : "no shard creation permitted";
            throw new IllegalStateException("no shard creation permitted");
        }
    };
}
