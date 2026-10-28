/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

/**
 * The reasons why an index or shard is being removed from a node.
 */
public enum IndexRemovalReason {
    /**
     * Shard of this index were previously assigned to this node but all shards have been relocated.
     * The index should be removed and all associated resources released. Persistent parts of the index
     * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
     */
    NO_LONGER_ASSIGNED,

    /**
     * The index is deleted. Persistent parts of the index  like the shards files, state and transaction logs are removed once
     * all resources are released.
     */
    DELETED,

    /**
     * The index has been closed. The index should be removed and all associated resources released. Persistent parts of the index
     * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
     */
    CLOSED,

    /**
     * Something around index management has failed and the index should be removed.
     * Persistent parts of the index like the shards files, state and transaction logs are kept around in the
     * case of a disaster recovery.
     */
    FAILURE,

    /**
     * The index has been reopened. The index should be removed and all associated resources released. Persistent parts of the index
     * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
     */
    REOPENED,

    /**
     * The index is closed as part of the node shutdown process. The index should be removed and all associated resources released.
     * Persistent parts of the index like the shards files, state and transaction logs should be kept around in the case the node
     * restarts.
     */
    SHUTDOWN,
}
