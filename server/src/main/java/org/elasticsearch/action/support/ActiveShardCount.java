/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS;

/**
 * A class whose instances represent a value for counting the number
 * of active shard copies for a given shard in an index.
 */
public record ActiveShardCount(int value) implements Writeable {

    private static final int ACTIVE_SHARD_COUNT_DEFAULT = -2;
    private static final int ALL_ACTIVE_SHARDS = -1;

    public static final ActiveShardCount DEFAULT = new ActiveShardCount(ACTIVE_SHARD_COUNT_DEFAULT);
    public static final ActiveShardCount ALL = new ActiveShardCount(ALL_ACTIVE_SHARDS);
    public static final ActiveShardCount NONE = new ActiveShardCount(0);
    public static final ActiveShardCount ONE = new ActiveShardCount(1);

    /**
     * Get an ActiveShardCount instance for the given value.  The value is first validated to ensure
     * it is a valid shard count and throws an IllegalArgumentException if validation fails.  Valid
     * values are any non-negative number.  Directly use {@link ActiveShardCount#DEFAULT} for the
     * default value (which is one shard copy) or {@link ActiveShardCount#ALL} to specify all the shards.
     */
    public static ActiveShardCount from(final int value) {
        if (value < 0) {
            throw new IllegalArgumentException("shard count cannot be a negative value");
        }
        return get(value);
    }

    /**
     * Validates that the instance is valid for the given number of replicas in an index.
     */
    public boolean validate(final int numberOfReplicas) {
        assert numberOfReplicas >= 0;
        return value <= numberOfReplicas + 1;
    }

    private static ActiveShardCount get(final int value) {
        switch (value) {
            case ACTIVE_SHARD_COUNT_DEFAULT:
                return DEFAULT;
            case ALL_ACTIVE_SHARDS:
                return ALL;
            case 1:
                return ONE;
            case 0:
                return NONE;
            default:
                assert value > 1;
                return new ActiveShardCount(value);
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeInt(value);
    }

    public static ActiveShardCount readFrom(final StreamInput in) throws IOException {
        return get(in.readInt());
    }

    /**
     * Parses the active shard count from the given string.  Valid values are "all" for
     * all shard copies, null for the default value (which defaults to one shard copy),
     * or a numeric value greater than or equal to 0. Any other input will throw an
     * IllegalArgumentException.
     */
    public static ActiveShardCount parseString(final String str) {
        if (str == null) {
            return ActiveShardCount.DEFAULT;
        } else if (str.equals("all")) {
            return ActiveShardCount.ALL;
        } else {
            int val;
            try {
                val = Integer.parseInt(str);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("cannot parse ActiveShardCount[" + str + "]", e);
            }
            return ActiveShardCount.from(val);
        }
    }

    /**
     * Returns true iff the given number of active shards is enough to meet
     * the required shard count represented by this instance.  This method
     * should only be invoked with {@link ActiveShardCount} objects created
     * from {@link #from(int)}, or {@link #NONE} or {@link #ONE}.
     */
    public boolean enoughShardsActive(final int activeShardCount) {
        if (this.value < 0) {
            throw new IllegalStateException("not enough information to resolve to shard count");
        }
        if (activeShardCount < 0) {
            throw new IllegalArgumentException("activeShardCount cannot be negative");
        }
        return this.value <= activeShardCount;
    }

    /**
     * Returns true iff the given cluster state's routing table contains enough active
     * shards for the given indices to meet the required shard count represented by this instance.
     */
    public boolean enoughShardsActive(
        @Nullable final ProjectMetadata projectMetadata,
        @Nullable final RoutingTable routingTable,
        final String... indices
    ) {
        if (this == ActiveShardCount.NONE) {
            // not waiting for any active shards
            return true;
        }
        if (projectMetadata == null || routingTable == null) {
            // while waiting, the project might have been deleted
            // in this case consider the wait for active shards over
            return true;
        }
        for (final String indexName : indices) {
            final IndexMetadata indexMetadata = projectMetadata.index(indexName);
            if (indexMetadata == null) {
                // its possible the index was deleted while waiting for active shard copies,
                // in this case, we'll just consider it that we have enough active shard copies
                // and we can stop waiting
                continue;
            }
            final IndexRoutingTable indexRoutingTable = routingTable.index(indexName);
            if (indexRoutingTable == null && indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                // its possible the index was closed while waiting for active shard copies,
                // in this case, we'll just consider it that we have enough active shard copies
                // and we can stop waiting
                continue;
            }
            assert indexRoutingTable != null;
            if (indexRoutingTable.allPrimaryShardsActive() == false) {
                // all primary shards aren't active yet
                return false;
            }
            ActiveShardCount waitForActiveShards = this;
            if (waitForActiveShards == ActiveShardCount.DEFAULT) {
                waitForActiveShards = SETTING_WAIT_FOR_ACTIVE_SHARDS.get(indexMetadata.getSettings());
            }
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                if (waitForActiveShards.enoughShardsActive(indexRoutingTable.shard(i)).enoughShards() == false) {
                    // not enough active shard copies yet
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Record that captures the decision of {@link #enoughShardsActive(IndexShardRoutingTable)}.
     * @param enoughShards the decision of whether the active shard count is enough to meet the required shard count of this instance
     * @param currentActiveShards the currently active shards considered for making the decision
     */
    public record EnoughShards(boolean enoughShards, int currentActiveShards) {};

    /**
     * Returns a {@link EnoughShards} record where the first value is true iff the active shard count in the shard routing table is enough
     * to meet the required shard count represented by this instance, and the second value is the active shard count.
     */
    public EnoughShards enoughShardsActive(final IndexShardRoutingTable shardRoutingTable) {
        final int activeShardCount = shardRoutingTable.activeShards().size();
        boolean enoughShards = false;
        int currentActiveShards = activeShardCount;
        if (this == ActiveShardCount.ALL) {
            enoughShards = activeShardCount == shardRoutingTable.size();
        } else if (value == 0) {
            enoughShards = true;
        } else if (value == 1) {
            if (shardRoutingTable.hasSearchShards()) {
                enoughShards = shardRoutingTable.getActiveSearchShardCount() >= 1;
                currentActiveShards = shardRoutingTable.getActiveSearchShardCount();
            } else {
                enoughShards = activeShardCount >= 1;
            }
        } else {
            enoughShards = shardRoutingTable.getActiveSearchShardCount() >= value;
            currentActiveShards = shardRoutingTable.getActiveSearchShardCount();
        }
        return new EnoughShards(enoughShards, currentActiveShards);
    }

    @Override
    public String toString() {
        return switch (value) {
            case ALL_ACTIVE_SHARDS -> "ALL";
            case ACTIVE_SHARD_COUNT_DEFAULT -> "DEFAULT";
            default -> Integer.toString(value);
        };
    }
}
