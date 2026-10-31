/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingField;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;

import java.util.Objects;

/**
 * When the left shards of a co-located join line up with the right shards. The join only requires the right index to be
 * routed by the join key; a left index that is routed by its key the same way is <i>aligned</i>: left shard {@code i} only
 * holds keys whose matches are on right shard {@code i}, so its rows need no hashing and its lookups stay on the node when
 * the two shards share one.
 */
public final class ColocatedRouting {

    private ColocatedRouting() {}

    /**
     * Whether a routing value goes to the same shard number in both indices.
     */
    public static boolean sameRouting(IndexMetadata left, IndexMetadata right) {
        if (left.getNumberOfShards() != right.getNumberOfShards()) {
            return false;
        }
        if (left.isRoutingPartitionedIndex() || right.isRoutingPartitionedIndex()) {
            return false;
        }
        if (left.getReshardingMetadata() != null || right.getReshardingMetadata() != null) {
            return false;
        }
        boolean leftModulo = IndexRouting.shouldUseShardCountModRouting(left.getCreationVersion());
        boolean rightModulo = IndexRouting.shouldUseShardCountModRouting(right.getCreationVersion());
        if (leftModulo != rightModulo) {
            return false;
        }
        return leftModulo
            || (left.getRoutingNumShards() == right.getRoutingNumShards() && left.getRoutingFactor() == right.getRoutingFactor());
    }

    /**
     * Whether {@code left} is aligned with {@code right} for a join on {@code leftKeyField}: both are routed by their join key,
     * the keys produce the same routing value for the same key value, and routing values go to the same shard number.
     *
     * @param leftKeyField the field the left join key reads, or {@code null} if it is computed
     */
    public static boolean aligned(IndexMetadata left, @Nullable String leftKeyField, IndexMetadata right) {
        String leftRoutingField = IndexSettings.ROUTING_FIELD.get(left.getSettings());
        String rightRoutingField = IndexSettings.ROUTING_FIELD.get(right.getSettings());
        if (leftRoutingField.isEmpty() || leftRoutingField.equals(leftKeyField) == false || rightRoutingField.isEmpty()) {
            return false;
        }
        RoutingField.Type leftType = RoutingField.typeFromMapping(left.mapping(), leftRoutingField);
        RoutingField.Type rightType = RoutingField.typeFromMapping(right.mapping(), rightRoutingField);
        return leftType != null && Objects.equals(leftType, rightType) && sameRouting(left, right);
    }
}
