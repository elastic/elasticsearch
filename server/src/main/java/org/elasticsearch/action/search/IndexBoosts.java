/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import joptsimple.internal.Strings;

import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Map;

/**
 * Keeps index boosts for {@link SearchSourceBuilder}s.
 */
public final class IndexBoosts {

    public static final float DEFAULT_INDEX_BOOST = 1.0f;

    public static final IndexBoosts EMPTY = new IndexBoosts(Map.of(), Map.of(), Map.of());

    /**
     * The current cluster's index boosts, indexed by UUID.
     */
    private final Map<String, Float> boostsByIndexUuid;
    /**
     * Index boosts, by name, that will be applied to all clusters.
     */
    private final Map<String, Float> universalIndexBoosts;
    /**
     * Index boosts that are applied to a specific remote only.
     */
    private final Map<String, Map<String, Float>> clusterBoosts;

    IndexBoosts(
        Map<String, Float> boostsByIndexUuid,
        Map<String, Float> universalIndexBoosts,
        Map<String, Map<String, Float>> clusterBoosts
    ) {
        this.boostsByIndexUuid = boostsByIndexUuid;
        this.universalIndexBoosts = universalIndexBoosts;
        this.clusterBoosts = clusterBoosts;
    }

    /**
     * Returns the boost for the concrete index addressed by {@code shardIt}.
     */
    public float lookup(SearchShardIterator shardIt) {
        if (Strings.isNullOrEmpty(shardIt.getClusterAlias())) {
            return boostsByIndexUuid.getOrDefault(shardIt.shardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST);
        } else {
            if (boostsByIndexUuid.containsKey(shardIt.shardId().getIndex().getUUID())) {
                return boostsByIndexUuid.get(shardIt.shardId().getIndex().getUUID());
            }
            if (universalIndexBoosts.containsKey(shardIt.shardId().getIndex().getName())) {
                return universalIndexBoosts.get(shardIt.shardId().getIndex().getName());
            }
            if (clusterBoosts.containsKey(shardIt.getClusterAlias())
                && clusterBoosts.get(shardIt.getClusterAlias()).containsKey(shardIt.shardId().getIndex().getName())) {
                return clusterBoosts.get(shardIt.getClusterAlias()).get(shardIt.shardId().getIndex().getName());
            }
        }
        return DEFAULT_INDEX_BOOST;
    }
}
