/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.index.Index;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Map;

/**
 * Keeps index boosts for {@link SearchSourceBuilder}s.
 */
public final class IndexBoosts {

    public static final float DEFAULT_INDEX_BOOST = 1.0f;

    public static final IndexBoosts EMPTY = new IndexBoosts(Map.of());

    /**
     * The current cluster's index boosts, indexed by UUID.
     */
    private final Map<String, Float> boostsByIndexUuid;

    IndexBoosts(Map<String, Float> boostsByIndexUuid) {
        this.boostsByIndexUuid = boostsByIndexUuid;
    }

    /**
     * Returns the boost for the concrete index addressed by {@code shardIt}.
     */
    public float lookup(Index index) {
        return boostsByIndexUuid.getOrDefault(index.getUUID(), DEFAULT_INDEX_BOOST);
    }
}
