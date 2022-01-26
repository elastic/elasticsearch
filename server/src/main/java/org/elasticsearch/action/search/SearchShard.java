/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.util.Comparator;
import java.util.Objects;

/**
 * A class that encapsulates the {@link ShardId} and the cluster alias
 * of a shard used during the search action.
 */
public record SearchShard(@Nullable String clusterAlias, ShardId shardId) implements Comparable<SearchShard> {

    @Override
    public int compareTo(SearchShard o) {
        int cmp = Objects.compare(clusterAlias, o.clusterAlias, Comparator.nullsFirst(Comparator.naturalOrder()));
        return cmp != 0 ? cmp : shardId.compareTo(o.shardId);
    }
}
