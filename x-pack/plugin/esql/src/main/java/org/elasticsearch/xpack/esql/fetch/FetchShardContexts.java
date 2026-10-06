/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.lucene.IndexedByShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The shards of one fetch request by their position in the request. The slot of a shard the node couldn't open stays
 * empty, and no page of the request names it.
 */
final class FetchShardContexts<T> implements IndexedByShardId<T> {
    private final List<T> byPosition;
    private final List<T> open;

    /**
     * @param byPosition the context of each shard of the request, {@code null} for a shard that failed to open
     */
    FetchShardContexts(List<T> byPosition) {
        this.byPosition = Collections.unmodifiableList(new ArrayList<>(byPosition));
        this.open = byPosition.stream().filter(Objects::nonNull).toList();
    }

    @Override
    public T get(int shardId) {
        T context = byPosition.get(shardId);
        if (context == null) {
            throw new IllegalStateException("shard [" + shardId + "] of the fetch request isn't open");
        }
        return context;
    }

    @Override
    public Iterable<? extends T> iterable() {
        return open;
    }

    @Override
    public int size() {
        return open.size();
    }

    @Override
    public <S> IndexedByShardId<S> map(Function<T, S> mapper) {
        List<S> mapped = new ArrayList<>(byPosition.size());
        for (T context : byPosition) {
            mapped.add(context == null ? null : mapper.apply(context));
        }
        return new FetchShardContexts<>(mapped);
    }
}
