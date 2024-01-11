/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Context of each shard we're operating against.
 */
public interface ShardContext {
    /**
     * The index of this shard in the list of shards being processed.
     */
    int index();

    /**
     * Get {@link IndexSearcher} holding the actual data.
     */
    IndexSearcher searcher();

    /**
     * Build a "sort" configuration from an Elasticsearch style builder.
     */
    Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) throws IOException;

    /**
     * The index <strong>name</strong> of the index.
     */
    Index fullyQualifiedIndex();

    /**
     * The number of the shard being processed. These are just counting numbers
     * starting with 0.
     */
    int shardId();
}
