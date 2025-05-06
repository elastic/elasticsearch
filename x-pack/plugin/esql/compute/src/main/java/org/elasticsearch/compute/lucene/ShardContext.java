/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
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
     * A "name" for the shard that you can look up against other APIs like
     * {@code _cat/shards}.
     */
    String shardIdentifier();

    /**
     * Build something to load source {@code _source}.
     */
    SourceLoader newSourceLoader();

    /**
     * Returns something to load values from this field into a {@link Block}.
     */
    BlockLoader blockLoader(String name, boolean asUnsupportedSource, MappedFieldType.FieldExtractPreference fieldExtractPreference);
}
