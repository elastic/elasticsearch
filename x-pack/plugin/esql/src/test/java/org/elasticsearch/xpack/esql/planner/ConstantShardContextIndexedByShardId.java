/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** A rather roundabout way of ignoring shard ref counters in tests. */
public class ConstantShardContextIndexedByShardId implements IndexedByShardId<EsPhysicalOperationProviders.ShardContext> {
    public static final ConstantShardContextIndexedByShardId INSTANCE = new ConstantShardContextIndexedByShardId();

    private ConstantShardContextIndexedByShardId() {}

    private static final EsPhysicalOperationProviders.ShardContext CONTEXT = new EsPhysicalOperationProviders.ShardContext() {
        @Override
        public Query toQuery(QueryBuilder queryBuilder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexSettings indexSettings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MappingLookup mappingLookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double storedFieldsSequentialProportion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int index() {
            return 0;
        }

        @Override
        public IndexSearcher searcher() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String shardIdentifier() {
            return "ConstantRefCountedIndexedByShardId.CONTEXT";
        }

        @Override
        public SourceLoader newSourceLoader(Set<String> sourcePaths) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader blockLoader(
            String name,
            boolean asUnsupportedSource,
            MappedFieldType.FieldExtractPreference fieldExtractPreference,
            BlockLoaderFunctionConfig blockLoaderFunctionConfig
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MappedFieldType fieldType(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ShardSearchStats stats() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    };

    @Override
    public EsPhysicalOperationProviders.ShardContext get(int shardId) {
        return CONTEXT;
    }

    @Override
    public Iterable<? extends EsPhysicalOperationProviders.ShardContext> iterable() {
        return List.of(CONTEXT);
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public <S> IndexedByShardId<S> map(Function<EsPhysicalOperationProviders.ShardContext, S> mapper) {
        throw new UnsupportedOperationException();
    }
}
