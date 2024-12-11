/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.Collections;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class SearchServiceTests extends IndexShardTestCase {

    public void testCanMatchMatchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        doTestCanMatch(searchRequest, null, true, null, false);
    }

    public void testCanMatchMatchNone() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder()));
        doTestCanMatch(searchRequest, null, false, null, false);
    }

    public void testCanMatchMatchNoneWithException() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder()));
        doTestCanMatch(searchRequest, null, true, null, true);
    }

    public void testCanMatchKeywordSortedQueryMatchNone() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().sort("field").query(new MatchNoneQueryBuilder()));
        SortField sortField = new SortField("field", SortField.Type.STRING);
        doTestCanMatch(searchRequest, sortField, false, null, false);
    }

    public void testCanMatchKeywordSortedQueryMatchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().sort("field").query(new MatchAllQueryBuilder()));
        SortField sortField = new SortField("field", SortField.Type.STRING);
        MinAndMax<BytesRef> expectedMinAndMax = new MinAndMax<>(new BytesRef("value"), new BytesRef("value"));
        doTestCanMatch(searchRequest, sortField, true, expectedMinAndMax, false);
    }

    public void testCanMatchKeywordSortedQueryMatchNoneWithException() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().sort("field").query(new MatchNoneQueryBuilder()));
        // provide a sort field that throws exception
        SortField sortField = new SortField("field", SortField.Type.STRING) {
            @Override
            public Type getType() {
                throw new UnsupportedOperationException();
            }
        };
        doTestCanMatch(searchRequest, sortField, false, null, false);
    }

    public void testCanMatchKeywordSortedQueryMatchAllWithException() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().sort("field").query(new MatchAllQueryBuilder()));
        // provide a sort field that throws exception
        SortField sortField = new SortField("field", SortField.Type.STRING) {
            @Override
            public Type getType() {
                throw new UnsupportedOperationException();
            }
        };
        doTestCanMatch(searchRequest, sortField, true, null, false);
    }

    private void doTestCanMatch(
        SearchRequest searchRequest,
        SortField sortField,
        boolean expectedCanMatch,
        MinAndMax<?> expectedMinAndMax,
        boolean throwException
    ) throws IOException {
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            new ShardId("index", "index", 0),
            0,
            5,
            AliasFilter.EMPTY,
            1.0f,
            0,
            null
        );
        IndexFieldData<LeafFieldData> indexFieldData = indexFieldData(sortField);
        IndexShard indexShard = newShard(true);
        try {
            recoverShardFromStore(indexShard);
            assertTrue(indexDoc(indexShard, "_doc", "id", "{\"field\":\"value\"}").isCreated());
            assertTrue(indexShard.refresh("test").refreshed());
            try (Engine.Searcher searcher = indexShard.acquireSearcher("test")) {
                SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
                    (mappedFieldType, fieldDataContext) -> indexFieldData,
                    searcher
                );
                SearchService.CanMatchContext canMatchContext = createCanMatchContext(
                    shardRequest,
                    indexShard,
                    searchExecutionContext,
                    parserConfig(),
                    throwException
                );
                CanMatchShardResponse canMatchShardResponse = SearchService.canMatch(canMatchContext, false);
                assertEquals(expectedCanMatch, canMatchShardResponse.canMatch());
                if (expectedMinAndMax == null) {
                    assertNull(canMatchShardResponse.estimatedMinAndMax());
                } else {
                    MinAndMax<?> minAndMax = canMatchShardResponse.estimatedMinAndMax();
                    assertNotNull(minAndMax);
                    assertEquals(expectedMinAndMax.getMin(), minAndMax.getMin());
                    assertEquals(expectedMinAndMax.getMin(), minAndMax.getMax());
                }

            }
        } finally {
            closeShards(indexShard);
        }
    }

    private SearchExecutionContext createSearchExecutionContext(
        BiFunction<MappedFieldType, FieldDataContext, IndexFieldData<?>> indexFieldDataLookup,
        IndexSearcher searcher
    ) {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        Predicate<String> indexNameMatcher = pattern -> Regex.simpleMatch(pattern, "index");

        MapperBuilderContext root = MapperBuilderContext.root(false, false);
        RootObjectMapper.Builder builder = new RootObjectMapper.Builder("_doc", ObjectMapper.Defaults.SUBOBJECTS);
        Mapping mapping = new Mapping(
            builder.build(MapperBuilderContext.root(false, false)),
            new MetadataFieldMapper[0],
            Collections.emptyMap()
        );
        KeywordFieldMapper keywordFieldMapper = new KeywordFieldMapper.Builder("field", IndexVersion.current()).build(root);
        MappingLookup mappingLookup = MappingLookup.fromMappers(
            mapping,
            Collections.singletonList(keywordFieldMapper),
            Collections.emptyList()
        );
        return new SearchExecutionContext(
            0,
            0,
            indexSettings,
            null,
            indexFieldDataLookup,
            null,
            mappingLookup,
            null,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            searcher,
            System::currentTimeMillis,
            null,
            indexNameMatcher,
            () -> true,
            null,
            Collections.emptyMap(),
            MapperMetrics.NOOP
        );
    }

    private static IndexFieldData<LeafFieldData> indexFieldData(SortField sortField) {
        return new IndexFieldData<>() {
            @Override
            public String getFieldName() {
                return "field";
            }

            @Override
            public ValuesSourceType getValuesSourceType() {
                throw new UnsupportedOperationException();
            }

            @Override
            public LeafFieldData load(LeafReaderContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public LeafFieldData loadDirect(LeafReaderContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SortField sortField(
                Object missingValue,
                MultiValueMode sortMode,
                XFieldComparatorSource.Nested nested,
                boolean reverse
            ) {
                return sortField;
            }

            @Override
            public BucketedSort newBucketedSort(
                BigArrays bigArrays,
                Object missingValue,
                MultiValueMode sortMode,
                XFieldComparatorSource.Nested nested,
                SortOrder sortOrder,
                DocValueFormat format,
                int bucketSize,
                BucketedSort.ExtraData extra
            ) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static SearchService.CanMatchContext createCanMatchContext(
        ShardSearchRequest shardRequest,
        IndexShard indexShard,
        SearchExecutionContext searchExecutionContext,
        XContentParserConfiguration parserConfig,
        boolean throwException
    ) {
        return new SearchService.CanMatchContext(shardRequest, null, null, -1, -1) {
            @Override
            IndexShard getShard() {
                return indexShard;
            }

            @Override
            QueryRewriteContext getQueryRewriteContext(IndexService indexService) {
                if (throwException) {
                    throw new IllegalArgumentException();
                }
                return new QueryRewriteContext(parserConfig, null, System::currentTimeMillis);
            }

            @Override
            SearchExecutionContext getSearchExecutionContext(Engine.Searcher searcher) {
                return searchExecutionContext;
            }

            @Override
            IndexService getIndexService() {
                // it's ok to return null because the three above methods are overridden
                return null;
            }
        };
    }
}
