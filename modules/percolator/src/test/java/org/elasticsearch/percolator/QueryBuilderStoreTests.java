/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.percolator;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.TestDocumentParserContext;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.script.field.BinaryDocValuesField;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.SearchExecutionContextHelper.SHARD_SEARCH_STATS;
import static org.hamcrest.Matchers.equalTo;

public class QueryBuilderStoreTests extends ESTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testStoringQueryBuilders() throws IOException {
        TermQueryBuilder[] queryBuilders = new TermQueryBuilder[randomIntBetween(1, 16)];
        for (int i = 0; i < queryBuilders.length; i++) {
            queryBuilders[i] = new TermQueryBuilder(randomAlphaOfLength(4), randomAlphaOfLength(8));
        }
        List<String> fieldNames = Arrays.stream(queryBuilders).map(TermQueryBuilder::fieldName).distinct().toList();

        try (Directory directory = newDirectory()) {
            PercolatorTestSetup setup = setupPercolatorTest(directory, fieldNames, queryBuilders);
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
            SearchExecutionContext searchExecutionContext = new SearchExecutionContext(setup.baseContext(), breaker);

            try {
                PercolateQuery.QueryStore queryStore = PercolateQueryBuilder.createStore(
                    setup.fieldMapper().fieldType(),
                    randomBoolean(),
                    searchExecutionContext
                );

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    LeafReaderContext leafContext = indexReader.leaves().get(0);
                    CheckedFunction<Integer, Query, IOException> queries = queryStore.getQueries(leafContext);
                    assertEquals(queryBuilders.length, leafContext.reader().numDocs());
                    for (int i = 0; i < queryBuilders.length; i++) {
                        TermQuery query = (TermQuery) queries.apply(i);
                        assertEquals(queryBuilders[i].fieldName(), query.getTerm().field());
                        assertEquals(queryBuilders[i].value(), query.getTerm().text());
                    }
                }
            } finally {
                searchExecutionContext.releaseQueryConstructionMemory();
            }
        }
    }

    public void testPerDocumentQueryConstructionKeepsBreakerAtBaseline() throws IOException {
        String fieldName = "keyword_field";
        QueryBuilder[] queryBuilders = new QueryBuilder[] {
            new WildcardQueryBuilder(fieldName, "test*pattern*with*wildcards"),
            new RegexpQueryBuilder(fieldName, ".*test.*regexp.*pattern.*"),
            new WildcardQueryBuilder(fieldName, "another*wildcard*query"),
            new RegexpQueryBuilder(fieldName, "prefix[0-9]+suffix"), };

        try (Directory directory = newDirectory()) {
            PercolatorTestSetup setup = setupPercolatorTest(directory, fieldName, queryBuilders);
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
            SearchExecutionContext searchExecutionContext = new SearchExecutionContext(setup.baseContext(), breaker);

            long baselineUsed = breaker.getUsed();
            try {
                PercolateQuery.QueryStore queryStore = PercolateQueryBuilder.createStore(
                    setup.fieldMapper().fieldType(),
                    false,
                    searchExecutionContext
                );

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    LeafReaderContext leafContext = indexReader.leaves().get(0);
                    CheckedFunction<Integer, Query, IOException> queries = queryStore.getQueries(leafContext);
                    assertEquals(queryBuilders.length, leafContext.reader().numDocs());

                    for (int i = 0; i < queryBuilders.length; i++) {
                        queries.apply(i);
                        assertThat(
                            "Per-doc query construction must release breaker bytes after iteration " + i,
                            breaker.getUsed(),
                            equalTo(baselineUsed)
                        );
                    }
                }
            } finally {
                searchExecutionContext.releaseQueryConstructionMemory();
            }
        }
    }

    public void testConcurrentPerDocumentQueryConstructionKeepsBreakerAtBaseline() throws IOException {
        String fieldName = "keyword_field";
        int numStoredQueries = 16;
        int clausesPerQuery = 32;

        QueryBuilder[] queryBuilders = new QueryBuilder[numStoredQueries];
        for (int i = 0; i < numStoredQueries; i++) {
            BoolQueryBuilder bool = new BoolQueryBuilder();
            for (int c = 0; c < clausesPerQuery; c++) {
                bool.should(new TermQueryBuilder(fieldName, "term_" + i + "_" + c));
            }
            queryBuilders[i] = bool;
        }

        try (Directory directory = newDirectory()) {
            PercolatorTestSetup setup = setupPercolatorTest(directory, fieldName, queryBuilders);
            CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
            SearchExecutionContext searchExecutionContext = new SearchExecutionContext(setup.baseContext(), circuitBreaker);

            PercolateQuery.QueryStore queryStore = PercolateQueryBuilder.createStore(
                setup.fieldMapper().fieldType(),
                false,
                searchExecutionContext
            );

            int numThreads = randomIntBetween(5, 30);
            int passesPerThread = randomIntBetween(10, 100);
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                LeafReaderContext leafContext = indexReader.leaves().get(0);

                long baselineUsed = circuitBreaker.getUsed();
                try {
                    startInParallel(numThreads, t -> {
                        try {
                            for (int pass = 0; pass < passesPerThread; pass++) {
                                CheckedFunction<Integer, Query, IOException> queries = queryStore.getQueries(leafContext);
                                for (int docId = 0; docId < queryBuilders.length; docId++) {
                                    Query q = queries.apply(docId);
                                    assertNotNull("apply(" + docId + ") must produce a query under concurrent load", q);
                                }
                            }
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    });

                    assertEquals(
                        "Concurrent per-iteration release must net to zero on the request breaker",
                        baselineUsed,
                        circuitBreaker.getUsed()
                    );
                } finally {
                    searchExecutionContext.releaseQueryConstructionMemory();
                }
            }
        }
    }

    private record PercolatorTestSetup(BinaryFieldMapper fieldMapper, SearchExecutionContext baseContext) {}

    private PercolatorTestSetup setupPercolatorTest(Directory directory, String keywordFieldName, QueryBuilder[] queryBuilders)
        throws IOException {
        return setupPercolatorTest(directory, List.of(keywordFieldName), queryBuilders);
    }

    private PercolatorTestSetup setupPercolatorTest(Directory directory, Collection<String> keywordFieldNames, QueryBuilder[] queryBuilders)
        throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        BinaryFieldMapper fieldMapper = PercolatorFieldMapper.Builder.createQueryBuilderFieldBuilder(
            MapperBuilderContext.root(false, false)
        );

        IndexVersion indexVersion = IndexVersion.current();
        try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
            for (QueryBuilder queryBuilder : queryBuilders) {
                DocumentParserContext documentParserContext = new TestDocumentParserContext();
                PercolatorFieldMapper.createQueryBuilderField(
                    indexVersion,
                    TransportVersion.current(),
                    fieldMapper,
                    queryBuilder,
                    documentParserContext
                );
                indexWriter.addDocument(documentParserContext.doc());
            }
        }

        NamedWriteableRegistry writeableRegistry = writableRegistry();
        XContentParserConfiguration parserConfig = parserConfig();
        Settings indexSettingsSettings = indexSettings(indexVersion, 1, 1).build();
        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test").settings(indexSettingsSettings).build(),
            Settings.EMPTY
        );

        List<FieldMapper> keywordMappers = keywordFieldNames.stream()
            .map(name -> new KeywordFieldMapper.Builder(name, indexSettings).build(MapperBuilderContext.root(false, false)))
            .collect(Collectors.toList());
        MappingLookup mappingLookup = MappingLookup.fromMappers(Mapping.EMPTY, keywordMappers, Collections.emptyList(), IndexMode.STANDARD);

        BytesBinaryIndexFieldData fieldData = new BytesBinaryIndexFieldData(
            fieldMapper.fullPath(),
            CoreValuesSourceType.KEYWORD,
            BinaryDocValuesField::new,
            indexVersion
        );
        BiFunction<MappedFieldType, FieldDataContext, IndexFieldData<?>> indexFieldDataLookup = (mft, fdc) -> fieldData;

        SearchExecutionContext baseContext = new SearchExecutionContext(
            0,
            0,
            indexSettings,
            null,
            indexFieldDataLookup,
            null,
            mappingLookup,
            null,
            null,
            parserConfig,
            writeableRegistry,
            null,
            null,
            System::currentTimeMillis,
            null,
            null,
            () -> true,
            null,
            Collections.emptyMap(),
            null,
            MapperMetrics.NOOP,
            SHARD_SEARCH_STATS
        );

        return new PercolatorTestSetup(fieldMapper, baseContext);
    }
}
