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
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.TestDocumentParserContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        try (Directory directory = newDirectory()) {
            TermQueryBuilder[] queryBuilders = new TermQueryBuilder[randomIntBetween(1, 16)];
            IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
            config.setMergePolicy(NoMergePolicy.INSTANCE);
            BinaryFieldMapper fieldMapper = PercolatorFieldMapper.Builder.createQueryBuilderFieldBuilder(
                MapperBuilderContext.root(false, false)
            );
            MappedFieldType.FielddataOperation fielddataOperation = MappedFieldType.FielddataOperation.SEARCH;

            try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
                for (int i = 0; i < queryBuilders.length; i++) {
                    queryBuilders[i] = new TermQueryBuilder(randomAlphaOfLength(4), randomAlphaOfLength(8));
                    DocumentParserContext documentParserContext = new TestDocumentParserContext();
                    PercolatorFieldMapper.createQueryBuilderField(
                        IndexVersion.current(),
                        TransportVersion.current(),
                        fieldMapper,
                        queryBuilders[i],
                        documentParserContext
                    );
                    indexWriter.addDocument(documentParserContext.doc());
                }
            }

            SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
            when(searchExecutionContext.indexVersionCreated()).thenReturn(IndexVersion.current());
            when(searchExecutionContext.getWriteableRegistry()).thenReturn(writableRegistry());
            when(searchExecutionContext.getParserConfig()).thenReturn(parserConfig());
            when(searchExecutionContext.getForField(fieldMapper.fieldType(), fielddataOperation)).thenReturn(
                new BytesBinaryIndexFieldData(fieldMapper.fullPath(), CoreValuesSourceType.KEYWORD)
            );
            when(searchExecutionContext.getFieldType(Mockito.anyString())).thenAnswer(invocation -> {
                final String fieldName = (String) invocation.getArguments()[0];
                return new KeywordFieldMapper.KeywordFieldType(fieldName);
            });
            PercolateQuery.QueryStore queryStore = PercolateQueryBuilder.createStore(fieldMapper.fieldType(), searchExecutionContext);

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
        }
    }

    public void testCircuitBreakerReleasedAfterPerDocumentQueryConstruction() throws IOException {
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofMb(100));

        String fieldName = "keyword_field";
        QueryBuilder[] queryBuilders = new QueryBuilder[] {
            new WildcardQueryBuilder(fieldName, "test*pattern*with*wildcards"),
            new RegexpQueryBuilder(fieldName, ".*test.*regexp.*pattern.*"),
            new WildcardQueryBuilder(fieldName, "another*wildcard*query"),
            new RegexpQueryBuilder(fieldName, "prefix[0-9]+suffix"), };

        try (Directory directory = newDirectory()) {
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

            KeywordFieldMapper keywordMapper = new KeywordFieldMapper.Builder(fieldName, indexSettings.getIndexVersionCreated()).build(
                MapperBuilderContext.root(false, false)
            );
            MappingLookup mappingLookup = MappingLookup.fromMappers(
                Mapping.EMPTY,
                List.of(keywordMapper),
                Collections.emptyList(),
                IndexMode.STANDARD
            );

            BytesBinaryIndexFieldData fieldData = new BytesBinaryIndexFieldData(fieldMapper.fullPath(), CoreValuesSourceType.KEYWORD);
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
                MapperMetrics.NOOP
            );
            SearchExecutionContext searchExecutionContext = new SearchExecutionContext(baseContext, circuitBreaker);

            PercolateQuery.QueryStore queryStore = PercolateQueryBuilder.createStore(fieldMapper.fieldType(), searchExecutionContext);

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                LeafReaderContext leafContext = indexReader.leaves().get(0);
                CheckedFunction<Integer, Query, IOException> queries = queryStore.getQueries(leafContext);
                assertEquals(queryBuilders.length, leafContext.reader().numDocs());

                long baselineUsed = circuitBreaker.getUsed();
                for (int i = 0; i < queryBuilders.length; i++) {
                    queries.apply(i);
                    assertThat(
                        "CB bytes should still be tracked (not leaked) after document " + i,
                        circuitBreaker.getUsed(),
                        greaterThan(baselineUsed)
                    );
                }

                searchExecutionContext.releaseQueryConstructionMemory();
                assertEquals("All CB bytes must be released after the request-end release", baselineUsed, circuitBreaker.getUsed());
            }
        }
    }
}
