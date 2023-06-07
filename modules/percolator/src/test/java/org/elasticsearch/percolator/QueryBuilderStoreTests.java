/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.TestDocumentParserContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

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
            BinaryFieldMapper fieldMapper = PercolatorFieldMapper.Builder.createQueryBuilderFieldBuilder(MapperBuilderContext.root(false));
            MappedFieldType.FielddataOperation fielddataOperation = MappedFieldType.FielddataOperation.SEARCH;

            try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
                for (int i = 0; i < queryBuilders.length; i++) {
                    queryBuilders[i] = new TermQueryBuilder(randomAlphaOfLength(4), randomAlphaOfLength(8));
                    DocumentParserContext documentParserContext = new TestDocumentParserContext();
                    PercolatorFieldMapper.createQueryBuilderField(
                        Version.CURRENT,
                        TransportVersion.current(),
                        fieldMapper,
                        queryBuilders[i],
                        documentParserContext
                    );
                    indexWriter.addDocument(documentParserContext.doc());
                }
            }

            SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
            when(searchExecutionContext.indexVersionCreated()).thenReturn(Version.CURRENT);
            when(searchExecutionContext.getWriteableRegistry()).thenReturn(writableRegistry());
            when(searchExecutionContext.getParserConfig()).thenReturn(parserConfig());
            when(searchExecutionContext.getForField(fieldMapper.fieldType(), fielddataOperation)).thenReturn(
                new BytesBinaryIndexFieldData(fieldMapper.name(), CoreValuesSourceType.KEYWORD)
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
}
