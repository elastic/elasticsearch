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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.field.BinaryDocValuesField;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.SearchExecutionContextHelper.SHARD_SEARCH_STATS;

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

            IndexVersion indexVersion = IndexVersion.current();
            NamedWriteableRegistry writeableRegistry = writableRegistry();
            XContentParserConfiguration parserConfig = parserConfig();
            BytesBinaryIndexFieldData fieldData = new BytesBinaryIndexFieldData(
                fieldMapper.fullPath(),
                CoreValuesSourceType.KEYWORD,
                BinaryDocValuesField::new
            );
            BiFunction<MappedFieldType, FieldDataContext, IndexFieldData<?>> indexFieldDataLookup = (mft, fdc) -> fieldData;
            Settings indexSettingsSettings = indexSettings(indexVersion, 1, 1).build();
            IndexSettings indexSettings = new IndexSettings(
                IndexMetadata.builder("test").settings(indexSettingsSettings).build(),
                Settings.EMPTY
            );
            List<String> fieldNames = Arrays.stream(queryBuilders).map(TermQueryBuilder::fieldName).distinct().toList();
            List<FieldMapper> keywordMappers = fieldNames.stream()
                .map(name -> new KeywordFieldMapper.Builder(name, indexSettings).build(MapperBuilderContext.root(false, false)))
                .collect(Collectors.toList());
            MappingLookup mappingLookup = MappingLookup.fromMappers(
                Mapping.EMPTY,
                keywordMappers,
                Collections.emptyList(),
                IndexMode.STANDARD
            );
            SearchExecutionContext searchExecutionContext = new SearchExecutionContext(
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

            PercolateQuery.QueryStore queryStore = PercolateQueryBuilder.createStore(
                fieldMapper.fieldType(),
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
        }
    }
}
