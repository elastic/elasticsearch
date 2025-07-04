/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.tests.util.LuceneTestCase.newDirectory;
import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BlockLoaderTestRunner {
    private final BlockLoaderTestCase.Params params;

    public BlockLoaderTestRunner(BlockLoaderTestCase.Params params) {
        this.params = params;
    }

    public void runTest(MapperService mapperService, Map<String, Object> document, Object expected, String blockLoaderFieldName)
        throws IOException {
        var documentXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(document);

        Object blockLoaderResult = setupAndInvokeBlockLoader(mapperService, documentXContent, blockLoaderFieldName);
        Assert.assertEquals(expected, blockLoaderResult);
    }

    private Object setupAndInvokeBlockLoader(MapperService mapperService, XContentBuilder document, String fieldName) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);

            var source = new SourceToParse(
                "1",
                BytesReference.bytes(document),
                XContentType.JSON,
                null,
                Map.of(),
                true,
                XContentMeteringParserDecorator.NOOP
            );
            LuceneDocument doc = mapperService.documentMapper().parse(source).rootDoc();

            iw.addDocument(doc);
            iw.close();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext context = reader.leaves().get(0);
                return load(createBlockLoader(mapperService, fieldName), context, mapperService);
            }
        }
    }

    private Object load(BlockLoader blockLoader, LeafReaderContext context, MapperService mapperService) throws IOException {
        // `columnAtATimeReader` is tried first, we mimic `ValuesSourceReaderOperator`
        var columnAtATimeReader = blockLoader.columnAtATimeReader(context);
        if (columnAtATimeReader != null) {
            BlockLoader.Docs docs = TestBlock.docs(0);
            var block = (TestBlock) columnAtATimeReader.read(TestBlock.factory(context.reader().numDocs()), docs);
            assertThat(block.size(), equalTo(1));
            return block.get(0);
        }

        StoredFieldsSpec storedFieldsSpec = blockLoader.rowStrideStoredFieldSpec();
        SourceLoader.Leaf leafSourceLoader = null;
        if (storedFieldsSpec.requiresSource()) {
            var sourceLoader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP);
            leafSourceLoader = sourceLoader.leaf(context.reader(), null);
            storedFieldsSpec = storedFieldsSpec.merge(
                new StoredFieldsSpec(true, storedFieldsSpec.requiresMetadata(), sourceLoader.requiredStoredFields())
            );
        }
        BlockLoaderStoredFieldsFromLeafLoader storedFieldsLoader = new BlockLoaderStoredFieldsFromLeafLoader(
            StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(context, null),
            leafSourceLoader
        );
        storedFieldsLoader.advanceTo(0);

        BlockLoader.Builder builder = blockLoader.builder(TestBlock.factory(context.reader().numDocs()), 1);
        blockLoader.rowStrideReader(context).read(0, storedFieldsLoader, builder);
        var block = (TestBlock) builder.build();
        assertThat(block.size(), equalTo(1));

        return block.get(0);
    }

    private BlockLoader createBlockLoader(MapperService mapperService, String fieldName) {
        SearchLookup searchLookup = new SearchLookup(mapperService.mappingLookup().fieldTypesLookup()::get, null, null);

        return mapperService.fieldType(fieldName).blockLoader(new MappedFieldType.BlockLoaderContext() {
            @Override
            public String indexName() {
                return mapperService.getIndexSettings().getIndex().getName();
            }

            @Override
            public IndexSettings indexSettings() {
                return mapperService.getIndexSettings();
            }

            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return params.preference();
            }

            @Override
            public SearchLookup lookup() {
                return searchLookup;
            }

            @Override
            public Set<String> sourcePaths(String name) {
                return mapperService.mappingLookup().sourcePaths(name);
            }

            @Override
            public String parentField(String field) {
                return mapperService.mappingLookup().parentField(field);
            }

            @Override
            public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                return (FieldNamesFieldMapper.FieldNamesFieldType) mapperService.fieldType(FieldNamesFieldMapper.NAME);
            }
        });
    }
}
