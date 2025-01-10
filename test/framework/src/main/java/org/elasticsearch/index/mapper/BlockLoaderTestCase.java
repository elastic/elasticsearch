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
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.DocumentGenerator;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.logsdb.datageneration.MappingGenerator;
import org.elasticsearch.logsdb.datageneration.Template;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BlockLoaderTestCase extends MapperServiceTestCase {
    private final FieldType fieldType;
    private final String fieldName;
    private final MappingGenerator mappingGenerator;
    private final DocumentGenerator documentGenerator;

    protected BlockLoaderTestCase(FieldType fieldType) {
        this.fieldType = fieldType;
        this.fieldName = randomAlphaOfLengthBetween(5, 10);

        // Disable all dynamic mapping
        var specification = DataGeneratorSpecification.builder()
            .withFullyDynamicMapping(false)
            .withDataSourceHandlers(List.of(new DataSourceHandler() {
                @Override
                public DataSourceResponse.DynamicMappingGenerator handle(DataSourceRequest.DynamicMappingGenerator request) {
                    return new DataSourceResponse.DynamicMappingGenerator(isObject -> false);
                }
            }))
            .build();

        this.mappingGenerator = new MappingGenerator(specification);
        this.documentGenerator = new DocumentGenerator(specification);
    }

    public void testBlockLoader() throws IOException {
        var template = new Template(Map.of(fieldName, new Template.Leaf(fieldName, fieldType)));
        runTest(template, fieldName);
    }

    public void testBlockLoaderForFieldInObject() throws IOException {
        var template = new Template(
            Map.of("object", new Template.Object("object", false, Map.of(fieldName, new Template.Leaf(fieldName, fieldType))))
        );
        runTest(template, "object." + fieldName);
    }

    private void runTest(Template template, String fieldName) throws IOException {
        var mapping = mappingGenerator.generate(template);
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());

        var syntheticSource = randomBoolean();
        var mapperService = syntheticSource ? createSytheticSourceMapperService(mappingXContent) : createMapperService(mappingXContent);

        var document = documentGenerator.generate(template, mapping);
        var documentXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(document);

        Object blockLoaderResult = setupAndInvokeBlockLoader(mapperService, documentXContent, fieldName);
        Object expected = expected(mapping.lookup().get(fieldName), getFieldValue(document, fieldName), syntheticSource);
        assertEquals(expected, blockLoaderResult);
    }

    protected abstract Object expected(Map<String, Object> fieldMapping, Object value, boolean syntheticSource);

    private Object getFieldValue(Map<String, Object> document, String fieldName) {
        if (fieldName.contains(".") == false) {
            return document.get(fieldName);
        }

        var parent = document.get(fieldName.split("\\.")[0]);
        var childName = fieldName.split("\\.")[1];
        if (parent instanceof Map<?, ?> m) {
            return m.get(childName);
        }
        if (parent instanceof List<?> l) {
            var results = new ArrayList<>();

            for (var object : l) {
                var childValue = ((Map<?, ?>) object).get(childName);
                if (childValue instanceof List<?> cl) {
                    results.addAll(cl);
                } else {
                    results.add(childValue);
                }
            }

            return results;
        }

        throw new IllegalStateException("Unexpected structure of document");
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
            var block = (TestBlock) columnAtATimeReader.read(TestBlock.factory(context.reader().numDocs()), TestBlock.docs(0));
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
                // TODO randomize when adding support for fields that care about this
                return MappedFieldType.FieldExtractPreference.NONE;
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
