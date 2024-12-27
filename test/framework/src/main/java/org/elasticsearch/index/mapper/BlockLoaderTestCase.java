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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.logsdb.datageneration.MappingGenerator;
import org.elasticsearch.logsdb.datageneration.Template;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BlockLoaderTestCase extends MapperServiceTestCase {
    private final String fieldName;
    private final Template template;
    private final MappingGenerator mappingGenerator;
    private final FieldDataGenerator generator;

    protected BlockLoaderTestCase(FieldType fieldType) {
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

        this.template = new Template(Map.of(fieldName, new Template.Leaf(fieldName, fieldType)));
        this.mappingGenerator = new MappingGenerator(specification);
        this.generator = fieldType.generator(fieldName, specification.dataSource());
    }

    public void testBlockLoader() throws IOException {
        var mapping = mappingGenerator.generate(template);
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());

        var syntheticSource = randomBoolean();
        var mapperService = syntheticSource ? createSytheticSourceMapperService(mappingXContent) : createMapperService(mappingXContent);

        var fieldValue = generator.generateValue();

        Object blockLoaderResult = setupAndInvokeBlockLoader(mapperService, fieldValue);
        Object expected = expected(mapping.lookup().get(fieldName), fieldValue, syntheticSource);
        assertEquals(expected, blockLoaderResult);
    }

    protected abstract Object expected(Map<String, Object> fieldMapping, Object value, boolean syntheticSource);

    private Object setupAndInvokeBlockLoader(MapperService mapperService, Object fieldValue) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);

            LuceneDocument doc = mapperService.documentMapper().parse(source(b -> {
                b.field(fieldName);
                b.value(fieldValue);
            })).rootDoc();

            iw.addDocument(doc);
            iw.close();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext context = reader.leaves().get(0);
                return load(createBlockLoader(mapperService), context, mapperService);
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

    private BlockLoader createBlockLoader(MapperService mapperService) {
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
