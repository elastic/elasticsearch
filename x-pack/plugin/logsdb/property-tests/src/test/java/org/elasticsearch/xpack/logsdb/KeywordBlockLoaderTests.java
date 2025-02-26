/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Builders;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.BeforeContainer;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class KeywordBlockLoaderTests {
    @BeforeContainer
    static void beforeContainer() {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    @Property(tries = 10000)
    void blockLoaderReturnsCorrectResults(
        @ForAll("settings") Settings indexSettings,
        @ForAll("mapping") Map<String, Object> fieldMapping,
        @ForAll("document") Map<String, Object> document
    ) throws IOException {
        var mapping = Map.of("_doc", Map.of("properties", Map.of("field", fieldMapping)));
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping);
        var mapperService = createMapperService(indexSettings, mappingXContent);

        var documentXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(document);

        Object blockLoaderResult = setupAndInvokeBlockLoader(mapperService, documentXContent, "field");

        Object expected = expected(
            fieldMapping,
            getFieldValue(document, "field"),
            "synthetic".equals(indexSettings.get("index.mapping.source.mode"))
        );
        Assertions.assertEquals(expected, blockLoaderResult);
    }

    @Provide
    Arbitrary<Settings> settings() {
        var syntheticSource = Arbitraries.of(true, false);

        return syntheticSource.map(syn -> {
            var settings = Settings.builder();
            if (syn) {
                settings.put("index.mapping.source.mode", "synthetic");
            }

            return settings.build();
        });
    }

    @Provide
    Arbitrary<Map<String, Object>> mapping() {
        var docValues = Arbitraries.of(true, false);
        var index = Arbitraries.of(true, false);
        var store = Arbitraries.of(true, false);
        var nullValue = Arbitraries.strings().withCharRange('0', 'z').ofMinLength(0).ofMaxLength(50);
        var ignoreAbove = Arbitraries.integers().between(1, 100);

        return Builders.withBuilder(() -> new HashMap<String, Object>() {
            {
                put("type", "keyword");
            }
        }).use(docValues).in((mapping, dv) -> {
            mapping.put("doc_values", dv);
            return mapping;
        }).use(index).in((mapping, i) -> {
            mapping.put("index", i);
            return mapping;
        }).use(store).in((mapping, s) -> {
            mapping.put("store", s);
            return mapping;
        }).use(nullValue).withProbability(0.2).in((mapping, nv) -> {
            mapping.put("null_value", nv);
            return mapping;
        }).use(ignoreAbove).withProbability(0.2).in((mapping, ib) -> {
            mapping.put("ignore_above", ib);
            return mapping;
        }).build(mapping -> mapping);
    }

    @Provide
    Arbitrary<Map<String, Object>> document() {
        var keywordValues = Arbitraries.strings()
            .withCharRange('0', 'z')
            .ofMinLength(0)
            .ofMaxLength(50)
            .injectNull(0.05)
            .list()
            .ofMinSize(0)
            .ofMaxSize(10);

        return keywordValues.map(l -> Map.of("field", l));
    }

    @Provide
    Arbitrary<Map<String, Object>> deepDocument() {
        var keywordValues = Arbitraries.strings()
            .withCharRange('0', 'z')
            .ofMinLength(0)
            .ofMaxLength(50)
            .injectNull(0.05)
            .list()
            .ofMinSize(0)
            .ofMaxSize(10);

        return keywordValues.map(l -> Map.of("field", l));
    }

    private MapperService createMapperService(Settings indexSettings, XContentBuilder mappingXContent) throws IOException {
        return new TestMapperServiceBuilder().withSettings(indexSettings).withMapping(mappingXContent).build();
    }

    private Object expected(Map<String, Object> fieldMapping, Object value, boolean syntheticSource) {
        var nullValue = (String) fieldMapping.get("null_value");

        var ignoreAbove = fieldMapping.get("ignore_above") == null
            ? Integer.MAX_VALUE
            : ((Number) fieldMapping.get("ignore_above")).intValue();

        if (value == null) {
            return convert(null, nullValue, ignoreAbove);
        }

        if (value instanceof String s) {
            return convert(s, nullValue, ignoreAbove);
        }

        Function<Stream<String>, Stream<BytesRef>> convertValues = s -> s.map(v -> convert(v, nullValue, ignoreAbove))
            .filter(Objects::nonNull);

        if ((boolean) fieldMapping.getOrDefault("doc_values", false)) {
            // Sorted and no duplicates

            var resultList = convertValues.andThen(Stream::distinct)
                .andThen(Stream::sorted)
                .andThen(Stream::toList)
                .apply(((List<String>) value).stream());
            return maybeFoldList(resultList);
        }

        // store: "true" and source
        var resultList = convertValues.andThen(Stream::toList).apply(((List<String>) value).stream());
        return maybeFoldList(resultList);
    }

    private BytesRef convert(String value, String nullValue, int ignoreAbove) {
        if (value == null) {
            if (nullValue != null) {
                value = nullValue;
            } else {
                return null;
            }
        }

        return value.length() <= ignoreAbove ? new BytesRef(value) : null;
    }

    private Object getFieldValue(Map<String, Object> document, String fieldName) {
        var rawValues = new ArrayList<>();
        processLevel(document, fieldName, rawValues);

        if (rawValues.size() == 1) {
            return rawValues.get(0);
        }

        return rawValues.stream().flatMap(v -> v instanceof List<?> l ? l.stream() : Stream.of(v)).toList();
    }

    @SuppressWarnings("unchecked")
    private void processLevel(Map<String, Object> level, String field, ArrayList<Object> values) {
        if (field.contains(".") == false) {
            var value = level.get(field);
            values.add(value);
            return;
        }

        var nameInLevel = field.split("\\.")[0];
        var entry = level.get(nameInLevel);
        if (entry instanceof Map<?, ?> m) {
            processLevel((Map<String, Object>) m, field.substring(field.indexOf('.') + 1), values);
        }
        if (entry instanceof List<?> l) {
            for (var object : l) {
                processLevel((Map<String, Object>) object, field.substring(field.indexOf('.') + 1), values);
            }
        }
    }

    protected static Object maybeFoldList(List<?> list) {
        if (list.isEmpty()) {
            return null;
        }

        if (list.size() == 1) {
            return list.get(0);
        }

        return list;
    }

    private Object setupAndInvokeBlockLoader(MapperService mapperService, XContentBuilder document, String fieldName) throws IOException {
        // TODO is this correct?
        try (Directory directory = new ByteBuffersDirectory()) {
            // TODO is RandomIndexWriter necessary ??
            IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig());

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
            var block = (TestBlock) columnAtATimeReader.read(TestBlock.factory(context.reader().numDocs()), TestBlock.docs(0));
            if (block.size() == 0) {
                return null;
            }
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
        if (block.size() == 0) {
            return null;
        }
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
