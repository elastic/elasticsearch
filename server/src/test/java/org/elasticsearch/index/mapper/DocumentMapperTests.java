/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DocumentMapperTests extends MapperServiceTestCase {

    public void testAddFields() throws Exception {
        DocumentMapper stage1 = createDocumentMapper(mapping(b -> b.startObject("name").field("type", "text").endObject()));
        DocumentMapper stage2 = createDocumentMapper(mapping(b -> {
            b.startObject("name").field("type", "text").endObject();
            b.startObject("age").field("type", "integer").endObject();
            b.startObject("obj1");
            {
                b.startObject("properties");
                {
                    b.startObject("prop1").field("type", "integer").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        Mapping merged = MapperService.mergeMappings(stage1, stage2.mapping(), reason);
        // stage1 mapping should not have been modified
        assertThat(stage1.mappers().getMapper("age"), nullValue());
        assertThat(stage1.mappers().getMapper("obj1.prop1"), nullValue());
        // but merged should
        DocumentParser documentParser = new DocumentParser(null, null);
        DocumentMapper mergedMapper = new DocumentMapper(documentParser, merged, merged.toCompressedXContent());
        assertThat(mergedMapper.mappers().getMapper("age"), notNullValue());
        assertThat(mergedMapper.mappers().getMapper("obj1.prop1"), notNullValue());
    }

    public void testMergeObjectDynamic() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        assertNull(mapper.mapping().getRoot().dynamic());

        DocumentMapper withDynamicMapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        assertThat(withDynamicMapper.mapping().getRoot().dynamic(), equalTo(ObjectMapper.Dynamic.FALSE));

        Mapping merged = MapperService.mergeMappings(mapper, withDynamicMapper.mapping(), MergeReason.MAPPING_UPDATE);
        assertThat(merged.getRoot().dynamic(), equalTo(ObjectMapper.Dynamic.FALSE));
    }

    public void testMergeObjectAndNested() throws Exception {
        DocumentMapper objectMapper = createDocumentMapper(mapping(b -> b.startObject("obj").field("type", "object").endObject()));
        DocumentMapper nestedMapper = createDocumentMapper(mapping(b -> b.startObject("obj").field("type", "nested").endObject()));
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);

        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> MapperService.mergeMappings(objectMapper, nestedMapper.mapping(), reason)
            );
            assertThat(e.getMessage(), containsString("can't merge a nested mapping [obj] with a non-nested mapping"));
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> MapperService.mergeMappings(nestedMapper, objectMapper.mapping(), reason)
            );
            assertThat(e.getMessage(), containsString("can't merge a non nested mapping [obj] with a nested mapping"));
        }
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()),
                "keyword",
                new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer()),
                "whitespace",
                new NamedAnalyzer("whitespace", AnalyzerScope.INDEX, new WhitespaceAnalyzer())
            )
        );
    }

    public void testMergeSearchAnalyzer() throws Exception {

        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.field("analyzer", "default");
            b.field("search_analyzer", "whitespace");
        }));

        assertThat(mapperService.fieldType("field").getTextSearchInfo().searchAnalyzer().name(), equalTo("whitespace"));

        merge(mapperService, fieldMapping(b -> {
            b.field("type", "text");
            b.field("analyzer", "default");
            b.field("search_analyzer", "keyword");
        }));
        assertThat(mapperService.fieldType("field").getTextSearchInfo().searchAnalyzer().name(), equalTo("keyword"));
    }

    public void testChangeSearchAnalyzerToDefault() throws Exception {

        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.field("analyzer", "default");
            b.field("search_analyzer", "whitespace");
        }));

        assertThat(mapperService.fieldType("field").getTextSearchInfo().searchAnalyzer().name(), equalTo("whitespace"));

        merge(mapperService, fieldMapping(b -> {
            b.field("type", "text");
            b.field("analyzer", "default");
        }));

        assertThat(mapperService.fieldType("field").getTextSearchInfo().searchAnalyzer().name(), equalTo("default"));
    }

    public void testConcurrentMergeTest() throws Throwable {

        final MapperService mapperService = createMapperService(mapping(b -> {}));
        final DocumentMapper documentMapper = mapperService.documentMapper();

        expectThrows(IllegalArgumentException.class, () -> documentMapper.mappers().indexAnalyzer("non_existing_field", f -> {
            throw new IllegalArgumentException();
        }));

        final AtomicBoolean stopped = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicReference<String> lastIntroducedFieldName = new AtomicReference<>();
        final AtomicReference<Exception> error = new AtomicReference<>();
        final Thread updater = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 200 && stopped.get() == false; i++) {
                    final String fieldName = Integer.toString(i);
                    ParsedDocument doc = documentMapper.parse(source(b -> b.field(fieldName, "test")));
                    Mapping update = doc.dynamicMappingsUpdate();
                    assert update != null;
                    lastIntroducedFieldName.set(fieldName);
                    mapperService.merge("_doc", new CompressedXContent(update.toString()), MergeReason.MAPPING_UPDATE);
                }
            } catch (Exception e) {
                error.set(e);
            } finally {
                stopped.set(true);
            }
        });
        updater.start();
        try {
            barrier.await();
            while (stopped.get() == false) {
                final String fieldName = lastIntroducedFieldName.get();
                if (fieldName == null) {
                    continue;
                }
                ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(b -> b.field(fieldName, "test")));
                if (parsedDoc.dynamicMappingsUpdate() != null) {
                    // not in the mapping yet, try again
                    continue;
                }
                Analyzer a = mapperService.indexAnalyzer(fieldName, f -> null);
                assertNotNull(a);
                assertNotNull(a.tokenStream(fieldName, "foo"));
            }
        } finally {
            stopped.set(true);
            updater.join();
        }
        if (error.get() != null) {
            throw error.get();
        }
    }

    public void testDoNotRepeatOriginalMapping() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.startObject("_source").field("enabled", false).endObject()));

        merge(mapperService, fieldMapping(b -> b.field("type", "text")));

        assertNotNull(mapperService.documentMapper().mappers().getMapper("field"));
        assertFalse(mapperService.documentMapper().sourceMapper().enabled());
    }

    public void testMergeMetadataFieldsForIndexTemplates() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.startObject("_source").field("enabled", false).endObject()));

        merge(mapperService, MergeReason.INDEX_TEMPLATE, topMapping(b -> b.startObject("_source").field("enabled", true).endObject()));
        DocumentMapper mapper = mapperService.documentMapper();
        assertTrue(mapper.sourceMapper().enabled());
    }

    public void testMergeMeta() throws IOException {
        DocumentMapper initMapper = createDocumentMapper(topMapping(b -> b.startObject("_meta").field("foo", "bar").endObject()));
        assertThat(initMapper.mapping().getMeta().get("foo"), equalTo("bar"));

        DocumentMapper updatedMapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text")));

        Mapping merged = MapperService.mergeMappings(initMapper, updatedMapper.mapping(), MergeReason.MAPPING_UPDATE);
        assertThat(merged.getMeta().get("foo"), equalTo("bar"));

        updatedMapper = createDocumentMapper(topMapping(b -> b.startObject("_meta").field("foo", "new_bar").endObject()));
        merged = MapperService.mergeMappings(initMapper, updatedMapper.mapping(), MergeReason.MAPPING_UPDATE);
        assertThat(merged.getMeta().get("foo"), equalTo("new_bar"));
    }

    public void testMergeMetaForIndexTemplate() throws IOException {
        DocumentMapper initMapper = createDocumentMapper(topMapping(b -> {
            b.startObject("_meta");
            {
                b.field("field", "value");
                b.startObject("object");
                {
                    b.field("field1", "value1");
                    b.field("field2", "value2");
                }
                b.endObject();
            }
            b.endObject();
        }));

        Map<String, Object> expected = Map.of("field", "value", "object", Map.of("field1", "value1", "field2", "value2"));
        assertThat(initMapper.mapping().getMeta(), equalTo(expected));

        DocumentMapper updatedMapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text")));
        Mapping merged = MapperService.mergeMappings(initMapper, updatedMapper.mapping(), MergeReason.INDEX_TEMPLATE);
        assertThat(merged.getMeta(), equalTo(expected));

        updatedMapper = createDocumentMapper(topMapping(b -> {
            b.startObject("_meta");
            {
                b.field("field", "value");
                b.startObject("object");
                {
                    b.field("field2", "new_value");
                    b.field("field3", "value3");
                }
                b.endObject();
            }
            b.endObject();
        }));
        merged = merged.merge(updatedMapper.mapping(), MergeReason.INDEX_TEMPLATE);

        expected = Map.of("field", "value", "object", Map.of("field1", "value1", "field2", "new_value", "field3", "value3"));
        assertThat(merged.getMeta(), equalTo(expected));
    }

    public void testEmptyDocumentMapper() {
        MapperService mapperService = createMapperService(IndexVersion.current(), Settings.EMPTY, () -> false);
        DocumentMapper documentMapper = DocumentMapper.createEmpty(mapperService);
        assertEquals("{\"_doc\":{}}", Strings.toString(documentMapper.mapping()));
        assertTrue(documentMapper.mappers().hasMappings());
        assertNotNull(documentMapper.mappers().getMapper(IdFieldMapper.NAME));
        assertNotNull(documentMapper.sourceMapper());
        assertNotNull(documentMapper.IndexFieldMapper());
        List<Class<?>> metadataMappers = new ArrayList<>(documentMapper.mappers().getMapping().getMetadataMappersMap().keySet());
        Collections.sort(metadataMappers, Comparator.comparing(c -> c.getSimpleName()));
        assertMap(
            metadataMappers,
            matchesList().item(DataStreamTimestampFieldMapper.class)
                .item(DocCountFieldMapper.class)
                .item(FieldNamesFieldMapper.class)
                .item(IgnoredFieldMapper.class)
                .item(IndexFieldMapper.class)
                .item(NestedPathFieldMapper.class)
                .item(ProvidedIdFieldMapper.class)
                .item(RoutingFieldMapper.class)
                .item(SeqNoFieldMapper.class)
                .item(SourceFieldMapper.class)
                .item(VersionFieldMapper.class)
        );
        List<String> matching = new ArrayList<>(documentMapper.mappers().getMatchingFieldNames("*"));
        Collections.sort(matching);
        assertMap(
            matching,
            matchesList().item(DataStreamTimestampFieldMapper.NAME)
                .item(DocCountFieldMapper.CONTENT_TYPE)
                .item(FieldNamesFieldMapper.CONTENT_TYPE)
                .item(IdFieldMapper.CONTENT_TYPE)
                .item(IgnoredFieldMapper.CONTENT_TYPE)
                .item(IndexFieldMapper.CONTENT_TYPE)
                .item(NestedPathFieldMapper.NAME)
                .item(RoutingFieldMapper.CONTENT_TYPE)
                .item(SeqNoFieldMapper.CONTENT_TYPE)
                .item(SourceFieldMapper.CONTENT_TYPE)
                .item(VersionFieldMapper.CONTENT_TYPE)
        );
    }

    public void testTooManyDimensionFields() {
        int max;
        Settings settings;
        if (randomBoolean()) {
            max = 21; // By default no more than 21 dimensions per document are supported
            settings = getIndexSettings();
        } else {
            max = between(1, 10000);
            settings = Settings.builder()
                .put(getIndexSettings())
                .put(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING.getKey(), max)
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), max + 1)
                .build();
        }
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createMapperService(settings, mapping(b -> {
            for (int i = 0; i <= max; i++) {
                b.startObject("field" + i)
                    .field("type", randomFrom("ip", "keyword", "long", "integer", "byte", "short"))
                    .field("time_series_dimension", true)
                    .endObject();
            }
        })));
        assertThat(e.getMessage(), containsString("Limit of total dimension fields [" + max + "] has been exceeded"));
    }

    public void testDeeplyNestedMapping() throws Exception {
        final int maxDepth = INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(Settings.EMPTY).intValue();
        {
            // test that the depth limit is enforced for object field
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
            for (int i = 0; i < maxDepth + 5; i++) {
                builder.startObject("obj" + i);
                builder.startObject("properties");
            }
            builder.startObject("foo").field("type", "keyword").endObject();
            for (int i = 0; i < maxDepth + 5; i++) {
                builder.endObject();
                builder.endObject();
            }
            builder.endObject().endObject().endObject();

            MapperParsingException exc = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(Settings.builder().put(getIndexSettings()).build(), builder)
            );
            assertThat(exc.getMessage(), containsString("Limit of mapping depth [" + maxDepth + "] has been exceeded"));
        }

        {
            // test that the limit is per individual field, so several object fields don't trip the limit
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
            for (int i = 0; i < maxDepth - 3; i++) {
                builder.startObject("obj" + i);
                builder.startObject("properties");
            }

            for (int i = 0; i < 2; i++) {
                builder.startObject("sub_obj1" + i);
                builder.startObject("properties");
            }
            builder.startObject("foo").field("type", "keyword").endObject();
            for (int i = 0; i < 2; i++) {
                builder.endObject();
                builder.endObject();
            }

            for (int i = 0; i < 2; i++) {
                builder.startObject("sub_obj2" + i);
                builder.startObject("properties");
            }
            builder.startObject("foo2").field("type", "keyword").endObject();
            for (int i = 0; i < 2; i++) {
                builder.endObject();
                builder.endObject();
            }

            for (int i = 0; i < maxDepth - 3; i++) {
                builder.endObject();
                builder.endObject();
            }
            builder.endObject().endObject().endObject();

            createMapperService(Settings.builder().put(getIndexSettings()).build(), builder);
        }
        {
            // test that parsing correct objects in parallel using the same MapperService don't trip the limit
            final int numThreads = randomIntBetween(2, 5);
            final XContentBuilder[] builders = new XContentBuilder[numThreads];

            for (int i = 0; i < numThreads; i++) {
                builders[i] = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
                for (int j = 0; j < maxDepth - 1; j++) {
                    builders[i].startObject("obj" + i + "_" + j);
                    builders[i].startObject("properties");
                }
                builders[i].startObject("foo").field("type", "keyword").endObject();
                for (int j = 0; j < maxDepth - 1; j++) {
                    builders[i].endObject();
                    builders[i].endObject();
                }
                builders[i].endObject().endObject().endObject();
            }

            final MapperService mapperService = createMapperService(IndexVersion.current(), Settings.EMPTY, () -> false);
            final CountDownLatch latch = new CountDownLatch(1);
            final Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < threads.length; i++) {
                final int threadId = i;
                threads[threadId] = new Thread(() -> {
                    try {
                        latch.await();
                        mapperService.parseMapping("_doc", new CompressedXContent(Strings.toString(builders[threadId])));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                });
                threads[threadId].start();
            }
            latch.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
        }
    }
}
