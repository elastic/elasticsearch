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
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.nik9000.mapmatcher.ListMatcher.matchesList;
import static io.github.nik9000.mapmatcher.MapMatcher.assertMap;
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
        DocumentParser documentParser = new DocumentParser(null, null, null, null);
        DocumentMapper mergedMapper = new DocumentMapper(documentParser, merged);
        assertThat(mergedMapper.mappers().getMapper("age"), notNullValue());
        assertThat(mergedMapper.mappers().getMapper("obj1.prop1"), notNullValue());
    }

    public void testMergeObjectDynamic() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> { }));
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
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> MapperService.mergeMappings(objectMapper, nestedMapper.mapping(), reason));
            assertThat(e.getMessage(), containsString("can't merge a nested mapping [obj] with a non-nested mapping"));
        }
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> MapperService.mergeMappings(nestedMapper, objectMapper.mapping(), reason));
            assertThat(e.getMessage(), containsString("can't merge a non nested mapping [obj] with a nested mapping"));
        }
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()));
        analyzers.put("keyword", new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer()));
        analyzers.put("whitespace", new NamedAnalyzer("whitespace", AnalyzerScope.INDEX, new WhitespaceAnalyzer()));
        return new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
    }

    public void testMergeSearchAnalyzer() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.field("analyzer", "default");
            b.field("search_analyzer", "whitespace");
        }));

        assertThat(mapperService.fieldType("field").getTextSearchInfo().getSearchAnalyzer().name(),
            equalTo("whitespace"));

        merge(mapperService, fieldMapping(b -> {
            b.field("type", "text");
            b.field("analyzer", "default");
            b.field("search_analyzer", "keyword");
        }));
        assertThat(mapperService.fieldType("field").getTextSearchInfo().getSearchAnalyzer().name(),
            equalTo("keyword"));
    }

    public void testChangeSearchAnalyzerToDefault() throws Exception {

        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.field("analyzer", "default");
            b.field("search_analyzer", "whitespace");
        }));

        assertThat(mapperService.fieldType("field").getTextSearchInfo().getSearchAnalyzer().name(),
            equalTo("whitespace"));

        merge(mapperService, fieldMapping(b -> {
            b.field("type", "text");
            b.field("analyzer", "default");
        }));

        assertThat(mapperService.fieldType("field").getTextSearchInfo().getSearchAnalyzer().name(),
            equalTo("default"));
    }

    public void testConcurrentMergeTest() throws Throwable {

        final MapperService mapperService = createMapperService(mapping(b -> {}));
        final DocumentMapper documentMapper = mapperService.documentMapper();

        expectThrows(IllegalArgumentException.class,
            () -> documentMapper.mappers().indexAnalyzer("non_existing_field", f -> {
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
            while(stopped.get() == false) {
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
        MapperService mapperService
            = createMapperService(topMapping(b -> b.startObject("_source").field("enabled", false).endObject()));

        merge(mapperService, fieldMapping(b -> b.field("type", "text")));

        assertNotNull(mapperService.documentMapper().mappers().getMapper("field"));
        assertFalse(mapperService.documentMapper().sourceMapper().enabled());
    }

    public void testMergeMetadataFieldsForIndexTemplates() throws IOException {
        MapperService mapperService
            = createMapperService(topMapping(b -> b.startObject("_source").field("enabled", false).endObject()));

        merge(mapperService, MergeReason.INDEX_TEMPLATE,
            topMapping(b -> b.startObject("_source").field("enabled", true).endObject()));
        DocumentMapper mapper = mapperService.documentMapper();
        assertTrue(mapper.sourceMapper().enabled());
    }

    public void testMergeMeta() throws IOException {
        DocumentMapper initMapper = createDocumentMapper(topMapping(b -> b.startObject("_meta").field("foo", "bar").endObject()));
        assertThat(initMapper.mapping().getMeta().get("foo"), equalTo("bar"));

        DocumentMapper updatedMapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text")));

        Mapping merged = MapperService.mergeMappings(initMapper, updatedMapper.mapping(), MergeReason.MAPPING_UPDATE);
        assertThat(merged.getMeta().get("foo"), equalTo("bar"));

        updatedMapper
            = createDocumentMapper(topMapping(b -> b.startObject("_meta").field("foo", "new_bar").endObject()));
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

        Map<String, Object> expected = org.elasticsearch.core.Map.of(
            "field", "value",
            "object", org.elasticsearch.core.Map.of("field1", "value1", "field2", "value2"));
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

        expected = org.elasticsearch.core.Map.of(
            "field", "value",
            "object", org.elasticsearch.core.Map.of("field1", "value1", "field2", "new_value", "field3", "value3"));
        assertThat(merged.getMeta(), equalTo(expected));
    }

    public void testEmptyDocumentMapper() {
        MapperService mapperService = createMapperService(Version.CURRENT, Settings.EMPTY, () -> false);
        String type = randomAlphaOfLengthBetween(3, 6);
        DocumentMapper documentMapper = DocumentMapper.createEmpty(type, mapperService);
        assertEquals("{\"" + type + "\":{}}", Strings.toString(documentMapper.mapping()));
        assertTrue(documentMapper.mappers().hasMappings());
        assertNotNull(documentMapper.idFieldMapper());
        assertNotNull(documentMapper.sourceMapper());
        assertNotNull(documentMapper.IndexFieldMapper());
        List<Class<?>> metadataMappers = new ArrayList<>(documentMapper.mappers().getMapping().getMetadataMappersMap().keySet());
        Collections.sort(metadataMappers, Comparator.comparing(c -> c.getSimpleName()));
        assertMap(
            metadataMappers,
            matchesList().item(DocCountFieldMapper.class)
                .item(FieldNamesFieldMapper.class)
                .item(IdFieldMapper.class)
                .item(IgnoredFieldMapper.class)
                .item(IndexFieldMapper.class)
                .item(RoutingFieldMapper.class)
                .item(SeqNoFieldMapper.class)
                .item(SourceFieldMapper.class)
                .item(TypeFieldMapper.class)
                .item(VersionFieldMapper.class)
        );
        List<String> matching = new ArrayList<>(documentMapper.mappers().getMatchingFieldNames("*"));
        Collections.sort(matching);
        assertMap(
            matching,
            matchesList().item(DocCountFieldMapper.CONTENT_TYPE)
                .item(FieldNamesFieldMapper.CONTENT_TYPE)
                .item(IdFieldMapper.CONTENT_TYPE)
                .item(IgnoredFieldMapper.CONTENT_TYPE)
                .item(IndexFieldMapper.CONTENT_TYPE)
                .item(RoutingFieldMapper.CONTENT_TYPE)
                .item(SeqNoFieldMapper.CONTENT_TYPE)
                .item(SourceFieldMapper.CONTENT_TYPE)
                .item(TypeFieldMapper.NAME)
                .item(VersionFieldMapper.CONTENT_TYPE)
        );
    }
}
