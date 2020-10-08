/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DocumentMapperTests extends MapperServiceTestCase {

    public void testAddFields() throws Exception {
        DocumentMapper stage1
            = createDocumentMapper(mapping(b -> b.startObject("name").field("type", "text").endObject()));

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
        DocumentMapper merged = stage1.merge(stage2.mapping(), reason);

        // stage1 mapping should not have been modified
        assertThat(stage1.mappers().getMapper("age"), nullValue());
        assertThat(stage1.mappers().getMapper("obj1.prop1"), nullValue());
        // but merged should
        assertThat(merged.mappers().getMapper("age"), notNullValue());
        assertThat(merged.mappers().getMapper("obj1.prop1"), notNullValue());
}

    public void testMergeObjectDynamic() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        assertNull(mapper.root().dynamic());

        DocumentMapper withDynamicMapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        assertThat(withDynamicMapper.root().dynamic(), equalTo(ObjectMapper.Dynamic.FALSE));

        DocumentMapper merged = mapper.merge(withDynamicMapper.mapping(), MergeReason.MAPPING_UPDATE);
        assertThat(merged.root().dynamic(), equalTo(ObjectMapper.Dynamic.FALSE));
    }

    public void testMergeObjectAndNested() throws Exception {
        DocumentMapper objectMapper
            = createDocumentMapper(mapping(b -> b.startObject("obj").field("type", "object").endObject()));
        DocumentMapper nestedMapper
            = createDocumentMapper(mapping(b -> b.startObject("obj").field("type", "nested").endObject()));
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> objectMapper.merge(nestedMapper.mapping(), reason));
            assertThat(e.getMessage(), containsString("cannot change object mapping from non-nested to nested"));
        }
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> nestedMapper.merge(objectMapper.mapping(), reason));
            assertThat(e.getMessage(), containsString("cannot change object mapping from nested to non-nested"));
        }
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return new IndexAnalyzers(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()),
                "keyword", new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer()),
                "whitespace", new NamedAnalyzer("whitespace", AnalyzerScope.INDEX, new WhitespaceAnalyzer())),
            Map.of(),
            Map.of());
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
            () -> documentMapper.mappers().indexAnalyzer().tokenStream("non_existing_field", "foo"));

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
                assertNotNull(mapperService.indexAnalyzer().tokenStream(fieldName, "foo"));
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

        DocumentMapper initMapper
            = createDocumentMapper(topMapping(b -> b.startObject("_meta").field("foo", "bar").endObject()));

        assertThat(initMapper.meta().get("foo"), equalTo("bar"));

        DocumentMapper updatedMapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text")));

        DocumentMapper mergedMapper = initMapper.merge(updatedMapper.mapping(), MergeReason.MAPPING_UPDATE);
        assertThat(mergedMapper.meta().get("foo"), equalTo("bar"));

        updatedMapper
            = createDocumentMapper(topMapping(b -> b.startObject("_meta").field("foo", "new_bar").endObject()));

        mergedMapper = initMapper.merge(updatedMapper.mapping(), MergeReason.MAPPING_UPDATE);
        assertThat(mergedMapper.meta().get("foo"), equalTo("new_bar"));
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

        Map<String, Object> expected = Map.of("field", "value",
            "object", Map.of("field1", "value1", "field2", "value2"));
        assertThat(initMapper.meta(), equalTo(expected));

        DocumentMapper updatedMapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text")));
        DocumentMapper mergedMapper = initMapper.merge(updatedMapper.mapping(), MergeReason.INDEX_TEMPLATE);
        assertThat(mergedMapper.meta(), equalTo(expected));

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
        mergedMapper = mergedMapper.merge(updatedMapper.mapping(), MergeReason.INDEX_TEMPLATE);

        expected = Map.of("field", "value",
            "object", Map.of("field1", "value1", "field2", "new_value", "field3", "value3"));
        assertThat(mergedMapper.meta(), equalTo(expected));
    }
}
