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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DocumentMapperTests extends ESSingleNodeTestCase {

    public void testAddFields() throws Exception {
        String stage1Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("person").startObject("properties")
                .startObject("name").field("type", "text").endObject()
                .endObject().endObject().endObject());
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper stage1 = parser.parse("person", new CompressedXContent(stage1Mapping));
        String stage2Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("person").startObject("properties")
                .startObject("name").field("type", "text").endObject()
                .startObject("age").field("type", "integer").endObject()
                .startObject("obj1").startObject("properties").startObject("prop1").field("type", "integer").endObject().endObject()
                .endObject()
                .endObject().endObject().endObject());
        DocumentMapper stage2 = parser.parse("person", new CompressedXContent(stage2Mapping));

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
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String objectMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1").endObject().endObject());
        DocumentMapper mapper = parser.parse("type1", new CompressedXContent(objectMapping));
        assertNull(mapper.root().dynamic());

        String withDynamicMapping = Strings.toString(
                XContentFactory.jsonBuilder().startObject().startObject("type1").field("dynamic", "false").endObject().endObject());
        DocumentMapper withDynamicMapper = parser.parse("type1", new CompressedXContent(withDynamicMapping));
        assertThat(withDynamicMapper.root().dynamic(), equalTo(ObjectMapper.Dynamic.FALSE));

        DocumentMapper merged = mapper.merge(withDynamicMapper.mapping(), MergeReason.MAPPING_UPDATE);
        assertThat(merged.root().dynamic(), equalTo(ObjectMapper.Dynamic.FALSE));
    }

    public void testMergeObjectAndNested() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String objectMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("obj").field("type", "object").endObject()
                .endObject().endObject().endObject());
        DocumentMapper objectMapper = parser.parse("type1", new CompressedXContent(objectMapping));
        String nestedMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("obj").field("type", "nested").endObject()
                .endObject().endObject().endObject());
        DocumentMapper nestedMapper = parser.parse("type1", new CompressedXContent(nestedMapping));
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);

        try {
            objectMapper.merge(nestedMapper.mapping(), reason);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("cannot change object mapping from non-nested to nested"));
        }

        try {
            nestedMapper.merge(objectMapper.mapping(), reason);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("cannot change object mapping from nested to non-nested"));
        }
    }

    public void testMergeSearchAnalyzer() throws Exception {
        XContentBuilder mapping1 = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .field("search_analyzer", "whitespace")
            .endObject().endObject()
        .endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping1).mapperService();

        assertThat(mapperService.fieldType("field").getTextSearchInfo().getSearchAnalyzer().name(), equalTo("whitespace"));

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .field("search_analyzer", "keyword")
            .endObject().endObject()
        .endObject().endObject());

        mapperService.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertThat(mapperService.fieldType("field").getTextSearchInfo().getSearchAnalyzer().name(), equalTo("keyword"));
    }

    public void testChangeSearchAnalyzerToDefault() throws Exception {
          XContentBuilder mapping1 = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .field("search_analyzer", "whitespace")
            .endObject().endObject()
        .endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping1).mapperService();

        assertThat(mapperService.fieldType("field").getTextSearchInfo().getSearchAnalyzer().name(), equalTo("whitespace"));

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
            .endObject().endObject()
        .endObject().endObject());

        mapperService.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertThat(mapperService.fieldType("field").getTextSearchInfo().getSearchAnalyzer().name(), equalTo("standard"));
    }

    public void testConcurrentMergeTest() throws Throwable {
        final MapperService mapperService = createIndex("test").mapperService();
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        mapperService.merge("test", new CompressedXContent("{\"test\":{}}"), reason);
        final DocumentMapper documentMapper = mapperService.documentMapper();

        MappingLookup dfm = documentMapper.mappers();
        try {
            assertNotNull(dfm.indexAnalyzer().tokenStream("non_existing_field", "foo"));
            fail();
        } catch (IllegalArgumentException e) {
            // ok that's expected
        }

        final AtomicBoolean stopped = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicReference<String> lastIntroducedFieldName = new AtomicReference<>();
        final AtomicReference<Exception> error = new AtomicReference<>();
        final Thread updater = new Thread() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    for (int i = 0; i < 200 && stopped.get() == false; i++) {
                        final String fieldName = Integer.toString(i);
                        ParsedDocument doc = documentMapper.parse(new SourceToParse("test",
                                fieldName,
                                new BytesArray("{ \"" + fieldName + "\" : \"test\" }"),
                                XContentType.JSON));
                        Mapping update = doc.dynamicMappingsUpdate();
                        assert update != null;
                        lastIntroducedFieldName.set(fieldName);
                        mapperService.merge("test", new CompressedXContent(update.toString()), MergeReason.MAPPING_UPDATE);
                    }
                } catch (Exception e) {
                    error.set(e);
                } finally {
                    stopped.set(true);
                }
            }
        };
        updater.start();
        try {
            barrier.await();
            while(stopped.get() == false) {
                final String fieldName = lastIntroducedFieldName.get();
                final BytesReference source = new BytesArray("{ \"" + fieldName + "\" : \"test\" }");
                ParsedDocument parsedDoc = documentMapper.parse(new SourceToParse("test",
                        "random",
                        source,
                        XContentType.JSON));
                if (parsedDoc.dynamicMappingsUpdate() != null) {
                    // not in the mapping yet, try again
                    continue;
                }
                dfm = documentMapper.mappers();
                assertNotNull(dfm.indexAnalyzer().tokenStream(fieldName, "foo"));
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
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("_source")
                        .field("enabled", false)
                    .endObject()
                .endObject().endObject()));
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", mapping, reason);

        CompressedXContent update = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject().endObject()));
        DocumentMapper mapper = mapperService.merge("type", update, reason);

        assertNotNull(mapper.mappers().getMapper("foo"));
        assertFalse(mapper.sourceMapper().enabled());
    }

    public void testMergeMetadataFieldsForIndexTemplates() throws IOException {
        CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("_source")
                        .field("enabled", false)
                    .endObject()
                .endObject().endObject()));
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", mapping, MergeReason.INDEX_TEMPLATE);

        CompressedXContent update = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("_source")
                        .field("enabled", true)
                    .endObject()
                .endObject().endObject()));
        DocumentMapper mapper = mapperService.merge("type", update, MergeReason.INDEX_TEMPLATE);
        assertTrue(mapper.sourceMapper().enabled());
    }

    public void testMergeMeta() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        String initMapping = Strings
            .toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("test")
                        .startObject("_meta")
                            .field("foo").value("bar")
                        .endObject()
                    .endObject()
                .endObject());
        DocumentMapper initMapper = parser.parse("test", new CompressedXContent(initMapping));

        assertThat(initMapper.meta().get("foo"), equalTo("bar"));

        String updateMapping = Strings
            .toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("test")
                        .startObject("properties")
                            .startObject("name").field("type", "text").endObject()
                        .endObject()
                    .endObject()
                .endObject());
        DocumentMapper updatedMapper = parser.parse("test", new CompressedXContent(updateMapping));

        DocumentMapper mergedMapper = initMapper.merge(updatedMapper.mapping(), MergeReason.MAPPING_UPDATE);
        assertThat(mergedMapper.meta().get("foo"), equalTo("bar"));

        updateMapping = Strings
            .toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("test")
                        .startObject("_meta")
                            .field("foo").value("new_bar")
                        .endObject()
                    .endObject()
                .endObject());
        updatedMapper = parser.parse("test", new CompressedXContent(updateMapping));

        mergedMapper = initMapper.merge(updatedMapper.mapping(), MergeReason.MAPPING_UPDATE);
        assertThat(mergedMapper.meta().get("foo"), equalTo("new_bar"));
    }

    public void testMergeMetaForIndexTemplate() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        String initMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_meta")
                    .field("field", "value")
                    .startObject("object")
                        .field("field1", "value1")
                        .field("field2", "value2")
                    .endObject()
                .endObject()
            .endObject());
        DocumentMapper initMapper = parser.parse(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(initMapping));

        Map<String, Object> expected = Map.of("field", "value",
            "object", Map.of("field1", "value1", "field2", "value2"));
        assertThat(initMapper.meta(), equalTo(expected));

        String updateMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("name").field("type", "text").endObject()
                .endObject()
            .endObject());
        DocumentMapper updatedMapper = parser.parse(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(updateMapping));
        DocumentMapper mergedMapper = initMapper.merge(updatedMapper.mapping(), MergeReason.INDEX_TEMPLATE);
        assertThat(mergedMapper.meta(), equalTo(expected));

        updateMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_meta")
                    .field("field", "value")
                    .startObject("object")
                        .field("field2", "new_value")
                        .field("field3", "value3")
                    .endObject()
                .endObject()
            .endObject());
        updatedMapper = parser.parse(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(updateMapping));
        mergedMapper = mergedMapper.merge(updatedMapper.mapping(), MergeReason.INDEX_TEMPLATE);

        expected = Map.of("field", "value",
            "object", Map.of("field1", "value1", "field2", "new_value", "field3", "value3"));
        assertThat(mergedMapper.meta(), equalTo(expected));
    }
}
