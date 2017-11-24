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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DocumentMapperMergeTests extends ESSingleNodeTestCase {

    public void test1Merge() throws Exception {

        String stage1Mapping = XContentFactory.jsonBuilder().startObject().startObject("person").startObject("properties")
                .startObject("name").field("type", "text").endObject()
                .endObject().endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper stage1 = parser.parse("person", new CompressedXContent(stage1Mapping));
        String stage2Mapping = XContentFactory.jsonBuilder().startObject().startObject("person").startObject("properties")
                .startObject("name").field("type", "text").endObject()
                .startObject("age").field("type", "integer").endObject()
                .startObject("obj1").startObject("properties").startObject("prop1").field("type", "integer").endObject().endObject().endObject()
                .endObject().endObject().endObject().string();
        DocumentMapper stage2 = parser.parse("person", new CompressedXContent(stage2Mapping));

        DocumentMapper merged = stage1.merge(stage2.mapping(), false);
        // stage1 mapping should not have been modified
        assertThat(stage1.mappers().smartNameFieldMapper("age"), nullValue());
        assertThat(stage1.mappers().smartNameFieldMapper("obj1.prop1"), nullValue());
        // but merged should
        assertThat(merged.mappers().smartNameFieldMapper("age"), notNullValue());
        assertThat(merged.mappers().smartNameFieldMapper("obj1.prop1"), notNullValue());
    }

    public void testMergeObjectDynamic() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String objectMapping = XContentFactory.jsonBuilder().startObject().startObject("type1").endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type1", new CompressedXContent(objectMapping));
        assertNull(mapper.root().dynamic());

        String withDynamicMapping = XContentFactory.jsonBuilder().startObject().startObject("type1").field("dynamic", "false").endObject().endObject().string();
        DocumentMapper withDynamicMapper = parser.parse("type1", new CompressedXContent(withDynamicMapping));
        assertThat(withDynamicMapper.root().dynamic(), equalTo(ObjectMapper.Dynamic.FALSE));

        DocumentMapper merged = mapper.merge(withDynamicMapper.mapping(), false);
        assertThat(merged.root().dynamic(), equalTo(ObjectMapper.Dynamic.FALSE));
    }

    public void testMergeObjectAndNested() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String objectMapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("obj").field("type", "object").endObject()
                .endObject().endObject().endObject().string();
        DocumentMapper objectMapper = parser.parse("type1", new CompressedXContent(objectMapping));
        String nestedMapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("obj").field("type", "nested").endObject()
                .endObject().endObject().endObject().string();
        DocumentMapper nestedMapper = parser.parse("type1", new CompressedXContent(nestedMapping));

        try {
            objectMapper.merge(nestedMapper.mapping(), false);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("object mapping [obj] can't be changed from non-nested to nested"));
        }

        try {
            nestedMapper.merge(objectMapper.mapping(), false);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("object mapping [obj] can't be changed from nested to non-nested"));
        }
    }

    public void testMergeSearchAnalyzer() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping1 = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "text").field("analyzer", "standard").field("search_analyzer", "whitespace").endObject().endObject()
                .endObject().endObject().string();
        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "text").field("analyzer", "standard").field("search_analyzer", "keyword").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper existing = parser.parse("type", new CompressedXContent(mapping1));
        DocumentMapper changed = parser.parse("type", new CompressedXContent(mapping2));

        assertThat(((NamedAnalyzer) existing.mappers().getMapper("field").fieldType().searchAnalyzer()).name(), equalTo("whitespace"));
        DocumentMapper merged = existing.merge(changed.mapping(), false);

        assertThat(((NamedAnalyzer) merged.mappers().getMapper("field").fieldType().searchAnalyzer()).name(), equalTo("keyword"));
    }

    public void testChangeSearchAnalyzerToDefault() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();
        String mapping1 = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "text").field("analyzer", "standard").field("search_analyzer", "whitespace").endObject().endObject()
                .endObject().endObject().string();
        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "text").field("analyzer", "standard").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper existing = mapperService.merge("type", new CompressedXContent(mapping1), MapperService.MergeReason.MAPPING_UPDATE, false);
        DocumentMapper merged = mapperService.merge("type", new CompressedXContent(mapping2), MapperService.MergeReason.MAPPING_UPDATE, false);

        assertThat(((NamedAnalyzer) existing.mappers().getMapper("field").fieldType().searchAnalyzer()).name(), equalTo("whitespace"));

        assertThat(((NamedAnalyzer) merged.mappers().getMapper("field").fieldType().searchAnalyzer()).name(), equalTo("standard"));
    }

    public void testConcurrentMergeTest() throws Throwable {
        final MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("test", new CompressedXContent("{\"test\":{}}"), MapperService.MergeReason.MAPPING_UPDATE, false);
        final DocumentMapper documentMapper = mapperService.documentMapper("test");

        DocumentFieldMappers dfm = documentMapper.mappers();
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
                        ParsedDocument doc = documentMapper.parse(SourceToParse.source("test",
                                "test",
                                fieldName,
                                new BytesArray("{ \"" + fieldName + "\" : \"test\" }"),
                                XContentType.JSON));
                        Mapping update = doc.dynamicMappingsUpdate();
                        assert update != null;
                        lastIntroducedFieldName.set(fieldName);
                        mapperService.merge("test", new CompressedXContent(update.toString()), MapperService.MergeReason.MAPPING_UPDATE, false);
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
                ParsedDocument parsedDoc = documentMapper.parse(SourceToParse.source("test",
                        "test",
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
        CompressedXContent mapping = new CompressedXContent(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("_source")
                        .field("enabled", false)
                    .endObject()
                .endObject().endObject().bytes());
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", mapping, MapperService.MergeReason.MAPPING_UPDATE, false);

        CompressedXContent update = new CompressedXContent(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject().endObject().bytes());
        DocumentMapper mapper = mapperService.merge("type", update, MapperService.MergeReason.MAPPING_UPDATE, false);

        assertNotNull(mapper.mappers().getMapper("foo"));
        assertFalse(mapper.sourceMapper().enabled());
    }

    public void testMergeChildType() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        String initMapping = XContentFactory.jsonBuilder().startObject().startObject("child")
            .startObject("_parent").field("type", "parent").endObject()
            .endObject().endObject().string();
        DocumentMapper initMapper = parser.parse("child", new CompressedXContent(initMapping));

        assertThat(initMapper.mappers().getMapper("_parent#parent"), notNullValue());

        String updatedMapping1 = XContentFactory.jsonBuilder().startObject().startObject("child")
            .startObject("properties")
            .startObject("name").field("type", "text").endObject()
            .endObject().endObject().endObject().string();
        DocumentMapper updatedMapper1 = parser.parse("child", new CompressedXContent(updatedMapping1));
        DocumentMapper mergedMapper1 = initMapper.merge(updatedMapper1.mapping(), false);

        assertThat(mergedMapper1.mappers().getMapper("_parent#parent"), notNullValue());
        assertThat(mergedMapper1.mappers().getMapper("name"), notNullValue());

        String updatedMapping2 = XContentFactory.jsonBuilder().startObject().startObject("child")
            .startObject("_parent").field("type", "parent").endObject()
            .startObject("properties")
            .startObject("age").field("type", "byte").endObject()
            .endObject().endObject().endObject().string();
        DocumentMapper updatedMapper2 = parser.parse("child", new CompressedXContent(updatedMapping2));
        DocumentMapper mergedMapper2 = mergedMapper1.merge(updatedMapper2.mapping(), false);

        assertThat(mergedMapper2.mappers().getMapper("_parent#parent"), notNullValue());
        assertThat(mergedMapper2.mappers().getMapper("name"), notNullValue());
        assertThat(mergedMapper2.mappers().getMapper("age"), notNullValue());

        String modParentMapping = XContentFactory.jsonBuilder().startObject().startObject("child")
            .startObject("_parent").field("type", "new_parent").endObject()
            .endObject().endObject().string();
        DocumentMapper modParentMapper = parser.parse("child", new CompressedXContent(modParentMapping));
        Exception e = expectThrows(IllegalArgumentException.class, () -> initMapper.merge(modParentMapper.mapping(), false));
        assertThat(e.getMessage(), containsString("The _parent field's type option can't be changed: [parent]->[new_parent]"));
    }

    public void testMergeAddingParent() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        String initMapping = XContentFactory.jsonBuilder().startObject().startObject("cowboy")
            .startObject("properties")
            .startObject("name").field("type", "text").endObject()
            .endObject().endObject().endObject().string();
        DocumentMapper initMapper = parser.parse("cowboy", new CompressedXContent(initMapping));

        assertThat(initMapper.mappers().getMapper("name"), notNullValue());

        String updatedMapping = XContentFactory.jsonBuilder().startObject().startObject("cowboy")
            .startObject("_parent").field("type", "parent").endObject()
            .startObject("properties")
            .startObject("age").field("type", "byte").endObject()
            .endObject().endObject().endObject().string();
        DocumentMapper updatedMapper = parser.parse("cowboy", new CompressedXContent(updatedMapping));
        Exception e = expectThrows(IllegalArgumentException.class, () -> initMapper.merge(updatedMapper.mapping(), false));
        assertThat(e.getMessage(), containsString("The _parent field's type option can't be changed: [null]->[parent]"));
    }

    public void testMergeMeta() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        String initMapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("test")
                    .startObject("_meta")
                        .field("foo").value("bar")
                    .endObject()
                .endObject()
            .endObject()
            .string();
        DocumentMapper initMapper = parser.parse("test", new CompressedXContent(initMapping));

        assertThat(initMapper.meta().get("foo"), equalTo("bar"));

        String updateMapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("test")
                    .startObject("properties")
                        .startObject("name").field("type", "text").endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();
        DocumentMapper updatedMapper = parser.parse("test", new CompressedXContent(updateMapping));

        assertThat(initMapper.merge(updatedMapper.mapping(), true).meta().get("foo"), equalTo("bar"));

        updateMapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("test")
                    .startObject("_meta")
                        .field("foo").value("new_bar")
                    .endObject()
                .endObject()
            .endObject()
            .string();
        updatedMapper = parser.parse("test", new CompressedXContent(updateMapping));

        assertThat(initMapper.merge(updatedMapper.mapping(), true).meta().get("foo"), equalTo("new_bar"));
    }
}
