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

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ObjectMapper.Dynamic;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class NestedObjectMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testEmptyNested() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").endObject()
                .endObject().endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .nullField("nested1")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.docs().size(), equalTo(1));

        doc = docMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested").endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.docs().size(), equalTo(1));
    }

    public void testSingleNested() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").endObject()
                .endObject().endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startObject("nested1").field("field1", "1").field("field2", "2").endObject()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.docs().size(), equalTo(2));
        assertThat(doc.docs().get(0).get(NestedPathFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePath()));
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("nested1.field2"), equalTo("2"));

        assertThat(doc.docs().get(1).get("field"), equalTo("value"));


        doc = docMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject().field("field1", "1").field("field2", "2").endObject()
                        .startObject().field("field1", "3").field("field2", "4").endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.docs().size(), equalTo(3));
        assertThat(doc.docs().get(0).get(NestedPathFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePath()));
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("nested1.field2"), equalTo("2"));
        assertThat(doc.docs().get(1).get(NestedPathFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePath()));
        assertThat(doc.docs().get(1).get("nested1.field1"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field2"), equalTo("4"));

        assertThat(doc.docs().get(2).get("field"), equalTo("value"));
    }

    public void testMultiNested() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").startObject("properties")
                .startObject("nested2").field("type", "nested")
                .endObject().endObject().endObject()
                .endObject().endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));
        assertThat(nested1Mapper.nested().isIncludeInParent(), equalTo(false));
        assertThat(nested1Mapper.nested().isIncludeInRoot(), equalTo(false));
        ObjectMapper nested2Mapper = docMapper.objectMappers().get("nested1.nested2");
        assertThat(nested2Mapper.nested().isNested(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInParent(), equalTo(false));
        assertThat(nested2Mapper.nested().isIncludeInRoot(), equalTo(false));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                            .startObject().field("field1", "1").startArray("nested2")
                            .startObject().field("field2", "2").endObject()
                            .startObject().field("field2", "3").endObject()
                        .endArray()
                        .endObject()
                        .startObject().field("field1", "4")
                        .startArray("nested2")
                            .startObject().field("field2", "5").endObject()
                            .startObject().field("field2", "6").endObject()
                        .endArray().endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(6).get("nested1.nested2.field2"), nullValue());
    }

    public void testMultiObjectAndNested1() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").startObject("properties")
                .startObject("nested2").field("type", "nested").field("include_in_parent", true)
                .endObject().endObject().endObject()
                .endObject().endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));
        assertThat(nested1Mapper.nested().isIncludeInParent(), equalTo(false));
        assertThat(nested1Mapper.nested().isIncludeInRoot(), equalTo(false));
        ObjectMapper nested2Mapper = docMapper.objectMappers().get("nested1.nested2");
        assertThat(nested2Mapper.nested().isNested(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInParent(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInRoot(), equalTo(false));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject().field("field1", "1")
                        .startArray("nested2")
                            .startObject().field("field2", "2").endObject()
                            .startObject().field("field2", "3").endObject()
                        .endArray().endObject()
                        .startObject().field("field1", "4")
                        .startArray("nested2")
                            .startObject().field("field2", "5").endObject()
                            .startObject().field("field2", "6").endObject()
                        .endArray().endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(6).get("nested1.nested2.field2"), nullValue());
    }

    public void testMultiObjectAndNested2() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("nested1").field("type", "nested").field("include_in_parent", true)
                .startObject("properties")
                .startObject("nested2").field("type", "nested").field("include_in_parent", true)
                .endObject().endObject().endObject()
                .endObject().endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));
        assertThat(nested1Mapper.nested().isIncludeInParent(), equalTo(true));
        assertThat(nested1Mapper.nested().isIncludeInRoot(), equalTo(false));
        ObjectMapper nested2Mapper = docMapper.objectMappers().get("nested1.nested2");
        assertThat(nested2Mapper.nested().isNested(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInParent(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInRoot(), equalTo(false));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject().field("field1", "1")
                        .startArray("nested2")
                            .startObject().field("field2", "2").endObject()
                            .startObject().field("field2", "3").endObject()
                        .endArray().endObject()
                        .startObject().field("field1", "4")
                        .startArray("nested2")
                            .startObject().field("field2", "5").endObject()
                            .startObject().field("field2", "6").endObject()
                        .endArray().endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).getFields("nested1.field1").length, equalTo(2));
        assertThat(doc.docs().get(6).getFields("nested1.nested2.field2").length, equalTo(4));
    }

    public void testMultiRootAndNested1() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").startObject("properties")
                .startObject("nested2").field("type", "nested").field("include_in_root", true)
                .endObject().endObject().endObject()
                .endObject().endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));
        assertThat(nested1Mapper.nested().isIncludeInParent(), equalTo(false));
        assertThat(nested1Mapper.nested().isIncludeInRoot(), equalTo(false));
        ObjectMapper nested2Mapper = docMapper.objectMappers().get("nested1.nested2");
        assertThat(nested2Mapper.nested().isNested(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInParent(), equalTo(false));
        assertThat(nested2Mapper.nested().isIncludeInRoot(), equalTo(true));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject().field("field1", "1")
                        .startArray("nested2")
                            .startObject().field("field2", "2").endObject()
                            .startObject().field("field2", "3").endObject()
                        .endArray().endObject()
                        .startObject().field("field1", "4")
                        .startArray("nested2")
                            .startObject().field("field2", "5").endObject()
                            .startObject().field("field2", "6").endObject()
                        .endArray().endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(6).getFields("nested1.nested2.field2").length, equalTo(4));
    }

    /**
     * Checks that multiple levels of nested includes where a node is both directly and transitively
     * included in root by {@code include_in_root} and a chain of {@code include_in_parent} does not
     * lead to duplicate fields on the root document.
     */
    public void testMultipleLevelsIncludeRoot1() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
            .startObject("nested1").field("type", "nested").field("include_in_root", true)
            .field("include_in_parent", true).startObject("properties")
            .startObject("nested2").field("type", "nested").field("include_in_root", true)
            .field("include_in_parent", true)
            .endObject().endObject().endObject()
            .endObject().endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject().startArray("nested1")
                        .startObject().startArray("nested2").startObject().field("foo", "bar")
                        .endObject().endArray().endObject().endArray()
                        .endObject()),
            XContentType.JSON));

        final Collection<IndexableField> fields = doc.rootDoc().getFields();
        assertThat(fields.size(), equalTo(new HashSet<>(fields).size()));
    }

    /**
     * Same as {@link NestedObjectMapperTests#testMultipleLevelsIncludeRoot1()} but tests for the
     * case where the transitive {@code include_in_parent} and redundant {@code include_in_root}
     * happen on a chain of nodes that starts from a parent node that is not directly connected to
     * root by a chain of {@code include_in_parent}, i.e. that has {@code include_in_parent} set to
     * {@code false} and {@code include_in_root} set to {@code true}.
     */
    public void testMultipleLevelsIncludeRoot2() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
            .startObject("nested1").field("type", "nested")
            .field("include_in_root", true).field("include_in_parent", true).startObject("properties")
            .startObject("nested2").field("type", "nested")
            .field("include_in_root", true).field("include_in_parent", false).startObject("properties")
            .startObject("nested3").field("type", "nested")
            .field("include_in_root", true).field("include_in_parent", true)
            .endObject().endObject().endObject().endObject().endObject()
            .endObject().endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject().startArray("nested1")
                        .startObject().startArray("nested2")
                        .startObject().startArray("nested3").startObject().field("foo", "bar")
                        .endObject().endArray().endObject().endArray().endObject().endArray()
                        .endObject()),
            XContentType.JSON));

        final Collection<IndexableField> fields = doc.rootDoc().getFields();
        assertThat(fields.size(), equalTo(new HashSet<>(fields).size()));
    }

    public void testNestedArrayStrict() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").field("dynamic", "strict").startObject("properties")
                .startObject("field1").field("type", "text")
                .endObject().endObject().endObject()
                .endObject().endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));
        assertThat(nested1Mapper.dynamic(), equalTo(Dynamic.STRICT));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject().field("field1", "1").endObject()
                        .startObject().field("field1", "4").endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.docs().size(), equalTo(3));
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("field"), equalTo("value"));
    }

    public void testLimitOfNestedFieldsPerIndex() throws Exception {
        Function<String, String> mapping = type -> {
            try {
                return Strings.toString(XContentFactory.jsonBuilder().startObject().startObject(type).startObject("properties")
                    .startObject("nested1").field("type", "nested").startObject("properties")
                    .startObject("nested2").field("type", "nested")
                    .endObject().endObject().endObject()
                    .endObject().endObject().endObject());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        // default limit allows at least two nested fields
        createIndex("test1").mapperService().merge("type", new CompressedXContent(mapping.apply("type")),
            MergeReason.MAPPING_UPDATE);

        // explicitly setting limit to 0 prevents nested fields
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            createIndex("test2", Settings.builder()
                .put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 0).build())
                .mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("Limit of nested fields [0] in index [test2] has been exceeded"));

        // setting limit to 1 with 2 nested fields fails
        e = expectThrows(IllegalArgumentException.class, () ->
            createIndex("test3", Settings.builder()
                .put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 1).build())
                .mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("Limit of nested fields [1] in index [test3] has been exceeded"));

        // do not check nested fields limit if mapping is not updated
        createIndex("test4", Settings.builder()
            .put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 0).build())
            .mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_RECOVERY);
    }

    public void testParentObjectMapperAreNested() throws Exception {
        MapperService mapperService = createIndex("index1", Settings.EMPTY, jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("comments")
                        .field("type", "nested")
                        .startObject("properties")
                            .startObject("messages")
                                .field("type", "nested").endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()).mapperService();
        ObjectMapper objectMapper = mapperService.getObjectMapper("comments.messages");
        assertTrue(objectMapper.parentObjectMapperAreNested(mapperService));

        mapperService = createIndex("index2", Settings.EMPTY, jsonBuilder().startObject()
            .startObject("properties")
                .startObject("comments")
                    .field("type", "object")
                        .startObject("properties")
                            .startObject("messages")
                                .field("type", "nested").endObject()
                            .endObject()
                    .endObject()
                .endObject()
            .endObject()).mapperService();
        objectMapper = mapperService.getObjectMapper("comments.messages");
        assertFalse(objectMapper.parentObjectMapperAreNested(mapperService));
    }

    public void testLimitNestedDocsDefaultSettings() throws Exception{
        Settings settings = Settings.builder().build();
        MapperService mapperService = createIndex("test1", settings).mapperService();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("nested1").field("type", "nested").endObject()
            .endObject().endObject().endObject());
        DocumentMapper docMapper = mapperService.documentMapperParser().parse("type", new CompressedXContent(mapping));
        long defaultMaxNoNestedDocs = MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.get(settings);

        // parsing a doc with No. nested objects > defaultMaxNoNestedDocs fails
        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject();
        {
            docBuilder.startArray("nested1");
            {
                for(int i = 0; i <= defaultMaxNoNestedDocs; i++) {
                    docBuilder.startObject().field("f", i).endObject();
                }
            }
            docBuilder.endArray();
        }
        docBuilder.endObject();
        SourceToParse source1 = new SourceToParse("test1", "1",
            BytesReference.bytes(docBuilder), XContentType.JSON);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source1));
        assertEquals(
            "The number of nested documents has exceeded the allowed limit of [" + defaultMaxNoNestedDocs
                + "]. This limit can be set by changing the [" + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
                + "] index level setting.",
            e.getMessage()
        );
    }

    public void testLimitNestedDocs() throws Exception{
        // setting limit to allow only two nested objects in the whole doc
        long maxNoNestedDocs = 2L;
        MapperService mapperService = createIndex("test1", Settings.builder()
            .put(MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey(), maxNoNestedDocs).build()).mapperService();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("nested1").field("type", "nested").endObject()
            .endObject().endObject().endObject());
        DocumentMapper docMapper = mapperService.documentMapperParser().parse("type", new CompressedXContent(mapping));

        // parsing a doc with 2 nested objects succeeds
        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject();
        {
            docBuilder.startArray("nested1");
            {
                docBuilder.startObject().field("field1", "11").field("field2", "21").endObject();
                docBuilder.startObject().field("field1", "12").field("field2", "22").endObject();
            }
            docBuilder.endArray();
        }
        docBuilder.endObject();
        SourceToParse source1 = new SourceToParse("test1", "1",
            BytesReference.bytes(docBuilder), XContentType.JSON);
        ParsedDocument doc = docMapper.parse(source1);
        assertThat(doc.docs().size(), equalTo(3));

        // parsing a doc with 3 nested objects fails
        XContentBuilder docBuilder2 = XContentFactory.jsonBuilder();
        docBuilder2.startObject();
        {
            docBuilder2.startArray("nested1");
            {
                docBuilder2.startObject().field("field1", "11").field("field2", "21").endObject();
                docBuilder2.startObject().field("field1", "12").field("field2", "22").endObject();
                docBuilder2.startObject().field("field1", "13").field("field2", "23").endObject();
            }
            docBuilder2.endArray();
        }
        docBuilder2.endObject();
        SourceToParse source2 = new SourceToParse("test1", "2",
            BytesReference.bytes(docBuilder2), XContentType.JSON);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source2));
        assertEquals(
            "The number of nested documents has exceeded the allowed limit of [" + maxNoNestedDocs
            + "]. This limit can be set by changing the [" + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
            + "] index level setting.",
            e.getMessage()
        );
    }

    public void testLimitNestedDocsMultipleNestedFields() throws Exception{
        // setting limit to allow only two nested objects in the whole doc
        long maxNoNestedDocs = 2L;
        MapperService mapperService = createIndex("test1", Settings.builder()
            .put(MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey(), maxNoNestedDocs).build()).mapperService();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("nested1").field("type", "nested").endObject()
            .startObject("nested2").field("type", "nested").endObject()
            .endObject().endObject().endObject());
        DocumentMapper docMapper = mapperService.documentMapperParser().parse("type", new CompressedXContent(mapping));

        // parsing a doc with 2 nested objects succeeds
        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject();
        {
            docBuilder.startArray("nested1");
            {
                docBuilder.startObject().field("field1", "11").field("field2", "21").endObject();
            }
            docBuilder.endArray();
            docBuilder.startArray("nested2");
            {
                docBuilder.startObject().field("field1", "21").field("field2", "22").endObject();
            }
            docBuilder.endArray();
        }
        docBuilder.endObject();
        SourceToParse source1 = new SourceToParse("test1", "1",
            BytesReference.bytes(docBuilder), XContentType.JSON);
        ParsedDocument doc = docMapper.parse(source1);
        assertThat(doc.docs().size(), equalTo(3));

        // parsing a doc with 3 nested objects fails
        XContentBuilder docBuilder2 = XContentFactory.jsonBuilder();
        docBuilder2.startObject();
        {
            docBuilder2.startArray("nested1");
            {
                docBuilder2.startObject().field("field1", "11").field("field2", "21").endObject();
            }
            docBuilder2.endArray();
            docBuilder2.startArray("nested2");
            {
                docBuilder2.startObject().field("field1", "12").field("field2", "22").endObject();
                docBuilder2.startObject().field("field1", "13").field("field2", "23").endObject();
            }
            docBuilder2.endArray();

        }
        docBuilder2.endObject();
        SourceToParse source2 = new SourceToParse("test1", "2",
            BytesReference.bytes(docBuilder2), XContentType.JSON);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source2));
        assertEquals(
            "The number of nested documents has exceeded the allowed limit of [" + maxNoNestedDocs
                + "]. This limit can be set by changing the [" + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
                + "] index level setting.",
            e.getMessage()
        );
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        /**
         * This is needed to force the index version with {@link IndexMetadata.SETTING_INDEX_VERSION_CREATED}.
         */
        return false;
    }

    public void testReorderParent() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("nested1").field("type", "nested").endObject()
            .endObject().endObject().endObject());

        Settings settings = Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
            VersionUtils.randomIndexCompatibleVersion(random())).build();
        DocumentMapper docMapper = createIndex("test", settings)
            .mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject()
                .field("field1", "1")
                .field("field2", "2")
                .endObject()
                .startObject()
                .field("field1", "3")
                .field("field2", "4")
                .endObject()
                .endArray()
                .endObject()),
            XContentType.JSON));

        assertThat(doc.docs().size(), equalTo(3));
        if (Version.indexCreated(settings).before(Version.V_8_0_0)) {
            assertThat(doc.docs().get(0).get(TypeFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePath()));
        } else {
            assertThat(doc.docs().get(0).get(NestedPathFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePath()));
        }
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("nested1.field2"), equalTo("2"));
        assertThat(doc.docs().get(1).get("nested1.field1"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field2"), equalTo("4"));
        assertThat(doc.docs().get(2).get("field"), equalTo("value"));
    }

    public void testMergeNestedMappings() throws IOException {
        MapperService mapperService = createIndex("index1", Settings.EMPTY, jsonBuilder().startObject()
            .startObject("properties")
                .startObject("nested1")
                    .field("type", "nested")
                .endObject()
            .endObject().endObject()).mapperService();

        String mapping1 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("nested1").field("type", "nested").field("include_in_parent", true)
            .endObject().endObject().endObject().endObject());

        // cannot update `include_in_parent` dynamically
        MapperException e1 = expectThrows(MapperException.class, () -> mapperService.merge("type",
            new CompressedXContent(mapping1), MergeReason.MAPPING_UPDATE));
        assertEquals("The [include_in_parent] parameter can't be updated for the nested object mapping [nested1].", e1.getMessage());

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("nested1").field("type", "nested").field("include_in_root", true)
            .endObject().endObject().endObject().endObject());

        // cannot update `include_in_root` dynamically
        MapperException e2 = expectThrows(MapperException.class, () -> mapperService.merge("type",
            new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE));
        assertEquals("The [include_in_root] parameter can't be updated for the nested object mapping [nested1].", e2.getMessage());
    }
}
