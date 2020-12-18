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

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

public class FieldFetcherTests extends MapperServiceTestCase {

    public void testLeafValues() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "first", "second")
            .startObject("object")
                .field("field", "third")
            .endObject()
        .endObject();

        List<FieldAndFormat> fieldAndFormats = List.of(
            new FieldAndFormat("field", null),
            new FieldAndFormat("object.field", null));
        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormats, null);
        assertThat(fields.size(), equalTo(2));

        DocumentField field = fields.get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("first", "second"));

        DocumentField objectField = fields.get("object.field");
        assertNotNull(objectField);
        assertThat(objectField.getValues().size(), equalTo(1));
        assertThat(objectField.getValues(), hasItems("third"));
    }

    public void testObjectValues() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .startObject("float_range")
                .field("gte", 0.0f)
                .field("lte", 2.718f)
            .endObject()
        .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "float_range");
        assertThat(fields.size(), equalTo(1));

        DocumentField rangeField = fields.get("float_range");
        assertNotNull(rangeField);
        assertThat(rangeField.getValues().size(), equalTo(1));
        assertThat(rangeField.getValue(), equalTo(Map.of("gte", 0.0f, "lte", 2.718f)));
    }

    public void testMixedObjectValues() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .startObject("foo").field("cat", "meow").endObject()
            .field("foo.bar", "baz")
            .endObject();

        ParsedDocument doc = mapperService.documentMapper().parse(source(Strings.toString(source)));
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "foo.bar");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("foo.bar");
        assertThat(field.getValues().size(), equalTo(1));
        assertThat(field.getValue(), equalTo("baz"));
    }

    public void testNonExistentField() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
        .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "non-existent");
        assertThat(fields.size(), equalTo(0));
    }

    public void testMetadataFields() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
        .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "_routing");
        assertTrue(fields.isEmpty());
    }

    public void testFetchAllFields() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .startObject("object")
                .field("field", "other-value")
            .endObject()
        .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "*");
        assertThat(fields.size(), equalTo(2));
    }

    public void testNestedArrays() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .startArray("field")
                .startArray().value("first").value("second").endArray()
            .endArray()
        .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "field");
        DocumentField field = fields.get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("first", "second"));

        source = XContentFactory.jsonBuilder().startObject()
            .startArray("object")
                .startObject().array("field", "first", "second").endObject()
                .startObject().array("field", "third").endObject()
                .startObject().field("field", "fourth").endObject()
            .endArray()
            .endObject();

        fields = fetchFields(mapperService, source, "object.field");
        field = fields.get("object.field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(4));
        assertThat(field.getValues(), hasItems("first", "second", "third", "fourth"));
    }

    public void testArrayValueMappers() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("geo_point", 27.1, 42.0)
        .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "geo_point");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("geo_point");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(1));

        // Test a field with multiple geo-points.
        source = XContentFactory.jsonBuilder().startObject()
            .startArray("geo_point")
                .startArray().value(27.1).value(42.0).endArray()
                .startArray().value(31.4).value(42.0).endArray()
            .endArray()
        .endObject();

        fields = fetchFields(mapperService, source, "geo_point");
        assertThat(fields.size(), equalTo(1));

        field = fields.get("geo_point");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
    }

    public void testFieldNamesWithWildcard() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "first", "second")
            .field("integer_field", 333)
            .startObject("object")
                .field("field", "fourth")
            .endObject()
        .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "*field");
        assertThat(fields.size(), equalTo(3));

        DocumentField field = fields.get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("first", "second"));

        DocumentField otherField = fields.get("integer_field");
        assertNotNull(otherField);
        assertThat(otherField.getValues().size(), equalTo(1));
        assertThat(otherField.getValues(), hasItems(333));

        DocumentField objectField = fields.get("object.field");
        assertNotNull(objectField);
        assertThat(objectField.getValues().size(), equalTo(1));
        assertThat(objectField.getValues(), hasItems("fourth"));
    }

    public void testDateFormat() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .field("date_field", "1990-12-29T00:00:00.000Z")
        .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, List.of(
            new FieldAndFormat("field", null),
            new FieldAndFormat("date_field", "yyyy/MM/dd")),
            null);
        assertThat(fields.size(), equalTo(2));

        DocumentField field = fields.get("field");
        assertNotNull(field);

        DocumentField dateField = fields.get("date_field");
        assertNotNull(dateField);
        assertThat(dateField.getValues().size(), equalTo(1));
        assertThat(dateField.getValue(), equalTo("1990/12/29"));
    }

    public void testIgnoreAbove() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "keyword");
            b.field("ignore_above", 20);
        }));

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "value", "other_value", "really_really_long_value")
            .endObject();
        Map<String, DocumentField> fields = fetchFields(mapperService, source, "field");
        DocumentField field = fields.get("field");
        assertThat(field.getValues().size(), equalTo(2));

        source = XContentFactory.jsonBuilder().startObject()
            .array("field", "really_really_long_value")
            .endObject();
        fields = fetchFields(mapperService, source, "field");
        assertFalse(fields.containsKey("field"));
    }

    public void testFieldAliases() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field").field("type", "keyword").endObject();
            b.startObject("alias_field");
            {
                b.field("type", "alias");
                b.field("path", "field");
            }
            b.endObject();
        }));

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "alias_field");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("alias_field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(1));
        assertThat(field.getValues(), hasItems("value"));

        fields = fetchFields(mapperService, source, "*field");
        assertThat(fields.size(), equalTo(2));
        assertTrue(fields.containsKey("alias_field"));
        assertTrue(fields.containsKey("field"));
    }

    public void testMultiFields() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "integer");
            b.startObject("fields");
            {
                b.startObject("keyword").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", 42)
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "field.keyword");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("field.keyword");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(1));
        assertThat(field.getValues(), hasItems("42"));

        fields = fetchFields(mapperService, source, "field*");
        assertThat(fields.size(), equalTo(2));
        assertTrue(fields.containsKey("field"));
        assertTrue(fields.containsKey("field.keyword"));
    }

    public void testCopyTo() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field").field("type", "keyword").endObject();
            b.startObject("other_field");
            {
                b.field("type", "integer");
                b.field("copy_to", "field");
            }
            b.endObject();
            b.startObject("yet_another_field");
            {
                b.field("type", "keyword");
                b.field("copy_to", "field");
            }
            b.endObject();
        }));

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "one", "two", "three")
            .array("other_field", 1, 2, 3)
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "field");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(6));
        assertThat(field.getValues(), hasItems("one", "two", "three", "1", "2", "3"));
    }

    public void testObjectFields() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "first", "second")
            .startObject("object")
                .field("field", "third")
            .endObject()
        .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "object");
        assertFalse(fields.containsKey("object"));
    }

    public void testTextSubFields() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.startObject("index_prefixes").endObject();
            b.field("index_phrases", true);
        }));

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "some text")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "*");
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("field"));

        for (DocumentField field : fields.values()) {
            assertThat(field.getValues().size(), equalTo(1));
            assertThat(field.getValue(), equalTo("some text"));
        }
    }

    public void testSimpleUnmappedFields() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
                .field("unmapped_f1", "some text")
                .field("unmapped_f2", "some text")
                .field("unmapped_f3", "some text")
                .field("something_else", "some text")
                .nullField("null_value")
                .startObject("object")
                    .field("a", "foo")
                .endObject()
                .field("object.b", "bar")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_f*", null, true), null);
        assertThat(fields.size(), equalTo(3));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_f1", "unmapped_f2", "unmapped_f3"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("un*1", null, true), null);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_f1"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("*thing*", null, true), null);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("something_else"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("null*", null, true), null);
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("object.a", null, true), null);
        assertThat(fields.size(), equalTo(1));
        assertEquals("foo", fields.get("object.a").getValues().get(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("object.b", null, true), null);
        assertThat(fields.size(), equalTo(1));
        assertEquals("bar", fields.get("object.b").getValues().get(0));
    }

    public void testSimpleUnmappedArray() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject().array("unmapped_field", "foo", "bar").endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field", null, true), null);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_field"));
        DocumentField field = fields.get("unmapped_field");

        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("foo", "bar"));
    }

    public void testSimpleUnmappedArrayWithObjects() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .startArray("unmapped_field")
                .startObject()
                    .field("f1", "a")
                .endObject()
                .startObject()
                    .field("f2", "b")
                .endObject()
            .endArray()
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field", null, true), null);
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field.f*", null, true), null);
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.get("unmapped_field.f1").getValue(), equalTo("a"));
        assertThat(fields.get("unmapped_field.f2").getValue(), equalTo("b"));

        source = XContentFactory.jsonBuilder().startObject()
            .startArray("unmapped_field")
                .startObject()
                    .field("f1", "a")
                    .array("f2", 1, 2)
                    .array("f3", 1, 2)
                .endObject()
                .startObject()
                    .field("f1", "b") // same field name, this should result in a list returned
                    .array("f2", 3, 4)
                    .array("f3", "foo")
                .endObject()
            .endArray()
            .endObject();

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field.f1", null, true), null);
        assertThat(fields.size(), equalTo(1));
        DocumentField field = fields.get("unmapped_field.f1");
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("a", "b"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field.f2", null, true), null);
        assertThat(fields.size(), equalTo(1));
        field = fields.get("unmapped_field.f2");
        assertThat(field.getValues().size(), equalTo(4));
        assertThat(field.getValues(), hasItems(1, 2, 3, 4));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field.f3", null, true), null);
        assertThat(fields.size(), equalTo(1));
        field = fields.get("unmapped_field.f3");
        assertThat(field.getValues().size(), equalTo(3));
        assertThat(field.getValues(), hasItems(1, 2, "foo"));
    }

    public void testUnmappedFieldsInsideObject() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
                .startObject("obj")
                    .field("type", "object")
                    .field("dynamic", "false")
                    .startObject("properties")
                        .startObject("f1").field("type", "keyword").endObject()
                    .endObject()
                .endObject()
            .endObject()
            .endObject()
        .endObject();

        MapperService mapperService = createMapperService(mapping);

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("obj.f1", "value1")
            .field("obj.f2", "unmapped_value_f2")
            .field("obj.innerObj.f3", "unmapped_value_f3")
            .field("obj.innerObj.f4", "unmapped_value_f4")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("*", null, false), null);

        // without unmapped fields this should only return "obj.f1"
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("obj.f1"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("*", null, true), null);
        assertThat(fields.size(), equalTo(4));
        assertThat(fields.keySet(), containsInAnyOrder("obj.f1", "obj.f2", "obj.innerObj.f3", "obj.innerObj.f4"));
    }

    public void testUnmappedFieldsInsideDisabledObject() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
                .startObject("obj")
                    .field("type", "object")
                    .field("enabled", "false")
                .endObject()
            .endObject()
            .endObject()
        .endObject();

        MapperService mapperService = createMapperService(mapping);

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .startArray("obj")
            .value("string_value")
            .startObject()
                .field("a", "b")
            .endObject()
            .startArray()
                .value(1).value(2).value(3)
            .endArray()
            .endArray()
        .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("*", null, false), null);
        // without unmapped fields this should return nothing
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("*", null, true), null);
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.keySet(), containsInAnyOrder("obj", "obj.a"));

        List<Object> obj = fields.get("obj").getValues();
        assertEquals(4, obj.size());
        assertThat(obj, hasItems("string_value", 1, 2, 3));

        List<Object> innerObj = fields.get("obj.a").getValues();
        assertEquals(1, innerObj.size());
        assertEquals("b", fields.get("obj.a").getValue());
    }

    /**
     * If a mapped field for some reason contains a "_source" value that is not returned by the
     * mapped retrieval mechanism (e.g. because its malformed), we don't want to fetch it from _source.
     */
    public void testMappedFieldNotOverwritten() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
                .startObject("f1")
                    .field("type", "integer")
                    .field("ignore_malformed", "true")
                .endObject()
            .endObject()
            .endObject()
        .endObject();

        MapperService mapperService = createMapperService(mapping);

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("f1", "malformed")
            .endObject();

        // this should not return a field bc. f1 is in the ignored fields
        Map<String, DocumentField> fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("*", null, true)), Set.of("f1"));
        assertThat(fields.size(), equalTo(0));

        // and this should neither
        fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("*", null, true)), Set.of("f1"));
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("f1", null, true)), Set.of("f1"));
        assertThat(fields.size(), equalTo(0));

        // check this also does not overwrite with arrays
        source = XContentFactory.jsonBuilder().startObject()
            .array("f1", "malformed")
            .endObject();

        fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("f1", null, true)), Set.of("f1"));
        assertThat(fields.size(), equalTo(0));
    }

    public void testUnmappedFieldsWildcard() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .startObject("unmapped_object")
                .field("a", "foo")
                .field("b", "bar")
            .endObject()
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_object", null, true), null);
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmap*object", null, true), null);
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_object.*", null, true), null);
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_object.a", "unmapped_object.b"));

        assertThat(fields.get("unmapped_object.a").getValue(), equalTo("foo"));
        assertThat(fields.get("unmapped_object.b").getValue(), equalTo("bar"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_object.a", null, true), null);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get("unmapped_object.a").getValue(), equalTo("foo"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_object.b", null, true), null);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get("unmapped_object.b").getValue(), equalTo("bar"));
    }

    public void testLastFormatWins() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .startArray("date_field")
                .value("2011-11-11T11:11:11")
                .value("2012-12-12T12:12:12")
            .endArray()
            .endObject();

        List<FieldAndFormat> ff = new ArrayList<>();
        ff.add(new FieldAndFormat("date_field", "year", false));
        Map<String, DocumentField> fields = fetchFields(mapperService, source, ff, null);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get("date_field").getValues().size(), equalTo(2));
        assertThat(fields.get("date_field").getValues().get(0), equalTo("2011"));
        assertThat(fields.get("date_field").getValues().get(1), equalTo("2012"));

        ff.add(new FieldAndFormat("date_field", "hour", false));
        fields = fetchFields(mapperService, source, ff, null);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get("date_field").getValues().size(), equalTo(2));
        assertThat(fields.get("date_field").getValues().get(0), equalTo("11"));
        assertThat(fields.get("date_field").getValues().get(1), equalTo("12"));
    }

    private List<FieldAndFormat> fieldAndFormatList(String name, String format, boolean includeUnmapped) {
        return Collections.singletonList(new FieldAndFormat(name, format, includeUnmapped));
    }

    private Map<String, DocumentField> fetchFields(MapperService mapperService, XContentBuilder source, String fieldPattern)
        throws IOException {
        return fetchFields(mapperService, source, fieldAndFormatList(fieldPattern, null, false), null);
    }

    private static Map<String, DocumentField> fetchFields(
        MapperService mapperService,
        XContentBuilder source,
        List<FieldAndFormat> fields,
        @Nullable Set<String> ignoreFields
    ) throws IOException {

        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(BytesReference.bytes(source));

        FieldFetcher fieldFetcher = FieldFetcher.create(newQueryShardContext(mapperService), null, fields);
        return fieldFetcher.fetch(sourceLookup, ignoreFields != null ? ignoreFields : Collections.emptySet());
    }

    public MapperService createMapperService() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
                .startObject("field").field("type", "keyword").endObject()
                .startObject("integer_field").field("type", "integer").endObject()
                .startObject("date_field").field("type", "date").endObject()
                .startObject("geo_point").field("type", "geo_point").endObject()
                .startObject("float_range").field("type", "float_range").endObject()
                .startObject("object")
                    .startObject("properties")
                        .startObject("field").field("type", "keyword").endObject()
                    .endObject()
                .endObject()
                .startObject("field_that_does_not_match").field("type", "keyword").endObject()
            .endObject()
            .endObject()
        .endObject();

        return createMapperService(mapping);
    }

    private static QueryShardContext newQueryShardContext(MapperService mapperService) {
        Settings settings = Settings.builder().put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid").build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        return new QueryShardContext(0, 0, indexSettings, null, null, null, mapperService, null, null, null, null, null, null, null, null,
            null, null, null, emptyMap());
    }
}
