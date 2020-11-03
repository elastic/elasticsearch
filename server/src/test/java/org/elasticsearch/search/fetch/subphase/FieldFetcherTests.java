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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

public class FieldFetcherTests extends ESSingleNodeTestCase {

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
        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormats, false);
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
        MapperService mapperService = createMapperService();;
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
            new FieldAndFormat("date_field", "yyyy/MM/dd")), false);
        assertThat(fields.size(), equalTo(2));

        DocumentField field = fields.get("field");
        assertNotNull(field);

        DocumentField dateField = fields.get("date_field");
        assertNotNull(dateField);
        assertThat(dateField.getValues().size(), equalTo(1));
        assertThat(dateField.getValue(), equalTo("1990/12/29"));
    }

    public void testIgnoreAbove() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("field")
                    .field("type", "keyword")
                    .field("ignore_above", 20)
                .endObject()
            .endObject()
        .endObject();

        IndexService indexService = createIndex("index", Settings.EMPTY, mapping);
        MapperService mapperService = indexService.mapperService();

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
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("field").field("type", "keyword").endObject()
                .startObject("alias_field")
                    .field("type", "alias")
                    .field("path", "field")
                .endObject()
            .endObject()
        .endObject();

        IndexService indexService = createIndex("index", Settings.EMPTY, mapping);
        MapperService mapperService = indexService.mapperService();

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
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("field")
                    .field("type", "integer")
                    .startObject("fields")
                        .startObject("keyword").field("type", "keyword").endObject()
                    .endObject()
                .endObject()
            .endObject()
        .endObject();

        IndexService indexService = createIndex("index", Settings.EMPTY, mapping);
        MapperService mapperService = indexService.mapperService();

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
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("field")
                    .field("type", "keyword")
                .endObject()
                .startObject("other_field")
                    .field("type", "integer")
                    .field("copy_to", "field")
                .endObject()
            .endObject()
        .endObject();

        IndexService indexService = createIndex("index", Settings.EMPTY, mapping);
        MapperService mapperService = indexService.mapperService();

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
        MapperService mapperService = createMapperService();;
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
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("field")
                    .field("type", "text")
                    .startObject("index_prefixes").endObject()
                    .field("index_phrases", true)
                .endObject()
            .endObject()
        .endObject();

        IndexService indexService = createIndex("index", Settings.EMPTY, mapping);
        MapperService mapperService = indexService.mapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "some text")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "*");
        assertThat(fields.size(), equalTo(3));
        assertThat(fields.keySet(), containsInAnyOrder("field", "field._index_prefix", "field._index_phrase"));

        for (DocumentField field : fields.values()) {
            assertThat(field.getValues().size(), equalTo(1));
            assertThat(field.getValue(), equalTo("some text"));
        }
    }

    public void testSimpleUnmappedFields() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("unmapped_f1", "some text")
            .field("unmapped_f2", "some text")
            .field("unmapped_f3", "some text")
            .field("something_else", "some text")
            .nullField("null_value")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "unmapped_f*", true);
        assertThat(fields.size(), equalTo(3));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_f1", "unmapped_f2", "unmapped_f3"));

        fields = fetchFields(mapperService, source, "un*1", true);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_f1"));

        fields = fetchFields(mapperService, source, "*thing*", true);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("something_else"));

        fields = fetchFields(mapperService, source, "*thing*", true);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("something_else"));

        // TODO discuss wether we want to return something here
        fields = fetchFields(mapperService, source, "null*", true);
        assertThat(fields.size(), equalTo(0));
        // assertThat(fields.get("null_value").getValue(), equalTo(null));
    }

    public void testSimpleUnmappedArray() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("unmapped_field", "foo", "bar")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "unmapped_field", true);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_field"));

        for (DocumentField field : fields.values()) {
            assertThat(field.getValues().size(), equalTo(2));
            assertThat(field.getValues().get(0), equalTo("foo"));
            assertThat(field.getValues().get(1), equalTo("bar"));
        }
    }

    public void testUnmappedFieldsInsideObject() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("obj")
                    .field("type", "object")
                    .field("dynamic", "false")
                    .startObject("properties")
                        .startObject("f1").field("type", "keyword").endObject()
                    .endObject()
                .endObject()
            .endObject()
        .endObject();

        IndexService indexService = createIndex("index", Settings.EMPTY, mapping);
        MapperService mapperService = indexService.mapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("obj.f1", "value1")
            .field("obj.f2", "unmapped_value_f2")
            .field("obj.innerObj.f3", "unmapped_value_f3")
            .field("obj.innerObj.f4", "unmapped_value_f4")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "*", false);

        // without unmapped fields this should only return "obj.f1"
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("obj.f1"));

        fields = fetchFields(mapperService, source, "*", true);
        assertThat(fields.size(), equalTo(4));
        assertThat(fields.keySet(), containsInAnyOrder("obj.f1", "obj.f2", "obj.innerObj.f3", "obj.innerObj.f4"));
    }

    /**
     * If a mapped field for some reason contains a "_source" value that is not returned by the
     * mapped retrieval mechanism (e.g. because its malformed), we don't want to fetch it from _source.
     */
    public void testMappedFieldNotOverwritten() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("f1")
                    .field("type", "integer")
                    .field("ignore_malformed", "true")
                .endObject()
            .endObject()
        .endObject();

        IndexService indexService = createIndex("index", Settings.EMPTY, mapping);
        MapperService mapperService = indexService.mapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("f1", "malformed")
            .endObject();

        // this should not return a field bc. f1 is in the ignored fields
        Map<String, DocumentField> fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("*", null)), false, Set.of("f1"));
        assertThat(fields.size(), equalTo(0));

        // and this should neither
        fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("*", null)), true, Set.of("f1"));
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("f1", null)), true, Set.of("f1"));
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

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "unmapped_object", true);
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, "unmapped_object.*", true);
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_object.a", "unmapped_object.b"));

        assertThat(fields.get("unmapped_object.a").getValue(), equalTo("foo"));
        assertThat(fields.get("unmapped_object.b").getValue(), equalTo("bar"));
    }

    private Map<String, DocumentField> fetchFields(
        MapperService mapperService,
        XContentBuilder source,
        String fieldPattern
    ) throws IOException {

        List<FieldAndFormat> fields = List.of(new FieldAndFormat(fieldPattern, null));
        return fetchFields(mapperService, source, fields, false);
    }

    private Map<String, DocumentField> fetchFields(
        MapperService mapperService,
        XContentBuilder source,
        String fieldPattern,
        boolean includeUnmapped
    ) throws IOException {
        List<FieldAndFormat> fields = List.of(new FieldAndFormat(fieldPattern, null));
        return fetchFields(mapperService, source, fields, includeUnmapped);
    }

    private static Map<String, DocumentField> fetchFields(
        MapperService mapperService,
        XContentBuilder source,
        List<FieldAndFormat> fields,
        boolean includeUnmapped
    ) throws IOException {

        return fetchFields(mapperService, source, fields, includeUnmapped, Set.of());
    }

    private static Map<String, DocumentField> fetchFields(
        MapperService mapperService,
        XContentBuilder source,
        List<FieldAndFormat> fields,
        boolean includeUnmapped,
        Set<String> ignoreFields
    ) throws IOException {

        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(BytesReference.bytes(source));

        FieldFetcher fieldFetcher = FieldFetcher.create(createQueryShardContext(mapperService), null, fields, includeUnmapped);
        return fieldFetcher.fetch(sourceLookup, ignoreFields);
    }

    public MapperService createMapperService() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
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
        .endObject();

        IndexService indexService = createIndex("index", Settings.EMPTY, mapping);
        return indexService.mapperService();
    }

    private static QueryShardContext createQueryShardContext(MapperService mapperService) {
        Settings settings = Settings.builder().put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid").build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        return new QueryShardContext(0, indexSettings, null, null, null, mapperService, null, null, null, null, null, null, null, null,
            null, null, null);
    }
}
