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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

public class FieldValueRetrieverTests extends ESSingleNodeTestCase {

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
        Map<String, DocumentField> fields = retrieveFields(mapperService, source, fieldAndFormats);
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

        Map<String, DocumentField> fields = retrieveFields(mapperService, source, "float_range");
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

        Map<String, DocumentField> fields = retrieveFields(mapperService, source, "non-existent");
        assertThat(fields.size(), equalTo(0));
    }

    public void testArrayValueMappers() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("completion", "first", "second")
        .endObject();

        Map<String, DocumentField> fields = retrieveFields(mapperService, source, "completion");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("completion");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("first", "second"));

        // Test a field with multiple geo-points.
        source = XContentFactory.jsonBuilder().startObject()
            .startObject("completion")
                .array("input", "first", "second")
                .field("weight", "2.718")
            .endObject()
        .endObject();

        fields = retrieveFields(mapperService, source, "completion");
        assertThat(fields.size(), equalTo(1));

        field = fields.get("completion");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(1));

        Map<String, Object> expected = Map.of("input", List.of("first", "second"),
            "weight", "2.718");
        assertThat(field.getValues().get(0), equalTo(expected));
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

        Map<String, DocumentField> fields = retrieveFields(mapperService, source, "*field");
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

        Map<String, DocumentField> fields = retrieveFields(mapperService, source, List.of(
            new FieldAndFormat("field", null),
            new FieldAndFormat("date_field", "yyyy/MM/dd")));
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
        Map<String, DocumentField> fields = retrieveFields(mapperService, source, "field");
        DocumentField field = fields.get("field");
        assertThat(field.getValues().size(), equalTo(2));

        source = XContentFactory.jsonBuilder().startObject()
            .array("field", "really_really_long_value")
            .endObject();
        fields = retrieveFields(mapperService, source, "field");
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

        Map<String, DocumentField> fields = retrieveFields(mapperService, source, "alias_field");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("alias_field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(1));
        assertThat(field.getValues(), hasItems("value"));

        fields = retrieveFields(mapperService, source, "*field");
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

        Map<String, DocumentField> fields = retrieveFields(mapperService, source, "field.keyword");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("field.keyword");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(1));
        assertThat(field.getValues(), hasItems(42));

        fields = retrieveFields(mapperService, source, "field*");
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

        Map<String, DocumentField> fields = retrieveFields(mapperService, source, "field");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(6));
        assertThat(field.getValues(), hasItems("one", "two", "three", 1, 2, 3));
    }

    public void testObjectFields() throws IOException {
        MapperService mapperService = createMapperService();;
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "first", "second")
            .startObject("object")
                .field("field", "third")
            .endObject()
        .endObject();

        Map<String, DocumentField> fields = retrieveFields(mapperService, source, "object");
        assertFalse(fields.containsKey("object"));
    }

    private Map<String, DocumentField> retrieveFields(MapperService mapperService, XContentBuilder source, String fieldPattern) {
        List<FieldAndFormat> fields = List.of(new FieldAndFormat(fieldPattern, null));
        return retrieveFields(mapperService, source, fields);
    }

    private Map<String, DocumentField> retrieveFields(MapperService mapperService, XContentBuilder source, List<FieldAndFormat> fields) {
        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(BytesReference.bytes(source));

        FieldValueRetriever fetchFieldsLookup = FieldValueRetriever.create(mapperService, fields);
        return fetchFieldsLookup.retrieve(sourceLookup, Set.of());
    }

    public MapperService createMapperService() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("field").field("type", "keyword").endObject()
                .startObject("integer_field").field("type", "integer").endObject()
                .startObject("date_field").field("type", "date").endObject()
                .startObject("completion").field("type", "completion").endObject()
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
}
