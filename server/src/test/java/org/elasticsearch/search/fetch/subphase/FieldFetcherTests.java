/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.LongFieldScriptTests;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xcontent.ObjectPath.eval;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

public class FieldFetcherTests extends MapperServiceTestCase {

    public void testLeafValues() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .array("field", "first", "second")
            .startObject("object")
            .field("field", "third")
            .endObject()
            .endObject();

        List<FieldAndFormat> fieldAndFormats = List.of(new FieldAndFormat("field", null), new FieldAndFormat("object.field", null));
        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormats);
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
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
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
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("foo")
            .field("cat", "meow")
            .endObject()
            .field("foo.bar", "baz")
            .endObject();

        ParsedDocument doc = mapperService.documentMapper().parse(source(Strings.toString(source)));
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "foo.bar");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("foo.bar");
        assertThat(field.getValues().size(), equalTo(1));
        assertThat(field.getValue(), equalTo("baz"));

        source = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("foo")
            .field("cat", "meow")
            .endObject()
            .field("foo.cat", "miau")
            .endObject();

        doc = mapperService.documentMapper().parse(source(Strings.toString(source)));

        fields = fetchFields(mapperService, source, "foo.cat");
        assertThat(fields.size(), equalTo(1));

        field = fields.get("foo.cat");
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), containsInAnyOrder("meow", "miau"));

        source = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("foo")
            .field("cat", "meow")
            .endObject()
            .array("foo.cat", "miau", "purr")
            .endObject();

        doc = mapperService.documentMapper().parse(source(Strings.toString(source)));

        fields = fetchFields(mapperService, source, "foo.cat");
        assertThat(fields.size(), equalTo(1));

        field = fields.get("foo.cat");
        assertThat(field.getValues().size(), equalTo(3));
        assertThat(field.getValues(), containsInAnyOrder("meow", "miau", "purr"));
    }

    public void testMixedDottedObjectSyntax() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("object")
            .field("field", "value")
            .endObject()
            .field("object.field", "value2")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "*");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("object.field");
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), containsInAnyOrder("value", "value2"));
    }

    public void testNullValues() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("object")
            .field("field", "value")
            .endObject()
            .nullField("object.field")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "*");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("object.field");
        assertThat(field.getValues().size(), equalTo(1));
        assertThat(field.getValues(), containsInAnyOrder("value"));

        source = XContentFactory.jsonBuilder().startObject().array("nullable_long_field", 1, 2, 3, null, 5).endObject();
        fields = fetchFields(mapperService, source, "*");
        assertThat(fields.size(), equalTo(1));

        field = fields.get("nullable_long_field");
        assertThat(field.getValues().size(), equalTo(5));
        assertThat(field.getValues(), containsInAnyOrder(1L, 2L, 3L, 5L, 42L));
    }

    public void testNonExistentField() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "non-existent");
        assertThat(fields.size(), equalTo(0));
    }

    public void testMetadataFields() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", "value").field("_doc_count", 100).endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "_doc_count");
        assertNotNull(fields.get("_doc_count"));
        assertEquals(100, ((Integer) fields.get("_doc_count").getValue()).intValue());

        // The _type field was deprecated in 7.x and is not supported in 8.0. So the behavior
        // should be the same as if the field didn't exist.
        fields = fetchFields(mapperService, source, "_type");
        assertTrue(fields.isEmpty());

        String docId = randomAlphaOfLength(12);
        String routing = randomAlphaOfLength(12);
        long version = randomLongBetween(1, 100);
        withLuceneIndex(mapperService, iw -> {
            ParsedDocument parsedDocument = mapperService.documentMapper()
                .parse(source(docId, b -> b.field("integer_field", "value"), routing));
            parsedDocument.version().setLongValue(version);
            iw.addDocument(parsedDocument.rootDoc());
        }, iw -> {
            List<FieldAndFormat> fieldList = List.of(
                new FieldAndFormat("_id", null),
                new FieldAndFormat("_index", null),
                new FieldAndFormat("_version", null),
                new FieldAndFormat("_routing", null),
                new FieldAndFormat("_ignored", null)
            );
            FieldFetcher fieldFetcher = FieldFetcher.create(
                newSearchExecutionContext(
                    mapperService,
                    (ft, fdc) -> fieldDataLookup(fdc.sourcePathsLookup()).apply(ft, fdc.lookupSupplier(), fdc.fielddataOperation())
                ),
                fieldList
            );
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext readerContext = searcher.getIndexReader().leaves().get(0);
            fieldFetcher.setNextReader(readerContext);

            Source s = SourceProvider.fromStoredFields().getSource(readerContext, 0);

            Map<String, DocumentField> fetchedFields = fieldFetcher.fetch(s, 0);
            assertThat(fetchedFields.size(), equalTo(5));
            assertEquals(docId, fetchedFields.get("_id").getValue());
            assertEquals(routing, fetchedFields.get("_routing").getValue());
            assertEquals("test", fetchedFields.get("_index").getValue());
            assertEquals(version, ((Long) fetchedFields.get("_version").getValue()).longValue());
            assertEquals("integer_field", fetchedFields.get("_ignored").getValue());
        });

        // several other metadata fields throw exceptions via their value fetchers when trying to get them
        for (String fieldname : List.of(
            SeqNoFieldMapper.NAME,
            SourceFieldMapper.NAME,
            FieldNamesFieldMapper.NAME,
            NestedPathFieldMapper.name(IndexVersion.current())
        )) {
            expectThrows(IllegalArgumentException.class, () -> fetchFields(mapperService, source, fieldname));
        }
    }

    public void testFetchAllFields() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .startObject("object")
            .field("field", "other-value")
            .endObject()
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "*");
        assertThat(fields.size(), equalTo(2));
    }

    public void testEmptyFetch() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject();

        // make sure that an empty fetch doesn't deserialize the document
        Source s = new Source() {
            @Override
            public XContentType sourceContentType() {
                return XContentType.JSON;
            }

            @Override
            public Map<String, Object> source() {
                throw new AssertionError("Empty fetch should not deserialize the document");
            }

            @Override
            public BytesReference internalSourceRef() {
                return BytesReference.bytes(source);
            }

            @Override
            public Source filter(SourceFilter sourceFilter) {
                return sourceFilter.filterBytes(this);
            }
        };
        FieldFetcher fieldFetcher = FieldFetcher.create(newSearchExecutionContext(mapperService), List.of());
        Map<String, DocumentField> fields = fieldFetcher.fetch(s, 0);
        assertThat(fields.size(), equalTo(0));

    }

    public void testNestedArrays() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("field")
            .startArray()
            .value("first")
            .value("second")
            .endArray()
            .endArray()
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "field");
        DocumentField field = fields.get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("first", "second"));

        source = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("object")
            .startObject()
            .array("field", "first", "second")
            .endObject()
            .startObject()
            .array("field", "third")
            .endObject()
            .startObject()
            .field("field", "fourth")
            .endObject()
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

        XContentBuilder source = XContentFactory.jsonBuilder().startObject().array("geo_point", 27.1, 42.0).endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, "geo_point");
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("geo_point");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(1));

        // Test a field with multiple geo-points.
        source = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("geo_point")
            .startArray()
            .value(27.1)
            .value(42.0)
            .endArray()
            .startArray()
            .value(31.4)
            .value(42.0)
            .endArray()
            .endArray()
            .endObject();

        fields = fetchFields(mapperService, source, "geo_point");
        assertThat(fields.size(), equalTo(1));

        field = fields.get("geo_point");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
    }

    public void testGeopointArrayInObject() throws IOException {
        MapperService mapperService = createMapperService();
        {
            String source = """
                {
                    "object" : [
                        {
                            "geo_point_in_obj" : [
                                {"lat" : 42.0, "lon" : 27.1},
                                [2.1, 41.0]
                            ]
                        }
                    ]
                }
                """;

            Map<String, DocumentField> fields = fetchFields(
                mapperService,
                source,
                fieldAndFormatList("object.geo_point_in_obj", null, false)
            );
            assertThat(fields.size(), equalTo(1));

            DocumentField field = fields.get("object.geo_point_in_obj");
            assertNotNull(field);
            List<Object> values = field.getValues();
            assertThat(values.size(), equalTo(2));
            assertPoint((Map<?, ?>) values.get(0), 42.0, 27.1);
            assertPoint((Map<?, ?>) values.get(1), 41.0, 2.1);
        }
        {
            // check the same without the root field as array
            String source = """
                {
                    "object" : {
                        "geo_point_in_obj" : [
                            {"lat" : 42.0, "lon" : 27.1},
                            [2.1, 41.0]
                        ]
                    }
                }
                """;

            Map<String, DocumentField> fields = fetchFields(
                mapperService,
                source,
                fieldAndFormatList("object.geo_point_in_obj", null, false)
            );
            assertThat(fields.size(), equalTo(1));

            DocumentField field = fields.get("object.geo_point_in_obj");
            assertNotNull(field);
            List<Object> values = field.getValues();
            assertThat(values.size(), equalTo(2));
            assertPoint((Map<?, ?>) values.get(0), 42.0, 27.1);
            assertPoint((Map<?, ?>) values.get(1), 41.0, 2.1);
        }
    }

    private void assertPoint(Map<?, ?> pointMap, double lat, double lon) {
        assertEquals("Point", pointMap.get("type"));
        assertEquals(List.of(lon, lat), pointMap.get("coordinates"));
    }

    public void testDenseVectorInObject() throws IOException {
        MapperService mapperService = createMapperService();
        {
            String source = """
                {
                    "object" : [
                        {
                            "dense_vector_in_obj" : [ 1, 2, 3]
                        }
                    ]
                }
                """;

            Map<String, DocumentField> fields = fetchFields(
                mapperService,
                source,
                fieldAndFormatList("object.dense_vector_in_obj", null, false)
            );
            assertThat(fields.size(), equalTo(1));

            DocumentField field = fields.get("object.dense_vector_in_obj");
            assertNotNull(field);
            List<Object> values = field.getValues();
            assertThat(field.getValues().size(), equalTo(3));
        }
        {
            // check the same without the root field as array
            String source = """
                {
                    "object" : {
                        "dense_vector_in_obj" : [ 1, 2, 3]
                    }
                }
                """;

            Map<String, DocumentField> fields = fetchFields(
                mapperService,
                source,
                fieldAndFormatList("object.dense_vector_in_obj", null, false)
            );
            assertThat(fields.size(), equalTo(1));

            DocumentField field = fields.get("object.dense_vector_in_obj");
            assertNotNull(field);
            List<Object> values = field.getValues();
            assertThat(values.size(), equalTo(3));
        }
    }

    public void testKeywordArrayInObject() throws IOException {
        MapperService mapperService = createMapperService();

        String source = """
            {
                "object" : [
                    {
                        "field" : [ "foo", "bar"]
                    }
                ]
            }
            """;

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("object.field", null, false));
        assertThat(fields.size(), equalTo(1));

        DocumentField field = fields.get("object.field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));

        source = """
            {
                "object" : {
                    "field" : [ "foo", "bar", "baz"]
                }
            }
            """;

        fields = fetchFields(mapperService, source, fieldAndFormatList("object.field", null, false));
        assertThat(fields.size(), equalTo(1));

        field = fields.get("object.field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(3));

        // mixing array and singleton object on two separate paths
        source = """
            {
                "object" : [
                    {
                        "field" : "foo"
                    },
                    {
                        "field" : [ "bar", "baz"]
                    }
                ]
            }
            """;

        fields = fetchFields(mapperService, source, fieldAndFormatList("object.field", null, false));
        assertThat(fields.size(), equalTo(1));

        field = fields.get("object.field");
        assertNotNull(field);
        assertThat(field.getValues(), containsInAnyOrder("foo", "bar", "baz"));
    }

    public void testFieldNamesWithWildcard() throws IOException {
        MapperService mapperService = createMapperService();
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
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
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .field("date_field", "1990-12-29T00:00:00.000Z")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(
            mapperService,
            source,
            List.of(new FieldAndFormat("field", null), new FieldAndFormat("date_field", "yyyy/MM/dd"))
        );
        assertThat(fields.size(), equalTo(2));

        DocumentField field = fields.get("field");
        assertNotNull(field);

        DocumentField dateField = fields.get("date_field");
        assertNotNull(dateField);
        assertThat(dateField.getValues().size(), equalTo(1));
        assertThat(dateField.getValue(), equalTo("1990/12/29"));

        // check that badly formed dates in source are just ignored when fetching
        source = XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .array("date_field", "1990-12-29T00:00:00.000Z", "baddate", "1991-12-29T00:00:00.000Z")
            .endObject();
        DocumentField dates = fetchFields(mapperService, source, List.of(new FieldAndFormat("date_field", "yyyy/MM/dd"))).get("date_field");
        assertThat(dates.getValues().size(), equalTo(2));
        assertThat(dates, containsInAnyOrder(equalTo("1990/12/29"), equalTo("1991/12/29")));
    }

    public void testIgnoreAbove() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "keyword");
            b.field("ignore_above", 20);
        }));

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .array("field", "value", "other_value", "really_really_long_value")
            .endObject();
        Map<String, DocumentField> fields = fetchFields(mapperService, source, "field");
        DocumentField field = fields.get("field");
        assertThat(field.getValues().size(), equalTo(2));

        source = XContentFactory.jsonBuilder().startObject().array("field", "really_really_long_value").endObject();
        fields = fetchFields(mapperService, source, "field");
        assertThat(fields.get("field").getValues().size(), equalTo(0));
        assertThat(fields.get("field").getIgnoredValues().size(), equalTo(1));
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

        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject();

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

        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", 42).endObject();

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

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
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
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
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

        XContentBuilder source = XContentFactory.jsonBuilder().startObject().array("field", "some text").endObject();

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

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_f*", null, true));
        assertThat(fields.size(), equalTo(3));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_f1", "unmapped_f2", "unmapped_f3"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("un*1", null, true));
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_f1"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("*thing*", null, true));
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("something_else"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("null*", null, true));
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("object.a", null, true));
        assertThat(fields.size(), equalTo(1));
        assertEquals("foo", fields.get("object.a").getValues().get(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("object.b", null, true));
        assertThat(fields.size(), equalTo(1));
        assertEquals("bar", fields.get("object.b").getValues().get(0));
    }

    public void testSimpleUnmappedArray() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject().array("unmapped_field", "foo", "bar").endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field", null, true));
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_field"));
        DocumentField field = fields.get("unmapped_field");

        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("foo", "bar"));
    }

    public void testSimpleUnmappedArrayWithObjects() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("unmapped_field")
            .startObject()
            .field("f1", "a")
            .endObject()
            .startObject()
            .field("f2", "b")
            .endObject()
            .endArray()
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field", null, true));
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field.f*", null, true));
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.get("unmapped_field.f1").getValue(), equalTo("a"));
        assertThat(fields.get("unmapped_field.f2").getValue(), equalTo("b"));

        source = XContentFactory.jsonBuilder()
            .startObject()
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

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field.f1", null, true));
        assertThat(fields.size(), equalTo(1));
        DocumentField field = fields.get("unmapped_field.f1");
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("a", "b"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field.f2", null, true));
        assertThat(fields.size(), equalTo(1));
        field = fields.get("unmapped_field.f2");
        assertThat(field.getValues().size(), equalTo(4));
        assertThat(field.getValues(), hasItems(1, 2, 3, 4));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_field.f3", null, true));
        assertThat(fields.size(), equalTo(1));
        field = fields.get("unmapped_field.f3");
        assertThat(field.getValues().size(), equalTo(3));
        assertThat(field.getValues(), hasItems(1, 2, "foo"));
    }

    public void testNestedFields() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("f1")
            .field("type", "keyword")
            .endObject()
            .startObject("obj")
            .field("type", "nested")
            .startObject("properties")
            .startObject("f2")
            .field("type", "keyword")
            .endObject()
            .startObject("f3")
            .field("type", "keyword")
            .endObject()
            .startObject("inner_nested")
            .field("type", "nested")
            .startObject("properties")
            .startObject("f4")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createMapperService(mapping);

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("f1", "value1")
            .startArray("obj")
            .startObject()
            .field("f2", "value2a")
            .startObject("inner_nested")
            .field("f4", "value4a")
            .endObject()
            .endObject()
            .startObject()
            .field("f2", "value2b")
            .field("f3", "value3b")
            .startObject("inner_nested")
            .field("f4", "value4b")
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("*", null, false));
        assertThat(fields.values(), hasSize(2));
        assertThat(fields.keySet(), containsInAnyOrder("f1", "obj"));
        assertEquals("value1", fields.get("f1").getValue());
        List<Object> obj = fields.get("obj").getValues();
        assertEquals(2, obj.size());
        Object obj0 = obj.get(0);
        assertEquals(2, ((Map<?, ?>) obj0).size());
        assertEquals("value2a", eval("f2.0", obj0));
        assertNull(eval("f3", obj0));
        assertEquals("value4a", eval("inner_nested.0.f4.0", obj0));

        Object obj1 = obj.get(1);
        assertEquals(3, ((Map<?, ?>) obj1).size());
        assertEquals("value2b", eval("f2.0", obj1));
        assertEquals("value3b", eval("f3.0", obj1));
        assertEquals("value4b", eval("inner_nested.0.f4.0", obj1));

        fields = fetchFields(mapperService, source, fieldAndFormatList("obj*", null, false));
        assertEquals(1, fields.size());
        assertThat(fields.keySet(), containsInAnyOrder("obj"));
        obj = fields.get("obj").getValues();
        assertEquals(2, ((Map<?, ?>) obj.get(0)).size());
        obj0 = obj.get(0);
        assertEquals(2, ((Map<?, ?>) obj0).size());
        assertEquals("value2a", eval("f2.0", obj0));
        assertNull(eval("f3", obj0));
        assertEquals("value4a", eval("inner_nested.0.f4.0", obj0));

        obj1 = obj.get(1);
        assertEquals(3, ((Map<?, ?>) obj1).size());
        assertEquals("value2b", eval("f2.0", obj1));
        assertEquals("value3b", eval("f3.0", obj1));
        assertEquals("value4b", eval("inner_nested.0.f4.0", obj1));

        fields = fetchFields(mapperService, source, fieldAndFormatList("obj*", null, false));
        assertEquals(1, fields.size());
        assertThat(fields.keySet(), containsInAnyOrder("obj"));
        obj = fields.get("obj").getValues();
        assertEquals(2, obj.size());
        obj0 = obj.get(0);
        assertEquals("value4a", eval("inner_nested.0.f4.0", obj0));
        obj1 = obj.get(1);
        assertEquals("value4b", eval("inner_nested.0.f4.0", obj1));
    }

    public void testDoublyNestedWithMultifields() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
              "user" : {
                "type" : "nested",
                "properties" : {
                  "first" : { "type" : "keyword" },
                  "last" : { "type" : "text", "fields" : { "keyword" : { "type" : "keyword" } } },
                  "address" : {
                    "type" : "nested",
                    "properties" : {
                      "city" : { "type" : "keyword" },
                      "zip" : { "type" : "keyword" }
                    }
                  }
                }
              }
            }}}
            """);

        String source = """
            { "user" : [ { "first" : "John",
                           "last" : "Smith",
                           "address" : [ { "city" : "Berlin", "zip" : "1111" }, { "city" : "Ottawa", "zip" : "1111" } ] } ] }
            """;

        var results = fetchFields(mapperService, source, fieldAndFormatList("*", null, false));
        DocumentField user = results.get("user");
        Map<?, ?> fields = (Map<?, ?>) user.getValues().get(0);
        assertThat(fields.keySet(), hasSize(4));
    }

    public void testNestedUnmappedFields() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
              "id" : { "type" : "keyword" },
              "user_account" : {
                "type" : "nested",
                "properties" : {
                  "details" : {
                    "type" : "object",
                    "enabled" : false
                  }
                }
              },
              "user" : {
                "type" : "nested",
                "properties" : {
                  "first" : { "type" : "keyword" },
                  "address" : {
                    "type" : "object",
                    "enabled" : false
                  }
                }
              }
            }}}
            """);
        String source = """
            { "id" : "1", "user" : { "first" : "John", "address" : { "city" : "Toronto" } }, "user_account" : { "details" : { "id" : 2 } } }
            """;

        var results = fetchFields(mapperService, source, fieldAndFormatList("*", null, true));
        assertNotNull(results.get("user_account"));
        assertEquals("2", eval(new String[] { "details.id", "0" }, results.get("user_account").getValues().get(0)).toString());

        results = fetchFields(mapperService, source, fieldAndFormatList("user.address.*", null, true));
        assertNotNull(results.get("user"));
        assertNull(eval("first", results.get("user").getValues().get(0)));
        assertEquals("Toronto", eval(new String[] { "address.city", "0" }, results.get("user").getValues().get(0)));
    }

    public void testNestedGrouping() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties": {
                                 "age": {
                                   "type": "integer"
                                 },
                                 "loan": {
                                   "type": "keyword"
                                 },
                                 "marital": {
                                   "type": "keyword"
                                 },
                                 "ml": {
                                   "properties": {
                                     "feature_importance": {
                                       "type": "nested",
                                       "dynamic": "false",
                                       "properties": {
                                         "classes": {
                                           "type": "nested",
                                           "dynamic": "false",
                                           "properties": {
                                             "class_name": {
                                               "type": "keyword"
                                             },
                                             "importance": {
                                               "type": "double"
                                             }
                                           }
                                         },
                                         "feature_name": {
                                           "type": "keyword"
                                         }
                                       }
                                     },
                                     "is_training": {
                                       "type": "boolean"
                                     },
                                     "prediction_probability": {
                                       "type": "double"
                                     },
                                     "prediction_score": {
                                       "type": "double"
                                     },
                                     "top_classes": {
                                       "type": "nested",
                                       "properties": {
                                         "class_name": {
                                           "type": "keyword"
                                         },
                                         "class_probability": {
                                           "type": "double"
                                         },
                                         "class_score": {
                                           "type": "double"
                                         }
                                       }
                                     },
                                     "y_prediction": {
                                       "type": "keyword"
                                     }
                                   }
                                 }
            }}}
            """);

        String source = """
            {
                       "loan": "no",
                       "ml__incremental_id": 26513,
                       "ml": {
                         "y_prediction": "no",
                         "top_classes": [
                           {
                             "class_name": "no",
                             "class_probability": 0.978734716971892,
                             "class_score": 0.17187636491006547
                           },
                           {
                             "class_name": "yes",
                             "class_probability": 0.02126528302810799,
                             "class_score": 0.02126528302810799
                           }
                         ],
                         "prediction_probability": 0.978734716971892,
                         "prediction_score": 0.17187636491006547,
                         "feature_importance": [
                           {
                             "feature_name": "duration",
                             "classes": [
                               {
                                 "class_name": "no",
                                 "importance": 0.4360196873080361
                               },
                               {
                                 "class_name": "yes",
                                 "importance": -0.4360196873080361
                               }
                             ]
                           },
                           {
                             "feature_name": "housing",
                             "classes": [
                               {
                                 "class_name": "no",
                                 "importance": 0.2993353230710585
                               },
                               {
                                 "class_name": "yes",
                                 "importance": -0.2993353230710585
                               }
                             ]
                           }
                         ],
                         "is_training": false
                       }
                     }
            """;

        var results = fetchFields(mapperService, source, fieldAndFormatList("*", null, false));
        SearchHit searchHit = SearchHit.unpooled(0);
        searchHit.addDocumentFields(results, Map.of());
        assertThat(Strings.toString(searchHit), containsString("\"ml.top_classes\":"));
    }

    public void testNestedIOOB() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
              "nested_field" : {
                "type" : "nested",
                "properties" : {
                  "file" : { "type" : "keyword" }
                }
              }
            }}}
            """);
        String source = """
            { "nested_field" : { "file" : "somefile.txt" } }
            """;
        var results = fetchFields(
            mapperService,
            source,
            List.of(new FieldAndFormat("file", null, true), new FieldAndFormat("*", null, true))
        );
        assertThat(results.keySet(), hasSize(1));

        results = fetchFields(mapperService, source, fieldAndFormatList("nested_field.file", null, true));
        assertThat(results.keySet(), hasSize(1));
    }

    @SuppressWarnings("unchecked")
    public void testFlattenedField() throws IOException {
        XContentBuilder mapping = mapping(b -> b.startObject("flat").field("type", "flattened").endObject());
        MapperService mapperService = createMapperService(mapping);

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("flat")
            .field("f1", "value1")
            .field("f2", 1)
            .endObject()
            .endObject();

        // requesting via wildcard should retrieve the root field as a structured map
        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("*", null, false));
        assertEquals(1, fields.size());
        assertThat(fields.keySet(), containsInAnyOrder("flat"));
        Map<String, Object> flattenedValue = (Map<String, Object>) fields.get("flat").getValue();
        assertThat(flattenedValue.keySet(), containsInAnyOrder("f1", "f2"));
        assertEquals("value1", flattenedValue.get("f1"));
        assertEquals(1, flattenedValue.get("f2"));

        // direct retrieval of subfield is possible
        List<FieldAndFormat> fieldAndFormatList = new ArrayList<>();
        fieldAndFormatList.add(new FieldAndFormat("flat.f1", null));
        fields = fetchFields(mapperService, source, fieldAndFormatList);
        assertEquals(1, fields.size());
        assertThat(fields.keySet(), containsInAnyOrder("flat.f1"));
        assertThat(fields.get("flat.f1").getValue(), equalTo("value1"));

        // direct retrieval of root field and subfield is possible
        fieldAndFormatList.add(new FieldAndFormat("*", null));
        fields = fetchFields(mapperService, source, fieldAndFormatList);
        assertEquals(2, fields.size());
        assertThat(fields.keySet(), containsInAnyOrder("flat", "flat.f1"));
        flattenedValue = (Map<String, Object>) fields.get("flat").getValue();
        assertThat(flattenedValue.keySet(), containsInAnyOrder("f1", "f2"));
        assertEquals("value1", flattenedValue.get("f1"));
        assertEquals(1, flattenedValue.get("f2"));
        assertThat(fields.get("flat.f1").getValue(), equalTo("value1"));

        // retrieval of subfield with wildcard is not possible
        fields = fetchFields(mapperService, source, fieldAndFormatList("flat.f*", null, false));
        assertEquals(0, fields.size());

        // retrieval of non-existing subfield returns empty result
        fields = fetchFields(mapperService, source, fieldAndFormatList("flat.baz", null, false));
        assertEquals(0, fields.size());
    }

    public void testUnmappedFieldsInsideObject() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("obj")
            .field("type", "object")
            .field("dynamic", "false")
            .startObject("properties")
            .startObject("f1")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createMapperService(mapping);

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("obj.f1", "value1")
            .field("obj.f2", "unmapped_value_f2")
            .field("obj.innerObj.f3", "unmapped_value_f3")
            .field("obj.innerObj.f4", "unmapped_value_f4")
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("*", null, false));

        // without unmapped fields this should only return "obj.f1"
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.keySet(), containsInAnyOrder("obj.f1"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("*", null, true));
        assertThat(fields.size(), equalTo(4));
        assertThat(fields.keySet(), containsInAnyOrder("obj.f1", "obj.f2", "obj.innerObj.f3", "obj.innerObj.f4"));
    }

    public void testUnmappedFieldsInsideDisabledObject() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
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

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("obj")
            .value("string_value")
            .startObject()
            .field("a", "b")
            .endObject()
            .startArray()
            .value(1)
            .value(2)
            .value(3)
            .endArray()
            .endArray()
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("*", null, false));
        // without unmapped fields this should return nothing
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("*", null, true));
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
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
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

        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("f1", "malformed").endObject();

        // this should not return a field bc. f1 is malformed
        Map<String, DocumentField> fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("*", null, true)));
        assertThat(fields.get("f1").getValues().size(), equalTo(0));
        assertThat(fields.get("f1").getIgnoredValues().size(), equalTo(1));

        // and this should neither
        fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("*", null, true)));
        assertThat(fields.get("f1").getValues().size(), equalTo(0));
        assertThat(fields.get("f1").getIgnoredValues().size(), equalTo(1));

        fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("f1", null, true)));
        assertThat(fields.get("f1").getValues().size(), equalTo(0));
        assertThat(fields.get("f1").getIgnoredValues().size(), equalTo(1));

        // check this also does not overwrite with arrays
        source = XContentFactory.jsonBuilder().startObject().array("f1", "malformed").endObject();

        fields = fetchFields(mapperService, source, List.of(new FieldAndFormat("f1", null, true)));
        assertThat(fields.get("f1").getValues().size(), equalTo(0));
        assertThat(fields.get("f1").getIgnoredValues().size(), equalTo(1));
    }

    public void testUnmappedFieldsWildcard() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("unmapped_object")
            .field("a", "foo")
            .field("b", "bar")
            .endObject()
            .endObject();

        Map<String, DocumentField> fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_object", null, true));
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmap*object", null, true));
        assertThat(fields.size(), equalTo(0));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_object.*", null, true));
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.keySet(), containsInAnyOrder("unmapped_object.a", "unmapped_object.b"));

        assertThat(fields.get("unmapped_object.a").getValue(), equalTo("foo"));
        assertThat(fields.get("unmapped_object.b").getValue(), equalTo("bar"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_object.a", null, true));
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get("unmapped_object.a").getValue(), equalTo("foo"));

        fields = fetchFields(mapperService, source, fieldAndFormatList("unmapped_object.b", null, true));
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get("unmapped_object.b").getValue(), equalTo("bar"));
    }

    public void testLastFormatWins() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("date_field")
            .value("2011-11-11T11:11:11")
            .value("2012-12-12T12:12:12")
            .endArray()
            .endObject();

        List<FieldAndFormat> ff = new ArrayList<>();
        ff.add(new FieldAndFormat("date_field", "year", false));
        Map<String, DocumentField> fields = fetchFields(mapperService, source, ff);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get("date_field").getValues().size(), equalTo(2));
        assertThat(fields.get("date_field").getValues().get(0), equalTo("2011"));
        assertThat(fields.get("date_field").getValues().get(1), equalTo("2012"));

        ff.add(new FieldAndFormat("date_field", "hour", false));
        fields = fetchFields(mapperService, source, ff);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get("date_field").getValues().size(), equalTo(2));
        assertThat(fields.get("date_field").getValues().get(0), equalTo("11"));
        assertThat(fields.get("date_field").getValues().get(1), equalTo("12"));
    }

    public void testNestedPrefix() throws IOException {
        String mapping = """
            {
              "_doc": {
                "properties" : {
                  "foo" : {
                    "type" : "nested",
                    "properties" : {
                      "nested_field" : {
                        "type" : "keyword"
                      }
                    }
                  },
                  "foo_bar" : {
                    "type" : "double"
                  }
                }
              }
            }
            """;
        MapperService mapperService = createMapperService(mapping);
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("foo_bar", 3.1).endObject();
        // the field should be returned
        Map<String, DocumentField> fields = fetchFields(mapperService, source, "foo_bar");
        assertThat(fields.get("foo_bar").getValues().size(), equalTo(1));
    }

    /**
     * Field patterns retrieved with "include_unmapped" use an automaton with a maximal allowed size internally.
     * This test checks we have a bound in place to avoid misuse of this with exceptionally large field patterns
     */
    public void testTooManyUnmappedFieldWildcardPattern() throws IOException {
        MapperService mapperService = createMapperService();

        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("a", "foo").endObject();

        List<FieldAndFormat> fieldAndFormatList = new ArrayList<>(8_000);
        for (int i = 0; i < 8000; i++) {
            fieldAndFormatList.add(new FieldAndFormat(randomAlphaOfLength(150) + "*", null, true));
        }
        expectThrows(TooComplexToDeterminizeException.class, () -> fetchFields(mapperService, source, fieldAndFormatList));
    }

    public void testFetchFromSourceWithSourceDisabled() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("_source").field("enabled", false).endObject();
                mapping.startObject("properties");
                {
                    mapping.startObject("field").field("type", "keyword").endObject();
                    mapping.startObject("location").field("type", "geo_point").endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        MapperService mapperService = createMapperService(mapping);
        {
            Map<String, DocumentField> fields = fetchFields(mapperService, null, "field");
            assertEquals(0, fields.size());
        }
        {
            Map<String, DocumentField> fields = fetchFields(mapperService, null, "location");
            assertEquals(0, fields.size());
        }
    }

    public void testFetchRuntimeFieldWithSourceDisabled() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("_source").field("enabled", false).endObject();
                mapping.startObject("runtime");
                {
                    mapping.startObject("runtime_field").field("type", "long").field("script", "emit(1);").endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        MapperService mapperService = createMapperService(mapping);
        SearchExecutionContext searchExecutionContext = newSearchExecutionContext(
            mapperService,
            (ft, fdc) -> fieldDataLookup(fdc.sourcePathsLookup()).apply(ft, fdc.lookupSupplier(), fdc.fielddataOperation())
        );
        withLuceneIndex(mapperService, iw -> iw.addDocument(new LuceneDocument()), iw -> {
            FieldFetcher fieldFetcher = FieldFetcher.create(searchExecutionContext, fieldAndFormatList("runtime_field", null, false));
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext readerContext = searcher.getIndexReader().leaves().get(0);
            fieldFetcher.setNextReader(readerContext);
            Source source = SourceProvider.fromStoredFields().getSource(readerContext, 0);
            Map<String, DocumentField> fields = fieldFetcher.fetch(source, 0);
            assertEquals(1, fields.size());
            DocumentField field = fields.get("runtime_field");
            assertEquals(1L, (long) field.getValue());
        });
    }

    public void testFetchMetadataFieldWithSourceDisabled() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("_source").field("enabled", false).endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        MapperService mapperService = createMapperService(mapping);
        SearchExecutionContext searchExecutionContext = newSearchExecutionContext(
            mapperService,
            (ft, fdc) -> fieldDataLookup(fdc.sourcePathsLookup()).apply(ft, fdc.lookupSupplier(), fdc.fielddataOperation())
        );
        withLuceneIndex(mapperService, iw -> {
            ParsedDocument parsedDocument = mapperService.documentMapper().parse(source("{}"));
            iw.addDocument(parsedDocument.rootDoc());
        }, iw -> {
            FieldFetcher fieldFetcher = FieldFetcher.create(searchExecutionContext, fieldAndFormatList("_id", null, false));
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext readerContext = searcher.getIndexReader().leaves().get(0);
            fieldFetcher.setNextReader(readerContext);
            Map<String, DocumentField> fields = fieldFetcher.fetch(Source.empty(XContentType.JSON), 0);
            assertEquals(1, fields.size());
            DocumentField field = fields.get("_id");
            assertEquals("1", field.getValue());
        });
    }

    public void testStoredFieldsSpec() throws IOException {
        List<FieldAndFormat> fields = List.of(new FieldAndFormat("field", null));
        FieldFetcher fieldFetcher = FieldFetcher.create(newSearchExecutionContext(createMapperService()), fields);
        assertEquals(StoredFieldsSpec.NEEDS_SOURCE, fieldFetcher.storedFieldsSpec());
    }

    private List<FieldAndFormat> fieldAndFormatList(String name, String format, boolean includeUnmapped) {
        return Collections.singletonList(new FieldAndFormat(name, format, includeUnmapped));
    }

    private Map<String, DocumentField> fetchFields(MapperService mapperService, XContentBuilder source, String fieldPattern)
        throws IOException {
        return fetchFields(mapperService, source, fieldAndFormatList(fieldPattern, null, false));
    }

    private static Map<String, DocumentField> fetchFields(MapperService mapperService, XContentBuilder source, List<FieldAndFormat> fields)
        throws IOException {
        Source s = source == null
            ? Source.empty(randomFrom(XContentType.values()))
            : Source.fromBytes(BytesReference.bytes(source), source.contentType());
        FieldFetcher fieldFetcher = FieldFetcher.create(newSearchExecutionContext(mapperService), fields);
        return fieldFetcher.fetch(s, -1);
    }

    private static Map<String, DocumentField> fetchFields(MapperService mapperService, String source, List<FieldAndFormat> fields)
        throws IOException {
        FieldFetcher fieldFetcher = FieldFetcher.create(newSearchExecutionContext(mapperService), fields);
        return fieldFetcher.fetch(Source.fromBytes(new BytesArray(source), XContentType.JSON), -1);
    }

    public MapperService createMapperService() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .endObject()
            .startObject("integer_field")
            .field("type", "integer")
            .field("ignore_malformed", "true")
            .endObject()
            .startObject("date_field")
            .field("type", "date")
            .endObject()
            .startObject("geo_point")
            .field("type", "geo_point")
            .endObject()
            .startObject("float_range")
            .field("type", "float_range")
            .endObject()
            .startObject("nullable_long_field")
            .field("type", "long")
            .field("null_value", 42)
            .endObject()
            .startObject("object")
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .endObject()
            .startObject("geo_point_in_obj")
            .field("type", "geo_point")
            .endObject()
            .startObject("dense_vector_in_obj")
            .field("type", "dense_vector")
            .field("dims", 3)
            .endObject()
            .endObject()
            .endObject()
            .startObject("field_that_does_not_match")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        return createMapperService(mapping);
    }

    private static SearchExecutionContext newSearchExecutionContext(MapperService mapperService) {
        return newSearchExecutionContext(mapperService, null);
    }

    private static SearchExecutionContext newSearchExecutionContext(
        MapperService mapperService,
        BiFunction<MappedFieldType, FieldDataContext, IndexFieldData<?>> indexFieldDataLookup
    ) {
        Settings settings = indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, "uuid").build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("test").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        return new SearchExecutionContext(
            0,
            0,
            indexSettings,
            null,
            indexFieldDataLookup,
            mapperService,
            mapperService.mappingLookup(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            emptyMap(),
            MapperMetrics.NOOP
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        return (T) LongFieldScriptTests.DUMMY;
    }
}
