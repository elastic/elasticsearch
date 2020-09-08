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

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.ParseContext.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BooleanFieldMapperTests extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "boolean");
    }

    public void testDefaults() throws IOException {

        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", true)));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            final LeafReader leaf = reader.leaves().get(0).reader();
            // boolean fields are indexed and have doc values by default
            assertEquals(new BytesRef("T"), leaf.terms("field").iterator().next());
            SortedNumericDocValues values = leaf.getSortedNumericDocValues("field");
            assertNotNull(values);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
        });
    }

    public void testSerialization() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper mapper = defaultMapper.mappers().getMapper("field");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"boolean\"}}", Strings.toString(builder));

        // now change some parameters
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "boolean");
            b.field("doc_values", false);
            b.field("null_value", true);
        }));
        mapper = defaultMapper.mappers().getMapper("field");
        builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"boolean\",\"doc_values\":false,\"null_value\":true}}", Strings.toString(builder));
    }

    public void testParsesBooleansStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        // omit "false"/"true" here as they should still be parsed correctly
        for (String value : new String[]{"off", "no", "0", "on", "yes", "1"}) {
            MapperParsingException ex = expectThrows(MapperParsingException.class,
                () -> defaultMapper.parse(source(b -> b.field("field", value))));
            assertEquals("failed to parse field [field] of type [boolean] in document with id '1'. " +
                "Preview of field's value: '" + value + "'", ex.getMessage());
        }
    }

    public void testParsesBooleansNestedStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MapperParsingException ex = expectThrows(MapperParsingException.class,
                () -> defaultMapper.parse(source(b -> {
                    b.startObject("field");
                    {
                        b.field("inner_field", "no");
                    }
                    b.endObject();
                })));
        assertEquals("failed to parse field [field] of type [boolean] in document with id '1'. " +
            "Preview of field's value: '{inner_field=no}'", ex.getMessage());
    }

    public void testMultiFields() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "boolean");
            b.startObject("fields");
            {
                b.startObject("as_string").field("type", "keyword").endObject();
            }
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", false)));
        assertNotNull(doc.rootDoc().getField("field.as_string"));
    }

    public void testDocValues() throws Exception {

        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {
            b.startObject("bool1").field("type", "boolean").endObject();
            b.startObject("bool2");
            {
                b.field("type", "boolean");
                b.field("index", false);
            }
            b.endObject();
            b.startObject("bool3");
            {
                b.field("type", "boolean");
                b.field("index", true);
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = defaultMapper.parse(source(b -> {
            b.field("bool1", true);
            b.field("bool2", true);
            b.field("bool3", true);
        }));

        Document doc = parsedDoc.rootDoc();
        IndexableField[] fields = doc.getFields("bool1");
        assertEquals(2, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
        assertEquals(DocValuesType.SORTED_NUMERIC, fields[1].fieldType().docValuesType());
        fields = doc.getFields("bool2");
        assertEquals(1, fields.length);
        assertEquals(DocValuesType.SORTED_NUMERIC, fields[0].fieldType().docValuesType());
        fields = doc.getFields("bool3");
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
        assertEquals(DocValuesType.SORTED_NUMERIC, fields[1].fieldType().docValuesType());
    }

    public void testBoosts() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("boost", 2.0);
        }));

        MappedFieldType ft = mapperService.fieldType("field");
        assertEquals(new BoostQuery(new TermQuery(new Term("field", "T")), 2.0f), ft.termQuery("true", null));
    }

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        BooleanFieldMapper mapper = new BooleanFieldMapper.Builder("field").build(context);
        assertEquals(List.of(true), fetchSourceValue(mapper, true));
        assertEquals(List.of(false), fetchSourceValue(mapper, "false"));
        assertEquals(List.of(false), fetchSourceValue(mapper, ""));

        Map<String, Object> mapping = Map.of("type", "boolean", "null_value", true);
        BooleanFieldMapper.Builder builder = new BooleanFieldMapper.Builder("field");
        builder.parse("field", null, new HashMap<>(mapping));
        BooleanFieldMapper nullValueMapper = builder.build(context);
        assertEquals(List.of(true), fetchSourceValue(nullValueMapper, null));
    }
}
