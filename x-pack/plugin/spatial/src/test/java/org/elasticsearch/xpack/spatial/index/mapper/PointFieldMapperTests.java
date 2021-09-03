/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class PointFieldMapperTests extends CartesianFieldMapperTests {

    @Override
    protected String getFieldName() {
        return "point";
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> {
            PointFieldMapper gpfm = (PointFieldMapper) m;
            assertTrue(gpfm.ignoreMalformed());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            PointFieldMapper gpfm = (PointFieldMapper) m;
            assertFalse(gpfm.ignoreZValue());
        });
    }

    public void testValuesStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("store", true);
        }));
        SourceToParse sourceToParse = source(b -> b.startObject(FIELD_NAME).field("x", 2000.1).field("y", 305.6).endObject());
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
    }

    public void testArrayValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("doc_values", false);
            b.field("store", true);
        }));

        SourceToParse sourceToParse = source(b ->
             b.startArray(FIELD_NAME)
            .startObject().field("x", 1.2).field("y", 1.3).endObject()
            .startObject().field("x", 1.4).field("y", 1.5).endObject()
            .endArray());
        ParsedDocument doc = mapper.parse(sourceToParse);

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields(FIELD_NAME), notNullValue());
        assertThat(doc.rootDoc().getFields(FIELD_NAME).length, equalTo(4));
    }

    public void testXYInOneValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse sourceToParse = source(b -> b.field(FIELD_NAME, "1.2,1.3"));
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
    }


    public void testInOneValueStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("store", true);
        }));
        SourceToParse sourceToParse = source(b -> b.field(FIELD_NAME, "1.2,1.3"));
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
    }

    public void testXYInOneValueArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("doc_values", false);
            b.field("store", true);
        }));
        SourceToParse sourceToParse = source(b -> b.startArray(FIELD_NAME).value("1.2,1.3").value("1.4,1.5").endArray());
        ParsedDocument doc = mapper.parse(sourceToParse);

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields(FIELD_NAME), notNullValue());
        assertThat(doc.rootDoc().getFields(FIELD_NAME).length, equalTo(4));
    }

    public void testArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse sourceToParse = source(b -> b.startArray(FIELD_NAME).value(1.3).value(1.2).endArray());
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
    }

    public void testArrayDynamic() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startArray("dynamic_templates");
        {
            mapping.startObject().startObject("point");
            {
                mapping.field("match", "point*");
                mapping.startObject("mapping").field("type", "point").endObject();
            }
            mapping.endObject().endObject();
        }
        mapping.endArray().endObject().endObject();
        DocumentMapper mapper = createDocumentMapper(mapping);

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("point").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testArrayStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("store", true);
        }));
        SourceToParse sourceToParse = source(b -> b.startArray(FIELD_NAME).value(1.3).value(1.2).endArray());
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
        assertThat(doc.rootDoc().getFields(FIELD_NAME).length, equalTo(3));
    }

    public void testArrayArrayStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("store", true);
            b.field("doc_values", false);
        }));

        SourceToParse sourceToParse = source(b ->
            b.startArray(FIELD_NAME)
                .startArray().value(1.3).value(1.2).endArray()
                .startArray().value(1.5).value(1.4).endArray()
                .endArray());
        ParsedDocument doc = mapper.parse(sourceToParse);

        assertThat(doc.rootDoc().getFields(FIELD_NAME), notNullValue());
        assertThat(doc.rootDoc().getFields(FIELD_NAME).length, equalTo(4));
    }

    public void testNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("null_value", "1,2");
        }));

        Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(PointFieldMapper.class));

        Object nullValue = ((PointFieldMapper) fieldMapper).getNullValue();
        assertThat(nullValue, equalTo(new CartesianPoint(1, 2)));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField(FIELD_NAME)));

        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
        BytesRef defaultValue = doc.rootDoc().getBinaryValue(FIELD_NAME);

        doc = mapper.parse(source(b -> b.field(FIELD_NAME, "1, 2")));
        // Shouldn't matter if we specify the value explicitly or use null value
        assertThat(defaultValue, equalTo(doc.rootDoc().getBinaryValue(FIELD_NAME)));

        doc = mapper.parse(source(b -> b.field(FIELD_NAME, "3, 4")));
        // Shouldn't matter if we specify the value explicitly or use null value
        assertThat(defaultValue, not(equalTo(doc.rootDoc().getBinaryValue(FIELD_NAME))));
    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        {
            // explicit true accept_z_value test
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
                b.field("type", "point");
                b.field("ignore_z_value", true);
            }));
            Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
            assertThat(fieldMapper, instanceOf(PointFieldMapper.class));
            boolean ignoreZValue = ((PointFieldMapper) fieldMapper).ignoreZValue();
            assertThat(ignoreZValue, equalTo(true));
        }
        {
            // explicit false accept_z_value test
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
                b.field("type", "point");
                b.field("ignore_z_value", false);
            }));
            Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
            assertThat(fieldMapper, instanceOf(PointFieldMapper.class));
            boolean ignoreZValue = ((PointFieldMapper) fieldMapper).ignoreZValue();
            assertThat(ignoreZValue, equalTo(false));
        }
    }

    public void testMultiFieldsDeprecationWarning() throws Exception {
        createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.startObject("fields");
            b.startObject("keyword").field("type", "keyword").endObject();
            b.endObject();
        }));
        assertWarnings("Adding multifields to [point] mappers has no effect and will be forbidden in future");
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }
}
