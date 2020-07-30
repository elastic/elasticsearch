/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.AbstractPointGeometryFieldMapper.Names.IGNORE_Z_VALUE;
import static org.elasticsearch.index.mapper.AbstractPointGeometryFieldMapper.Names.NULL_VALUE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class PointFieldMapperTests extends CartesianFieldMapperTests {

    @Override
    protected XContentBuilder createDefaultMapping(String fieldName,
                                                   boolean ignored_malformed,
                                                   boolean ignoreZValue) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject(fieldName).field("type", "point");
        if (ignored_malformed || randomBoolean()) {
            xContentBuilder.field(PointFieldMapper.Names.IGNORE_MALFORMED.getPreferredName(), ignored_malformed);
        }
        if (ignoreZValue == false || randomBoolean()) {
            xContentBuilder.field(PointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName(), ignoreZValue);
        }
        return xContentBuilder.endObject().endObject().endObject().endObject();
    }

    public void testValuesStored() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.field("store", true).endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("point").field("x", 2000.1).field("y", 305.6).endObject()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testArrayValues() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point").field("doc_values", false);
        String mapping = Strings.toString(xContentBuilder.field("store", true).endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point")
                        .startObject().field("x", 1.2).field("y", 1.3).endObject()
                        .startObject().field("x", 1.4).field("y", 1.5).endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("point"), notNullValue());
        assertThat(doc.rootDoc().getFields("point").length, equalTo(4));
    }

    public void testLatLonInOneValue() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("point", "1.2,1.3")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testInOneValueStored() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.field("store", true).endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("point", "1.2,1.3")
                        .endObject()),
                XContentType.JSON));
        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testLatLonInOneValueArray() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point").field("doc_values", false);
        String mapping = Strings.toString(xContentBuilder.field("store", true).endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point")
                        .value("1.2,1.3")
                        .value("1.4,1.5")
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("point"), notNullValue());
        assertThat(doc.rootDoc().getFields("point").length, equalTo(4));
    }

    public void testArray() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point").value(1.3).value(1.2).endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testArrayDynamic() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startArray("dynamic_templates").startObject().startObject("point").field("match", "point*")
            .startObject("mapping").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endArray().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point").value(1.3).value(1.2).endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testArrayStored() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.field("store", true).endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point").value(1.3).value(1.2).endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
        assertThat(doc.rootDoc().getFields("point").length, equalTo(3));
    }

    public void testArrayArrayStored() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.field("store", true)
            .field("doc_values", false).endObject().endObject()
            .endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point")
                        .startArray().value(1.3).value(1.2).endArray()
                        .startArray().value(1.5).value(1.4).endArray()
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getFields("point"), notNullValue());
        assertThat(doc.rootDoc().getFields("point").length, CoreMatchers.equalTo(4));
    }

    public void testNullValue() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("location")
            .field("type", "point")
            .field(NULL_VALUE.getPreferredName(), "1,2")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(PointFieldMapper.class));

        Object nullValue = ((PointFieldMapper) fieldMapper).getNullValue();
        assertThat(nullValue, equalTo(new CartesianPoint(1, 2)));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .nullField("location")
                    .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("location"), notNullValue());
        BytesRef defaultValue = doc.rootDoc().getBinaryValue("location");

        doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("location", "1, 2")
                    .endObject()),
            XContentType.JSON));
        // Shouldn't matter if we specify the value explicitly or use null value
        assertThat(defaultValue, equalTo(doc.rootDoc().getBinaryValue("location")));

        doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("location", "3, 4")
                    .endObject()),
            XContentType.JSON));
        // Shouldn't matter if we specify the value explicitly or use null value
        assertThat(defaultValue, not(equalTo(doc.rootDoc().getBinaryValue("location"))));
    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "point")
            .field(IGNORE_Z_VALUE.getPreferredName(), "true")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(PointFieldMapper.class));

        boolean ignoreZValue = ((PointFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "point")
            .field(IGNORE_Z_VALUE.getPreferredName(), "false")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(PointFieldMapper.class));

        ignoreZValue = ((PointFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(false));
    }

    public void testParseSourceValue() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        AbstractGeometryFieldMapper<?, ?> mapper = new PointFieldMapper.Builder("field").build(context);
        SourceLookup sourceLookup = new SourceLookup();

        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(42.0, 27.1));
        String wktPoint = "POINT (42.0 27.1)";
        Map<String, Object> otherJsonPoint = Map.of("type", "Point", "coordinates", List.of(30.0, 50.0));
        String otherWktPoint = "POINT (30.0 50.0)";

        // Test a single point in [x, y] array format.
        sourceLookup.setSource(Collections.singletonMap("field", List.of(42.0, 27.1)));
        assertEquals(List.of(jsonPoint), mapper.lookupValues(sourceLookup, null));
        assertEquals(List.of(wktPoint), mapper.lookupValues(sourceLookup, "wkt"));

        // Test a single point in "x, y" string format.
        sourceLookup.setSource(Collections.singletonMap("field", "42.0,27.1"));
        assertEquals(List.of(jsonPoint), mapper.lookupValues(sourceLookup, null));
        assertEquals(List.of(wktPoint), mapper.lookupValues(sourceLookup, "wkt"));

        // Test a list of points in [x, y] array format.
        sourceLookup.setSource(Collections.singletonMap("field", List.of(List.of(42.0, 27.1), List.of(30.0, 50.0))));
        assertEquals(List.of(jsonPoint, otherJsonPoint), mapper.lookupValues(sourceLookup, null));
        assertEquals(List.of(wktPoint, otherWktPoint), mapper.lookupValues(sourceLookup, "wkt"));

        // Test a single point in well-known text format.
        sourceLookup.setSource(Collections.singletonMap("field", "POINT (42.0 27.1)"));
        assertEquals(List.of(jsonPoint), mapper.lookupValues(sourceLookup, null));
        assertEquals(List.of(wktPoint), mapper.lookupValues(sourceLookup, "wkt"));
    }
}
