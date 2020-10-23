/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/** testing for {@link org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper} */
public class ShapeFieldMapperTests extends CartesianFieldMapperTests {

    @Override
    protected XContentBuilder createDefaultMapping(String fieldName,
                                                   boolean ignored_malformed,
                                                   boolean ignoreZValue) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject(fieldName).field("type", "shape");
        if (ignored_malformed || randomBoolean()) {
            xContentBuilder.field("ignore_malformed", ignored_malformed);
        }
        if (ignoreZValue == false || randomBoolean()) {
            xContentBuilder.field("ignore_z_value", ignoreZValue);
        }
        return xContentBuilder.endObject().endObject().endObject().endObject();
    }

    public void testDefaultConfiguration() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "shape")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ShapeFieldMapper shapeFieldMapper = (ShapeFieldMapper) fieldMapper;
        assertThat(shapeFieldMapper.fieldType().orientation(),
            equalTo(ShapeBuilder.Orientation.RIGHT));
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "shape")
            .field("orientation", "left")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ShapeBuilder.Orientation orientation = ((ShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // explicit right orientation test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "shape")
            .field("orientation", "right")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        orientation = ((ShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
    }

    /**
     * Test that coerce parameter correctly parses
     */
    public void testCoerceParsing() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "shape")
            .field("coerce", "true")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        boolean coerce = ((ShapeFieldMapper)fieldMapper).coerce();
        assertThat(coerce, equalTo(true));

        // explicit false coerce test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "shape")
            .field("coerce", "false")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        coerce = ((ShapeFieldMapper)fieldMapper).coerce();
        assertThat(coerce, equalTo(false));
    }


    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "shape")
            .field("ignore_z_value", "true")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        boolean ignoreZValue = ((ShapeFieldMapper)fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "shape")
            .field("ignore_z_value", "false")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ignoreZValue = ((ShapeFieldMapper)fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(false));
    }

    /**
     * Test that ignore_malformed parameter correctly parses
     */
    public void testIgnoreMalformedParsing() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "shape")
            .field("ignore_malformed", "true")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        boolean ignoreMalformed = ((ShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed, equalTo(true));

        // explicit false ignore_malformed test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "shape")
            .field("ignore_malformed", "false")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ignoreMalformed = ((ShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed, equalTo(false));
    }

    public void testShapeMapperMerge() throws Exception {
        String stage1Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("shape").field("type", "shape")
            .field("orientation", "ccw")
            .endObject().endObject().endObject().endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper docMapper = mapperService.merge("type", new CompressedXContent(stage1Mapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        String stage2Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("shape").field("type", "shape")
            .field("orientation", "cw").endObject().endObject().endObject().endObject());
        mapperService.merge("type", new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE);

        // verify nothing changed
        Mapper fieldMapper = docMapper.mappers().getMapper("shape");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ShapeFieldMapper ShapeFieldMapper = (ShapeFieldMapper) fieldMapper;
        assertThat(ShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CCW));

        // change mapping; orientation
        stage2Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("shape").field("type", "shape")
            .field("orientation", "cw").endObject().endObject().endObject().endObject());
        docMapper = mapperService.merge("type", new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE);

        fieldMapper = docMapper.mappers().getMapper("shape");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ShapeFieldMapper shapeFieldMapper = (ShapeFieldMapper) fieldMapper;
        assertThat(shapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CW));
    }

    public void testSerializeDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "shape")
            .endObject().endObject()
            .endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().parse("type1", new CompressedXContent(mapping));
        String serialized = toXContentString((ShapeFieldMapper) defaultMapper.mappers().getMapper("location"));
        assertTrue(serialized, serialized.contains("\"orientation\":\"" +
            ShapeBuilder.Orientation.RIGHT + "\""));
    }

    public void testShapeArrayParsing() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("location")
            .field("type", "shape")
            .endObject()
            .endObject()
            .endObject());

        DocumentMapper mapper = createIndex("test").mapperService().parse("_doc", new CompressedXContent(mapping));

        BytesReference arrayedDoc = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startArray("shape")
            .startObject()
            .field("type", "Point")
            .startArray("coordinates").value(176.0).value(15.0).endArray()
            .endObject()
            .startObject()
            .field("type", "Point")
            .startArray("coordinates").value(76.0).value(-15.0).endArray()
            .endObject()
            .endArray()
            .endObject()
        );

        SourceToParse sourceToParse = new SourceToParse("test", "1", arrayedDoc, XContentType.JSON);
        ParsedDocument document = mapper.parse(sourceToParse);
        assertThat(document.docs(), hasSize(1));
        IndexableField[] fields = document.docs().get(0).getFields("shape.type");
        assertThat(fields.length, equalTo(2));
    }

    public String toXContentString(ShapeFieldMapper mapper, boolean includeDefaults) {
        if (includeDefaults) {
            ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"));
            return Strings.toString(mapper, params);
        } else {
            return Strings.toString(mapper);
        }
    }

    public String toXContentString(ShapeFieldMapper mapper) {
        return toXContentString(mapper, true);
    }
}
