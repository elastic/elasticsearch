/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/** testing for {@link org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper} */
public class ShapeFieldMapperTests extends CartesianFieldMapperTests {

    @Override
    protected String getFieldName() {
        return "shape";
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerUpdateCheck(b -> b.field("orientation", "right"), m -> {
            ShapeFieldMapper gsfm = (ShapeFieldMapper) m;
            assertEquals(Orientation.RIGHT, gsfm.orientation());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> {
            ShapeFieldMapper gpfm = (ShapeFieldMapper) m;
            assertTrue(gpfm.ignoreMalformed());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            ShapeFieldMapper gpfm = (ShapeFieldMapper) m;
            assertFalse(gpfm.ignoreZValue());
        });
        checker.registerUpdateCheck(b -> b.field("coerce", true), m -> {
            ShapeFieldMapper gpfm = (ShapeFieldMapper) m;
            assertTrue(gpfm.coerce());
        });
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    public void testDefaultConfiguration() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ShapeFieldMapper shapeFieldMapper = (ShapeFieldMapper) fieldMapper;
        assertThat(shapeFieldMapper.fieldType().orientation(), equalTo(Orientation.RIGHT));
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "shape");
            b.field("orientation", "left");
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        Orientation orientation = ((ShapeFieldMapper) fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.LEFT));
        assertThat(orientation, equalTo(Orientation.CW));

        // explicit right orientation test
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "shape");
            b.field("orientation", "right");
        }));
        fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        orientation = ((ShapeFieldMapper) fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.RIGHT));
        assertThat(orientation, equalTo(Orientation.CCW));
    }

    /**
     * Test that coerce parameter correctly parses
     */
    public void testCoerceParsing() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "shape");
            b.field("coerce", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        boolean coerce = ((ShapeFieldMapper) fieldMapper).coerce();
        assertThat(coerce, equalTo(true));

        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "shape");
            b.field("coerce", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        coerce = ((ShapeFieldMapper) fieldMapper).coerce();
        assertThat(coerce, equalTo(false));

    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "shape");
            b.field("ignore_z_value", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        boolean ignoreZValue = ((ShapeFieldMapper) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "shape");
            b.field("ignore_z_value", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ignoreZValue = ((ShapeFieldMapper) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(false));
    }

    /**
     * Test that ignore_malformed parameter correctly parses
     */
    public void testIgnoreMalformedParsing() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "shape");
            b.field("ignore_malformed", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        boolean ignoreMalformed = ((ShapeFieldMapper) fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed, equalTo(true));

        // explicit false ignore_malformed test
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "shape");
            b.field("ignore_malformed", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ignoreMalformed = ((ShapeFieldMapper) fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed, equalTo(false));
    }

    public void testGeoShapeMapperMerge() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "shape");
            b.field("orientation", "ccw");
        }));

        merge(mapperService, fieldMapping(b -> {
            b.field("type", "shape");
            b.field("orientation", "cw");
        }));

        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ShapeFieldMapper shapeFieldMapper = (ShapeFieldMapper) fieldMapper;
        assertThat(shapeFieldMapper.fieldType().orientation(), equalTo(Orientation.CW));
    }

    public void testSerializeDefaults() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        String serialized = toXContentString((ShapeFieldMapper) defaultMapper.mappers().getMapper(FIELD_NAME));
        assertTrue(serialized, serialized.contains("\"orientation\":\"" + Orientation.RIGHT + "\""));
    }

    public void testShapeArrayParsing() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        SourceToParse sourceToParse = source(b -> {
            b.startArray("shape")
                .startObject()
                .field("type", "Point")
                .startArray("coordinates")
                .value(176.0)
                .value(15.0)
                .endArray()
                .endObject()
                .startObject()
                .field("type", "Point")
                .startArray("coordinates")
                .value(76.0)
                .value(-15.0)
                .endArray()
                .endObject()
                .endArray();
        });

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

    public void testMultiFieldsDeprecationWarning() throws Exception {
        createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.startObject("fields");
            b.startObject("keyword").field("type", "keyword").endObject();
            b.endObject();
        }));
        assertWarnings("Adding multifields to [shape] mappers has no effect and will be forbidden in future");
    }

    public String toXContentString(ShapeFieldMapper mapper) {
        return toXContentString(mapper, true);
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }
}
