/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;

/** testing for {@link org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper} */
public class ShapeFieldMapperTests extends CartesianFieldMapperTests {

    @Override
    protected String getFieldName() {
        return "shape";
    }

    @Override
    protected void assertXYPointField(IndexableField field, float x, float y) {
        // TODO: ShapeField contains binary, perhaps we can decode the x/y from that
        assertThat(field, isA(ShapeField.Triangle.class));
    }

    /** The GeoJSON parser used by 'geo_shape' has a more complex exception handling approach than for 'geo_point' or 'point' */
    @Override
    protected void assertGeoJSONParseException(MapperParsingException e, String missingField) {
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("Failed to build"));
        assertThat(e.getCause().getCause().getMessage(), containsString(missingField + " not included"));
    }

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
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

    public void testDefaultConfiguration() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ShapeFieldMapper shapeFieldMapper = (ShapeFieldMapper) fieldMapper;
        assertThat(shapeFieldMapper.fieldType().orientation(), equalTo(Orientation.RIGHT));
        assertTrue(shapeFieldMapper.fieldType().hasDocValues());
    }

    public void testDefaultDocValueConfigurationOnPre8_4() throws IOException {
        Version oldVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_8_3_0);
        DocumentMapper defaultMapper = createDocumentMapper(oldVersion, fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ShapeFieldMapper shapeFieldMapper = (ShapeFieldMapper) fieldMapper;
        assertFalse(shapeFieldMapper.fieldType().hasDocValues());
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
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
            b.field("type", getFieldName());
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
            b.field("type", getFieldName());
            b.field("coerce", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        boolean coerce = ((ShapeFieldMapper) fieldMapper).coerce();
        assertThat(coerce, equalTo(true));

        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
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
            b.field("type", getFieldName());
            b.field("ignore_z_value", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        boolean ignoreZValue = ((ShapeFieldMapper) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
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
            b.field("type", getFieldName());
            b.field("ignore_malformed", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        boolean ignoreMalformed = ((ShapeFieldMapper) fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed, equalTo(true));

        // explicit false ignore_malformed test
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_malformed", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        ignoreMalformed = ((ShapeFieldMapper) fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed, equalTo(false));
    }

    public void testIgnoreMalformedValues() throws IOException {

        DocumentMapper ignoreMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_malformed", true);
        }));
        DocumentMapper failMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_malformed", false);
        }));

        {
            BytesReference arrayedDoc = BytesReference.bytes(
                XContentFactory.jsonBuilder().startObject().field("field", "Bad shape").endObject()
            );
            SourceToParse sourceToParse = new SourceToParse("1", arrayedDoc, XContentType.JSON);
            ParsedDocument document = ignoreMapper.parse(sourceToParse);
            assertThat(document.docs().get(0).getFields("field").length, equalTo(0));
            MapperParsingException exception = expectThrows(MapperParsingException.class, () -> failMapper.parse(sourceToParse));
            assertThat(exception.getCause().getMessage(), containsString("Unknown geometry type: bad"));
        }
        {
            BytesReference arrayedDoc = BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field(
                        "field",
                        "POLYGON ((18.9401790919516 -33.9681188869036, 18.9401790919516 -33.9681188869036, 18.9401790919517 "
                            + "-33.9681188869036, 18.9401790919517 -33.9681188869036, 18.9401790919516 -33.9681188869036))"
                    )
                    .endObject()
            );
            SourceToParse sourceToParse = new SourceToParse("1", arrayedDoc, XContentType.JSON);
            ParsedDocument document = ignoreMapper.parse(sourceToParse);
            assertThat(document.docs().get(0).getFields("field").length, equalTo(0));
            MapperParsingException exception = expectThrows(MapperParsingException.class, () -> failMapper.parse(sourceToParse));
            assertThat(exception.getCause().getMessage(), containsString("at least three non-collinear points required"));
        }
    }

    /**
     * Test that doc_values parameter correctly parses
     */
    public void testDocValues() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("doc_values", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        boolean hasDocValues = ((ShapeFieldMapper) fieldMapper).fieldType().hasDocValues();
        assertTrue(hasDocValues);

        // explicit false doc_values
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("doc_values", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(ShapeFieldMapper.class));

        hasDocValues = ((ShapeFieldMapper) fieldMapper).fieldType().hasDocValues();
        assertFalse(hasDocValues);
    }

    public void testShapeMapperMerge() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("orientation", "ccw");
        }));

        merge(mapperService, fieldMapping(b -> {
            b.field("type", getFieldName());
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
        assertTrue(serialized, serialized.contains("\"doc_values\":true"));
    }

    public void testSerializeDocValues() throws IOException {
        boolean docValues = randomBoolean();
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("doc_values", docValues);
        }));
        String serialized = toXContentString((ShapeFieldMapper) mapper.mappers().getMapper("field"));
        assertTrue(serialized, serialized.contains("\"orientation\":\"" + Orientation.RIGHT + "\""));
        assertTrue(serialized, serialized.contains("\"doc_values\":" + docValues));
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

    public void testMultiFieldsDeprecationWarning() throws Exception {
        createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.startObject("fields");
            b.startObject("keyword").field("type", "keyword").endObject();
            b.endObject();
        }));
        assertWarnings("Adding multifields to [shape] mappers has no effect and will be forbidden in future");
    }

    public void testSelfIntersectPolygon() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field(FIELD_NAME, "POLYGON((0 0, 1 1, 0 1, 1 0, 0 0))")))
        );
        assertThat(ex.getCause().getMessage(), containsString("Polygon self-intersection at lat=0.5 lon=0.5"));
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

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
