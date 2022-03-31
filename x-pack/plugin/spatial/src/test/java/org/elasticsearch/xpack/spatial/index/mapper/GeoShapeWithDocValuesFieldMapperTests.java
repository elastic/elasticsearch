/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

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
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeWithDocValuesFieldMapperTests extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_shape");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "POINT (14.0 15.0)";
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
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerUpdateCheck(b -> b.field("orientation", "right"), m -> {
            GeoShapeWithDocValuesFieldMapper gsfm = (GeoShapeWithDocValuesFieldMapper) m;
            assertEquals(Orientation.RIGHT, gsfm.orientation());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> {
            GeoShapeWithDocValuesFieldMapper gpfm = (GeoShapeWithDocValuesFieldMapper) m;
            assertTrue(gpfm.ignoreMalformed());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            GeoShapeWithDocValuesFieldMapper gpfm = (GeoShapeWithDocValuesFieldMapper) m;
            assertFalse(gpfm.ignoreZValue());
        });
        checker.registerUpdateCheck(b -> b.field("coerce", true), m -> {
            GeoShapeWithDocValuesFieldMapper gpfm = (GeoShapeWithDocValuesFieldMapper) m;
            assertTrue(gpfm.coerce());
        });
    }

    @Override
    protected Collection<Plugin> getPlugins() {
        return Collections.singletonList(new LocalStateSpatialPlugin());
    }

    public void testDefaultConfiguration() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        GeoShapeWithDocValuesFieldMapper geoShapeFieldMapper = (GeoShapeWithDocValuesFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(Orientation.RIGHT));
        assertTrue(geoShapeFieldMapper.fieldType().hasDocValues());
    }

    public void testDefaultDocValueConfigurationOnPre7_8() throws IOException {

        Version oldVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_7_0);
        DocumentMapper defaultMapper = createDocumentMapper(oldVersion, fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        GeoShapeWithDocValuesFieldMapper geoShapeFieldMapper = (GeoShapeWithDocValuesFieldMapper) fieldMapper;
        assertFalse(geoShapeFieldMapper.fieldType().hasDocValues());
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("orientation", "left");
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        Orientation orientation = ((GeoShapeWithDocValuesFieldMapper) fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.LEFT));
        assertThat(orientation, equalTo(Orientation.CW));

        // explicit right orientation test
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("orientation", "right");
        }));
        fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        orientation = ((GeoShapeWithDocValuesFieldMapper) fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.RIGHT));
        assertThat(orientation, equalTo(Orientation.CCW));
    }

    /**
     * Test that coerce parameter correctly parses
     */
    public void testCoerceParsing() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("coerce", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        boolean coerce = ((GeoShapeWithDocValuesFieldMapper) fieldMapper).coerce();
        assertThat(coerce, equalTo(true));

        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("coerce", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        coerce = ((GeoShapeWithDocValuesFieldMapper) fieldMapper).coerce();
        assertThat(coerce, equalTo(false));

    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("ignore_z_value", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        boolean ignoreZValue = ((GeoShapeWithDocValuesFieldMapper) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("ignore_z_value", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        ignoreZValue = ((GeoShapeWithDocValuesFieldMapper) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(false));
    }

    /**
     * Test that ignore_malformed parameter correctly parses
     */
    public void testIgnoreMalformedParsing() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("ignore_malformed", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        boolean ignoreMalformed = ((GeoShapeWithDocValuesFieldMapper) fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed, equalTo(true));

        // explicit false ignore_malformed test
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("ignore_malformed", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        ignoreMalformed = ((GeoShapeWithDocValuesFieldMapper) fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed, equalTo(false));
    }

    public void testIgnoreMalformedValues() throws IOException {

        DocumentMapper ignoreMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("ignore_malformed", true);
        }));
        DocumentMapper failMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
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
            b.field("type", "geo_shape");
            b.field("doc_values", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        boolean hasDocValues = ((GeoShapeWithDocValuesFieldMapper) fieldMapper).fieldType().hasDocValues();
        assertTrue(hasDocValues);

        // explicit false doc_values
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("doc_values", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        hasDocValues = ((GeoShapeWithDocValuesFieldMapper) fieldMapper).fieldType().hasDocValues();
        assertFalse(hasDocValues);
    }

    public void testGeoShapeMapperMerge() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("orientation", "ccw");
        }));

        merge(mapperService, fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("orientation", "cw");
        }));

        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        GeoShapeWithDocValuesFieldMapper geoShapeFieldMapper = (GeoShapeWithDocValuesFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(Orientation.CW));
    }

    public void testInvalidCurrentVersion() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> super.createMapperService(
                Version.CURRENT,
                fieldMapping((b) -> { b.field("type", "geo_shape").field("strategy", "recursive"); })
            )
        );
        assertThat(
            e.getMessage(),
            containsString("using deprecated parameters [strategy] " + "in mapper [field] of type [geo_shape] is no longer allowed")
        );
    }

    public void testGeoShapeLegacyMerge() throws Exception {
        Version version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        MapperService m = createMapperService(version, fieldMapping(b -> b.field("type", "geo_shape")));
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(m, fieldMapping(b -> b.field("type", "geo_shape").field("strategy", "recursive")))
        );

        assertThat(e.getMessage(), containsString("mapper [field] of type [geo_shape] cannot change strategy from [BKD] to [recursive]"));
        assertFieldWarnings("strategy");

        MapperService lm = createMapperService(version, fieldMapping(b -> b.field("type", "geo_shape").field("strategy", "recursive")));
        e = expectThrows(IllegalArgumentException.class, () -> merge(lm, fieldMapping(b -> b.field("type", "geo_shape"))));
        assertThat(e.getMessage(), containsString("mapper [field] of type [geo_shape] cannot change strategy from [recursive] to [BKD]"));
        assertFieldWarnings("strategy");
    }

    private void assertFieldWarnings(String... fieldNames) {
        String[] warnings = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            warnings[i] = "Parameter [" + fieldNames[i] + "] " + "is deprecated and will be removed in a future version";
        }
        assertWarnings(warnings);
    }

    public void testSerializeDefaults() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        String serialized = toXContentString((GeoShapeWithDocValuesFieldMapper) defaultMapper.mappers().getMapper("field"));
        assertTrue(serialized, serialized.contains("\"orientation\":\"" + Orientation.RIGHT + "\""));
        assertTrue(serialized, serialized.contains("\"doc_values\":true"));
    }

    public void testSerializeDocValues() throws IOException {
        boolean docValues = randomBoolean();
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("doc_values", docValues);
        }));
        String serialized = toXContentString((GeoShapeWithDocValuesFieldMapper) mapper.mappers().getMapper("field"));
        assertTrue(serialized, serialized.contains("\"orientation\":\"" + Orientation.RIGHT + "\""));
        assertTrue(serialized, serialized.contains("\"doc_values\":" + docValues));
    }

    public void testGeoShapeArrayParsing() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        BytesReference arrayedDoc = BytesReference.bytes(
            XContentFactory.jsonBuilder()
                .startObject()
                .startArray("shape")
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
                .endArray()
                .endObject()
        );

        SourceToParse sourceToParse = new SourceToParse("1", arrayedDoc, XContentType.JSON);
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
        assertWarnings("Adding multifields to [geo_shape] mappers has no effect and will be forbidden in future");
    }

    public String toXContentString(GeoShapeWithDocValuesFieldMapper mapper, boolean includeDefaults) {
        if (includeDefaults) {
            ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"));
            return Strings.toString(mapper, params);
        } else {
            return Strings.toString(mapper);
        }
    }

    public String toXContentString(GeoShapeWithDocValuesFieldMapper mapper) throws IOException {
        return toXContentString(mapper, true);
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {

    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }
}
