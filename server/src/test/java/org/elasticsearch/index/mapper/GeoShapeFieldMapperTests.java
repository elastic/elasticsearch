/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeFieldMapperTests extends MapperTestCase {

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("orientation", "right"), m -> {
            GeoShapeFieldMapper gsfm = (GeoShapeFieldMapper) m;
            assertEquals(Orientation.RIGHT, gsfm.orientation());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> {
            GeoShapeFieldMapper gpfm = (GeoShapeFieldMapper) m;
            assertTrue(gpfm.ignoreMalformed());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            GeoShapeFieldMapper gpfm = (GeoShapeFieldMapper) m;
            assertFalse(gpfm.ignoreZValue());
        });
        checker.registerUpdateCheck(b -> b.field("coerce", true), m -> {
            GeoShapeFieldMapper gpfm = (GeoShapeFieldMapper) m;
            assertTrue(gpfm.coerce.value());
        });
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new TestGeoShapeFieldMapperPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_shape");
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "POINT (14.0 15.0)";
    }

    public void testDefaultConfiguration() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(),
            equalTo(Orientation.RIGHT));
        assertThat(geoShapeFieldMapper.fieldType().hasDocValues(), equalTo(false));
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "left")));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        Orientation orientation = ((GeoShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.LEFT));
        assertThat(orientation, equalTo(Orientation.CW));

        // explicit right orientation test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "right")));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        orientation = ((GeoShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.RIGHT));
        assertThat(orientation, equalTo(Orientation.CCW));
    }

    /**
     * Test that coerce parameter correctly parses
     */
    public void testCoerceParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("coerce", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        boolean coerce = ((GeoShapeFieldMapper)fieldMapper).coerce();
        assertThat(coerce, equalTo(true));

        // explicit false coerce test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("coerce", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        coerce = ((GeoShapeFieldMapper)fieldMapper).coerce();
        assertThat(coerce, equalTo(false));
    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_z_value", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        boolean ignoreZValue = ((GeoShapeFieldMapper)fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_z_value", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        ignoreZValue = ((GeoShapeFieldMapper)fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(false));
    }

    /**
     * Test that ignore_malformed parameter correctly parses
     */
    public void testIgnoreMalformedParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_malformed", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        boolean ignoreMalformed = ((GeoShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed, equalTo(true));

        // explicit false ignore_malformed test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_malformed", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        ignoreMalformed = ((GeoShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed, equalTo(false));
    }

    private void assertFieldWarnings(String... fieldNames) {
        String[] warnings = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            warnings[i] = "Parameter [" + fieldNames[i] + "] "
                + "is deprecated and will be removed in a future version";
        }
        assertWarnings(warnings);
    }

    public void testGeoShapeMapperMerge() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "ccw")));
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(Orientation.CCW));

        // change mapping; orientation
        merge(mapperService, fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "cw")));
        fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(Orientation.CW));
    }

    public void testGeoShapeLegacyMerge() throws Exception {
        Version version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        MapperService m = createMapperService(version, fieldMapping(b -> b.field("type", "geo_shape")));
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> merge(m, fieldMapping(b -> b.field("type", "geo_shape").field("strategy", "recursive"))));

        assertThat(e.getMessage(),
            containsString("mapper [field] of type [geo_shape] cannot change strategy from [BKD] to [recursive]"));
        assertFieldWarnings("strategy");

        MapperService lm = createMapperService(version, fieldMapping(b -> b.field("type", "geo_shape").field("strategy", "recursive")));
        e = expectThrows(IllegalArgumentException.class,
            () -> merge(lm, fieldMapping(b -> b.field("type", "geo_shape"))));
        assertThat(e.getMessage(),
            containsString("mapper [field] of type [geo_shape] cannot change strategy from [recursive] to [BKD]"));
        assertFieldWarnings("strategy");
    }

    public void testSerializeDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertThat(
            Strings.toString(
                mapper.mappers().getMapper("field"),
                new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"))
            ),
            containsString("\"orientation\":\"" + Orientation.RIGHT + "\"")
        );
    }

    public void testGeoShapeArrayParsing() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument document = mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("type", "Point").startArray("coordinates").value(176.0).value(15.0).endArray().endObject();
                b.startObject().field("type", "Point").startArray("coordinates").value(76.0).value(-15.0).endArray().endObject();
            }
            b.endArray();
        }));
        assertThat(document.docs(), hasSize(1));
        assertThat(document.docs().get(0).getFields("field").length, equalTo(2));
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

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    protected void assertSearchable(MappedFieldType fieldType) {
        //always searchable even if it uses TextSearchInfo.NONE
        assertTrue(fieldType.isSearchable());
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }
}
