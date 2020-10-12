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

import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeFieldMapperTests extends FieldMapperTestCase2<GeoShapeFieldMapper.Builder> {

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "doc_values", "store");
    }

    @Override
    protected GeoShapeFieldMapper.Builder newBuilder() {
        return new GeoShapeFieldMapper.Builder("geoshape");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("orientation", "right"), m -> {
            GeoShapeFieldMapper gsfm = (GeoShapeFieldMapper) m;
            assertEquals(ShapeBuilder.Orientation.RIGHT, gsfm.orientation());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> {
            GeoShapeFieldMapper gpfm = (GeoShapeFieldMapper) m;
            assertTrue(gpfm.ignoreMalformed.value());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            GeoShapeFieldMapper gpfm = (GeoShapeFieldMapper) m;
            assertFalse(gpfm.ignoreZValue.value());
        });
        checker.registerUpdateCheck(b -> b.field("coerce", true), m -> {
            GeoShapeFieldMapper gpfm = (GeoShapeFieldMapper) m;
            assertTrue(gpfm.coerce.value());
        });
    }

    @Before
    public void addModifiers() {
        addModifier("orientation", true, (a, b) -> {
            a.orientation(ShapeBuilder.Orientation.LEFT);
            b.orientation(ShapeBuilder.Orientation.RIGHT);
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
    protected Object getSampleValueForDocument() {
        return "POINT (14.0 15.0)";
    }

    public void testDefaultConfiguration() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(),
            equalTo(GeoShapeFieldMapper.Defaults.ORIENTATION.value()));
        assertThat(geoShapeFieldMapper.fieldType().hasDocValues(), equalTo(false));
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "left")));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        ShapeBuilder.Orientation orientation = ((GeoShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // explicit right orientation test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "right")));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        orientation = ((GeoShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
    }

    /**
     * Test that coerce parameter correctly parses
     */
    public void testCoerceParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("coerce", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        boolean coerce = ((GeoShapeFieldMapper)fieldMapper).coerce().value();
        assertThat(coerce, equalTo(true));

        // explicit false coerce test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("coerce", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        coerce = ((GeoShapeFieldMapper)fieldMapper).coerce().value();
        assertThat(coerce, equalTo(false));
        assertFieldWarnings("tree");
    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_z_value", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        boolean ignoreZValue = ((GeoShapeFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_z_value", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        ignoreZValue = ((GeoShapeFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(false));
    }

    /**
     * Test that ignore_malformed parameter correctly parses
     */
    public void testIgnoreMalformedParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_malformed", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        Explicit<Boolean> ignoreMalformed = ((GeoShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.value(), equalTo(true));

        // explicit false ignore_malformed test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_malformed", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        ignoreMalformed = ((GeoShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.explicit(), equalTo(true));
        assertThat(ignoreMalformed.value(), equalTo(false));
    }

    private void assertFieldWarnings(String... fieldNames) {
        String[] warnings = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            warnings[i] = "Field parameter [" + fieldNames[i] + "] "
                + "is deprecated and will be removed in a future version.";
        }
    }

    public void testGeoShapeMapperMerge() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "ccw")));
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CCW));

        // change mapping; orientation
        merge(mapperService, fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "cw")));
        fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CW));
    }

    public void testSerializeDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertThat(
            Strings.toString(
                mapper.mappers().getMapper("field"),
                new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"))
            ),
            containsString("\"orientation\":\"" + AbstractShapeGeometryFieldMapper.Defaults.ORIENTATION.value() + "\"")
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

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    protected void assertSearchable(MappedFieldType fieldType) {
        //always searchable even if it uses TextSearchInfo.NONE
        assertTrue(fieldType.isSearchable());
    }
}
