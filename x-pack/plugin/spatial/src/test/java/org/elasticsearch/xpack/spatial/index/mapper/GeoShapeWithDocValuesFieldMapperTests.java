/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

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
package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapperTestCase;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.index.mapper.AbstractPointGeometryFieldMapper.Names.IGNORE_Z_VALUE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeWithDocValuesFieldMapperTests extends FieldMapperTestCase<GeoShapeWithDocValuesFieldMapper.Builder> {

    @Override
    protected GeoShapeWithDocValuesFieldMapper.Builder newBuilder() {
        return new GeoShapeWithDocValuesFieldMapper.Builder("geoshape");
    }

    @Before
    public void addModifiers() {
        addModifier("orientation", true, (a, b) -> {
            a.orientation(ShapeBuilder.Orientation.RIGHT);
            b.orientation(ShapeBuilder.Orientation.LEFT);
        });
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, LocalStateCompositeXPackPlugin.class, LocalStateSpatialPlugin.class);
    }

    public void testDefaultConfiguration() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        GeoShapeWithDocValuesFieldMapper geoShapeFieldMapper = (GeoShapeWithDocValuesFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(),
            equalTo(org.elasticsearch.index.mapper.GeoShapeFieldMapper.Defaults.ORIENTATION.value()));
        assertFalse(geoShapeFieldMapper.docValues().explicit());
        assertTrue(geoShapeFieldMapper.docValues().value());
        assertTrue(geoShapeFieldMapper.fieldType().hasDocValues());
    }

    public void testDefaultDocValueConfigurationOnPre7_8() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .endObject().endObject()
            .endObject().endObject());

        Version oldVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_7_0);
        DocumentMapper defaultMapper = createIndex("test", settings(oldVersion).build()).mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        GeoShapeWithDocValuesFieldMapper geoShapeFieldMapper = (GeoShapeWithDocValuesFieldMapper) fieldMapper;
        assertFalse(geoShapeFieldMapper.docValues().explicit());
        assertFalse(geoShapeFieldMapper.docValues().value());
        assertFalse(geoShapeFieldMapper.fieldType().hasDocValues());
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("orientation", "left")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        ShapeBuilder.Orientation orientation = ((GeoShapeWithDocValuesFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // explicit right orientation test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("orientation", "right")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        orientation = ((GeoShapeWithDocValuesFieldMapper)fieldMapper).fieldType().orientation();
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
            .field("type", "geo_shape")
            .field("coerce", "true")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        boolean coerce = ((GeoShapeWithDocValuesFieldMapper)fieldMapper).coerce().value();
        assertThat(coerce, equalTo(true));

        // explicit false coerce test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("coerce", "false")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        coerce = ((GeoShapeWithDocValuesFieldMapper)fieldMapper).coerce().value();
        assertThat(coerce, equalTo(false));
        assertFieldWarnings("tree");
    }


    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field(IGNORE_Z_VALUE.getPreferredName(), "true")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        boolean ignoreZValue = ((GeoShapeWithDocValuesFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field(IGNORE_Z_VALUE.getPreferredName(), "false")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        ignoreZValue = ((GeoShapeWithDocValuesFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(false));
    }

    /**
     * Test that ignore_malformed parameter correctly parses
     */
    public void testIgnoreMalformedParsing() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("ignore_malformed", "true")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        Explicit<Boolean> ignoreMalformed = ((GeoShapeWithDocValuesFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.value(), equalTo(true));

        // explicit false ignore_malformed test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("ignore_malformed", "false")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        ignoreMalformed = ((GeoShapeWithDocValuesFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.explicit(), equalTo(true));
        assertThat(ignoreMalformed.value(), equalTo(false));
    }

    /**
     * Test that doc_values parameter correctly parses
     */
    public void testDocValues() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("doc_values", true)
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        assertTrue(((GeoShapeWithDocValuesFieldMapper)fieldMapper).docValues().explicit());
        assertTrue(((GeoShapeWithDocValuesFieldMapper)fieldMapper).docValues().value());
        boolean hasDocValues = ((GeoShapeWithDocValuesFieldMapper)fieldMapper).fieldType().hasDocValues();
        assertTrue(hasDocValues);

        // explicit false doc_values
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("doc_values", "false")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        assertTrue(((GeoShapeWithDocValuesFieldMapper)fieldMapper).docValues().explicit());
        assertFalse(((GeoShapeWithDocValuesFieldMapper)fieldMapper).docValues().value());
        hasDocValues = ((GeoShapeWithDocValuesFieldMapper)fieldMapper).fieldType().hasDocValues();
        assertFalse(hasDocValues);
    }

    private void assertFieldWarnings(String... fieldNames) {
        String[] warnings = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            warnings[i] = "Field parameter [" + fieldNames[i] + "] "
                + "is deprecated and will be removed in a future version.";
        }
    }

    public void testGeoShapeMapperMerge() throws Exception {
        String stage1Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("shape").field("type", "geo_shape")
            .field("orientation", "ccw")
            .endObject().endObject().endObject().endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper docMapper = mapperService.merge("type", new CompressedXContent(stage1Mapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        String stage2Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("shape").field("type", "geo_shape")
            .field("orientation", "cw").endObject().endObject().endObject().endObject());
        mapperService.merge("type", new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE);

        // verify nothing changed
        Mapper fieldMapper = docMapper.mappers().getMapper("shape");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        GeoShapeWithDocValuesFieldMapper geoShapeFieldMapper = (GeoShapeWithDocValuesFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CCW));

        // change mapping; orientation
        stage2Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("shape").field("type", "geo_shape")
            .field("orientation", "cw").endObject().endObject().endObject().endObject());
        docMapper = mapperService.merge("type", new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE);

        fieldMapper = docMapper.mappers().getMapper("shape");
        assertThat(fieldMapper, instanceOf(GeoShapeWithDocValuesFieldMapper.class));

        geoShapeFieldMapper = (GeoShapeWithDocValuesFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CW));
    }

    public void testEmptyName() throws Exception {
        // after 5.x
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("")
            .field("type", "geo_shape")
            .endObject().endObject()
            .endObject().endObject());
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type1", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testSerializeDefaults() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .endObject().endObject()
                .endObject().endObject());
            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            String serialized = toXContentString((GeoShapeWithDocValuesFieldMapper) defaultMapper.mappers().getMapper("location"));
            assertTrue(serialized, serialized.contains("\"orientation\":\"" +
                AbstractShapeGeometryFieldMapper.Defaults.ORIENTATION.value() + "\""));
            assertTrue(serialized, serialized.contains("\"doc_values\":true"));
        }
    }

    public void testSerializeDocValues() throws IOException {
        boolean docValues = randomBoolean();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("doc_values", docValues)
            .endObject().endObject()
            .endObject().endObject());
        DocumentMapper mapper = parser.parse("type1", new CompressedXContent(mapping));
        String serialized = toXContentString((GeoShapeWithDocValuesFieldMapper) mapper.mappers().getMapper("location"));
        assertTrue(serialized, serialized.contains("\"orientation\":\"" +
            AbstractShapeGeometryFieldMapper.Defaults.ORIENTATION.value() + "\""));
        assertTrue(serialized, serialized.contains("\"doc_values\":" + docValues));
    }

    public String toXContentString(GeoShapeWithDocValuesFieldMapper mapper, boolean includeDefaults) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        ToXContent.Params params;
        if (includeDefaults) {
            params = new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"));
        } else {
            params = ToXContent.EMPTY_PARAMS;
        }
        mapper.doXContentBody(builder, includeDefaults, params);
        return Strings.toString(builder.endObject());
    }

    public String toXContentString(GeoShapeWithDocValuesFieldMapper mapper) throws IOException {
        return toXContentString(mapper, true);
    }
}
