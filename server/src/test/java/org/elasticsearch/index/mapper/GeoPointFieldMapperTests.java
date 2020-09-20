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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.lookup.SourceLookup;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.geometry.utils.Geohash.stringEncode;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GeoPointFieldMapperTests extends FieldMapperTestCase2<GeoPointFieldMapper.Builder> {

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "doc_values");
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_point");
    }

    public void testGeoHashValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", stringEncode(1.3, 1.2))));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testWKT() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "POINT (2 3)")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonValuesStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("field").field("lat", 1.2).field("lon", 1.3).endObject()));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testArrayLatLonValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_point").field("doc_values", false).field("store", true))
        );
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("field");
            b.startObject().field("lat", 1.2).field("lon", 1.3).endObject();
            b.startObject().field("lat", 1.4).field("lon", 1.5).endObject();
            b.endArray();
        }));

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("field"), notNullValue());
        assertThat(doc.rootDoc().getFields("field").length, equalTo(4));
    }

    public void testLatLonInOneValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.2,1.3")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonStringWithZValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.2,1.3,10.0")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonStringWithZValueException() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", false)));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", "1.2,1.3,10.0"))));
        assertThat(e.getCause().getMessage(), containsString("but [ignore_z_value] parameter is [false]"));
    }

    public void testLatLonInOneValueStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.2,1.3")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonInOneValueArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_point").field("doc_values", false).field("store", true))
        );
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value("1.2,1.3").value("1.4,1.5").endArray()));

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("field"), notNullValue());
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(4));
    }

    public void testLonLatArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLonLatArrayDynamic() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startArray("dynamic_templates");
        {
            mapping.startObject().startObject("point");
            {
                mapping.field("match", "point*");
                mapping.startObject("mapping").field("type", "geo_point").endObject();
            }
            mapping.endObject().endObject();
        }
        mapping.endArray().endObject().endObject();
        DocumentMapper mapper = createDocumentMapper(mapping);

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("point").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testLonLatArrayStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getFields("field").length, equalTo(3));
    }

    public void testLonLatArrayArrayStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_point").field("store", true).field("doc_values", false))
        );
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("field");
            b.startArray().value(1.3).value(1.2).endArray();
            b.startArray().value(1.5).value(1.4).endArray();
            b.endArray();
        }));
        assertThat(doc.rootDoc().getFields("field"), notNullValue());
        assertThat(doc.rootDoc().getFields("field").length, CoreMatchers.equalTo(4));
    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));
        boolean ignoreZValue = ((GeoPointFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));
        ignoreZValue = ((GeoPointFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(false));
    }

    public void testMultiField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_point").field("doc_values", false);
            b.startObject("fields");
            {
                b.startObject("geohash").field("type", "keyword").field("doc_values", false).endObject();  // test geohash as keyword
                b.startObject("latlon").field("type", "text").field("doc_values", false).endObject();  // test geohash as text
            }
            b.endObject();
        }));
        ParseContext.Document doc = mapper.parse(source(b -> b.field("field", "POINT (2 3)"))).rootDoc();
        assertThat(doc.getFields("field"), arrayWithSize(1));
        assertThat(doc.getField("field"), hasToString(both(containsString("field:2.999")).and(containsString("1.999"))));
        assertThat(doc.getFields("field.geohash"), arrayWithSize(1));
        assertThat(doc.getField("field.geohash").binaryValue().utf8ToString(), equalTo("s093jd0k72s1"));
        assertThat(doc.getFields("field.latlon"), arrayWithSize(1));
        assertThat(doc.getField("field.latlon").stringValue(), equalTo("s093jd0k72s1"));
    }

    public void testNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("null_value", "1,2")));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        AbstractPointGeometryFieldMapper.ParsedPoint nullValue = ((GeoPointFieldMapper) fieldMapper).nullValue;
        assertThat(nullValue, equalTo(new GeoPoint(1, 2)));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
        BytesRef defaultValue = doc.rootDoc().getBinaryValue("field");

        // Shouldn't matter if we specify the value explicitly or use null value
        doc = mapper.parse(source(b -> b.field("field", "1, 2")));
        assertThat(defaultValue, equalTo(doc.rootDoc().getBinaryValue("field")));

        doc = mapper.parse(source(b -> b.field("field", "3, 4")));
        assertThat(defaultValue, not(equalTo(doc.rootDoc().getBinaryValue("field"))));
    }

    /**
     * Test the fix for a bug that would read the value of field "ignore_z_value" for "ignore_malformed"
     * when setting the "null_value" field. See PR https://github.com/elastic/elasticsearch/pull/49645
     */
    public void testNullValueWithIgnoreMalformed() throws Exception {
        // Set ignore_z_value = false and ignore_malformed = true and test that a malformed point for null_value is normalized.
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "geo_point")
                    .field("ignore_z_value", false)
                    .field("ignore_malformed", true)
                    .field("null_value", "91,181")
            )
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        AbstractPointGeometryFieldMapper.ParsedPoint nullValue = ((GeoPointFieldMapper) fieldMapper).nullValue;
        // geo_point [91, 181] should have been normalized to [89, 1]
        assertThat(nullValue, equalTo(new GeoPoint(89, 1)));
    }

    public void testInvalidGeohashIgnored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_point").field("ignore_malformed", "true"))
        );
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234.333")));
        assertThat(doc.rootDoc().getField("field"), nullValue());
    }

    public void testInvalidGeohashNotIgnored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", "1234.333")))
        );
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [geo_point]"));
        assertThat(e.getRootCause().getMessage(), containsString("unsupported symbol [.] in geohash [1234.333]"));
    }

    public void testInvalidGeopointValuesIgnored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_malformed", "true")));

        assertThat(mapper.parse(source(b -> b.field("field", "1234.333"))).rootDoc().getField("field"), nullValue());
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", "-").field("lon", 1.3).endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", 1.3).field("lon", "-").endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(mapper.parse(source(b -> b.field("field", "-,1.3"))).rootDoc().getField("field"), nullValue());
        assertThat(mapper.parse(source(b -> b.field("field", "1.3,-"))).rootDoc().getField("field"), nullValue());
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", "NaN").field("lon", 1.2).endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", 1.2).field("lon", "NaN").endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(mapper.parse(source(b -> b.field("field", "1.3,NaN"))).rootDoc().getField("field"), nullValue());
        assertThat(mapper.parse(source(b -> b.field("field", "NaN,1.3"))).rootDoc().getField("field"), nullValue());
        assertThat(
            mapper.parse(source(b -> b.startObject("field").nullField("lat").field("lon", 1.2).endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", 1.2).nullField("lon").endObject())).rootDoc().getField("field"),
            nullValue()
        );
    }

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        AbstractGeometryFieldMapper<?, ?> mapper = new GeoPointFieldMapper.Builder("field").build(context);
        SourceLookup sourceLookup = new SourceLookup();

        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(42.0, 27.1));
        Map<String, Object> otherJsonPoint = Map.of("type", "Point", "coordinates", List.of(30.0, 50.0));
        String wktPoint = "POINT (42.0 27.1)";
        String otherWktPoint = "POINT (30.0 50.0)";

        // Test a single point in [lon, lat] array format.
        Object sourceValue = List.of(42.0, 27.1);
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single point in "lat, lon" string format.
        sourceValue = "27.1,42.0";
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of points in [lon, lat] array format.
        sourceValue = List.of(List.of(42.0, 27.1), List.of(30.0, 50.0));
        assertEquals(List.of(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single point in well-known text format.
        sourceValue = "POINT (42.0 27.1)";
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }

    @Override
    protected GeoPointFieldMapper.Builder newBuilder() {
        return new GeoPointFieldMapper.Builder("geo");
    }
}
