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

import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.geo.RandomGeoGenerator;
import org.hamcrest.CoreMatchers;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.geo.GeoHashUtils.stringEncode;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GeoPointFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testLegacyLatLonValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();

        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_5_0_0_alpha5);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        boolean indexCreatedBefore22 = version.before(Version.V_2_2_0);
        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        final boolean stored = false;
        assertThat(doc.rootDoc().getField("point.lat").fieldType().stored(), is(stored));
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon").fieldType().stored(), is(stored));
        assertThat(doc.rootDoc().getField("point.geohash"), nullValue());
        if (indexCreatedBefore22) {
            assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
        } else {
            assertThat(Long.parseLong(doc.rootDoc().get("point")), equalTo(GeoPointField.encodeLatLon(1.2, 1.3)));
        }
    }

    public void testLegacyLatLonValuesWithGeohash() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true)
                .field("geohash", true).endObject().endObject()
                .endObject().endObject().string();

        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_5_0_0_alpha5);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        if (version.onOrAfter(Version.V_5_0_0_alpha1)) {
            assertThat(doc.rootDoc().getBinaryValue("point.geohash"), equalTo(new BytesRef(stringEncode(1.3, 1.2))));
        } else {
            assertThat(doc.rootDoc().get("point.geohash"), equalTo(stringEncode(1.3, 1.2)));
        }
    }

    public void testLegacyLatLonInOneValueWithGeohash() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true)
                .field("geohash", true).endObject().endObject().endObject().endObject().string();

        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_5_0_0_alpha5);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "1.2,1.3")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        if (version.onOrAfter(Version.V_5_0_0_alpha1)) {
            assertThat(doc.rootDoc().getBinaryValue("point.geohash"), equalTo(new BytesRef(stringEncode(1.3, 1.2))));
        } else {
            assertThat(doc.rootDoc().get("point.geohash"), equalTo(stringEncode(1.3, 1.2)));
        }
    }

    public void testLegacyGeoHashIndexValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true)
                .field("geohash", true).endObject().endObject().endObject().endObject().string();

        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_5_0_0_alpha5);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", stringEncode(1.3, 1.2))
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        if (version.onOrAfter(Version.V_5_0_0_alpha1)) {
            assertThat(doc.rootDoc().getBinaryValue("point.geohash"), equalTo(new BytesRef(stringEncode(1.3, 1.2))));
        } else {
            assertThat(doc.rootDoc().get("point.geohash"), equalTo(stringEncode(1.3, 1.2)));
        }
    }

    public void testGeoHashValue() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "geo_point");
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            xContentBuilder = xContentBuilder.field("lat_lon", true);
        }
        String mapping = xContentBuilder.endObject().endObject().endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", stringEncode(1.3, 1.2))
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point"), notNullValue());
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
            assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        }
    }

    public void testNormalizeLegacyLatLonValuesDefault() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_5_0_0_alpha5);
        // default to normalize
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point");
        if (version.before(Version.V_2_2_0)) {
            mapping.field("coerce", true);
        }
        mapping.field("ignore_malformed", true).endObject().endObject().endObject().endObject();

        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping.string()));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 91).field("lon", 181).endObject()
                .endObject()
                .bytes());

        if (version.before(Version.V_2_2_0)) {
            assertThat(doc.rootDoc().get("point"), equalTo("89.0,1.0"));
        } else {
            assertThat(Long.parseLong(doc.rootDoc().get("point")), equalTo(GeoPointField.encodeLatLon(89.0, 1.0)));
        }

        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", -91).field("lon", -181).endObject()
                .endObject()
                .bytes());

        if (version.before(Version.V_2_2_0)) {
            assertThat(doc.rootDoc().get("point"), equalTo("-89.0,-1.0"));
        } else {
            assertThat(Long.parseLong(doc.rootDoc().get("point")), equalTo(GeoPointField.encodeLatLon(-89.0, -1.0)));
        }

        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 181).field("lon", 361).endObject()
                .endObject()
                .bytes());

        if (version.before(Version.V_2_2_0)) {
            assertThat(doc.rootDoc().get("point"), equalTo("-1.0,-179.0"));
        } else {
            assertThat(Long.parseLong(doc.rootDoc().get("point")), equalTo(GeoPointField.encodeLatLon(-1.0, -179.0)));
        }
    }

    public void testLegacyValidateLatLonValues() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_5_0_0_alpha5);
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true);
        if (version.before(Version.V_2_2_0)) {
            mapping.field("coerce", false);
        }
        mapping.field("ignore_malformed", false).endObject().endObject().endObject().endObject().string();

        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping.string()));

        defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 90).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        expectThrows(MapperParsingException.class, () ->
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", -91).field("lon", 1.3).endObject()
                    .endObject()
                    .bytes()));

        expectThrows(MapperParsingException.class, () ->
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", 91).field("lon", 1.3).endObject()
                    .endObject()
                    .bytes()));

        expectThrows(MapperParsingException.class, () ->
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", 1.2).field("lon", -181).endObject()
                    .endObject()
                    .bytes()));

        expectThrows(MapperParsingException.class, () ->
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", 1.2).field("lon", 181).endObject()
                    .endObject()
                    .bytes()));

        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", "-").field("lon", 1.3).endObject()
                    .endObject()
                    .bytes()));
        assertThat(e.getRootCause(), instanceOf(NumberFormatException.class));
        assertThat(e.getRootCause().toString(), containsString("java.lang.NumberFormatException: For input string: \"-\""));

        e = expectThrows(MapperParsingException.class, () ->
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", 1.2).field("lon", "-").endObject()
                    .endObject()
                    .bytes()));
        assertThat(e.getRootCause(), instanceOf(NumberFormatException.class));
        assertThat(e.getRootCause().toString(), containsString("java.lang.NumberFormatException: For input string: \"-\""));

        e = expectThrows(MapperParsingException.class, () ->
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", "-").field("lon", "-").endObject()
                    .endObject()
                    .bytes()));
        assertThat(e.getRootCause(), instanceOf(NumberFormatException.class));
        assertThat(e.getRootCause().toString(), containsString("java.lang.NumberFormatException: For input string: \"-\""));
    }

    public void testNoValidateLegacyLatLonValues() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_5_0_0_alpha5);
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true);
        if (version.before(Version.V_2_2_0)) {
            mapping.field("coerce", false);
        }
        mapping.field("ignore_malformed", true).endObject().endObject().endObject().endObject().string();

        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping.string()));

        defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 90).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", -91).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 91).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", -181).endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 181).endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", "-").field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", "-").endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", "-").field("lon", "-").endObject()
                .endObject()
                .bytes());
    }

    public void testLatLonValuesStored() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.CURRENT, Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "geo_point");
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            xContentBuilder = xContentBuilder.field("lat_lon", true);
        }
        String mapping = xContentBuilder.field("store", true).endObject().endObject().endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point"), notNullValue());
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
            assertThat(doc.rootDoc().getField("point.lat").numericValue().doubleValue(), equalTo(1.2));
            assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
            assertThat(doc.rootDoc().getField("point.lon").numericValue().doubleValue(), equalTo(1.3));
            assertThat(doc.rootDoc().getField("point.geohash"), nullValue());
            if (version.before(Version.V_2_2_0)) {
                assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
            } else {
                assertThat(Long.parseLong(doc.rootDoc().get("point")), equalTo(GeoPointField.encodeLatLon(1.2, 1.3)));
            }
        }
    }

    public void testArrayLatLonValues() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "geo_point").field("doc_values", false);
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            xContentBuilder = xContentBuilder.field("lat_lon", true);
        }
        String mapping = xContentBuilder.field("store", true).endObject().endObject().endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point")
                .startObject().field("lat", 1.2).field("lon", 1.3).endObject()
                .startObject().field("lat", 1.4).field("lon", 1.5).endObject()
                .endArray()
                .endObject()
                .bytes());

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("point"), notNullValue());
        if (version.onOrAfter(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(doc.rootDoc().getFields("point").length, equalTo(4));
        } else {
            assertThat(doc.rootDoc().getFields("point").length, equalTo(2));
        }
        if (version.before(Version.V_5_0_0_alpha2)) {
            assertThat(doc.rootDoc().getFields("point.lat").length, equalTo(2));
            assertThat(doc.rootDoc().getFields("point.lon").length, equalTo(2));
            assertThat(doc.rootDoc().getFields("point.lat")[0].numericValue().doubleValue(), equalTo(1.2));
            assertThat(doc.rootDoc().getFields("point.lon")[0].numericValue().doubleValue(), equalTo(1.3));
            if (version.before(Version.V_2_2_0)) {
                assertThat(doc.rootDoc().getFields("point")[0].stringValue(), equalTo("1.2,1.3"));
            } else {
                assertThat(Long.parseLong(doc.rootDoc().getFields("point")[0].stringValue()), equalTo(GeoPointField.encodeLatLon(1.2, 1.3)));
            }
            assertThat(doc.rootDoc().getFields("point.lat")[1].numericValue().doubleValue(), equalTo(1.4));
            assertThat(doc.rootDoc().getFields("point.lon")[1].numericValue().doubleValue(), equalTo(1.5));
            if (version.before(Version.V_2_2_0)) {
                assertThat(doc.rootDoc().getFields("point")[1].stringValue(), equalTo("1.4,1.5"));
            } else {
                assertThat(Long.parseLong(doc.rootDoc().getFields("point")[1].stringValue()), equalTo(GeoPointField.encodeLatLon(1.4, 1.5)));
            }
        }
    }

    public void testLatLonInOneValue() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "geo_point");
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            xContentBuilder = xContentBuilder.field("lat_lon", true);
        }
        String mapping = xContentBuilder.endObject().endObject().endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type",
            new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "1.2,1.3")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point"), notNullValue());
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
            assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
            if (version.before(Version.V_2_2_0)) {
                assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
            } else {
                assertThat(Long.parseLong(doc.rootDoc().getFields("point")[0].stringValue()), equalTo(GeoPointField.encodeLatLon(1.2, 1.3)));
            }
        }
    }

    public void testLatLonInOneValueStored() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "geo_point");
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            xContentBuilder = xContentBuilder.field("lat_lon", true);
        }
        String mapping = xContentBuilder.field("store", true).endObject().endObject().endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type",
            new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "1.2,1.3")
                .endObject()
                .bytes());
        assertThat(doc.rootDoc().getField("point"), notNullValue());
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
            assertThat(doc.rootDoc().getField("point.lat").numericValue().doubleValue(), equalTo(1.2));
            assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
            assertThat(doc.rootDoc().getField("point.lon").numericValue().doubleValue(), equalTo(1.3));
            if (version.before(Version.V_2_2_0)) {
                assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
            } else {
                assertThat(Long.parseLong(doc.rootDoc().getFields("point")[0].stringValue()),
                    equalTo(GeoPointField.encodeLatLon(1.2, 1.3)));
            }
        }
    }

    public void testLatLonInOneValueArray() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "geo_point").field("doc_values", false);
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            xContentBuilder = xContentBuilder.field("lat_lon", true);
        }
        String mapping = xContentBuilder.field("store", true).endObject().endObject().endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type",
            new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point")
                .value("1.2,1.3")
                .value("1.4,1.5")
                .endArray()
                .endObject()
                .bytes());

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("point"), notNullValue());
        if (version.before(Version.V_5_0_0_alpha2)) {
            assertThat(doc.rootDoc().getFields("point").length, equalTo(2));
            assertThat(doc.rootDoc().getFields("point.lat").length, equalTo(2));
            assertThat(doc.rootDoc().getFields("point.lon").length, equalTo(2));
            assertThat(doc.rootDoc().getFields("point.lat")[0].numericValue().doubleValue(), equalTo(1.2));
            assertThat(doc.rootDoc().getFields("point.lon")[0].numericValue().doubleValue(), equalTo(1.3));
            assertThat(doc.rootDoc().getFields("point.lat")[1].numericValue().doubleValue(), equalTo(1.4));
            assertThat(doc.rootDoc().getFields("point.lon")[1].numericValue().doubleValue(), equalTo(1.5));
        } else if (version.onOrAfter(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(doc.rootDoc().getFields("point").length, equalTo(4));
        }
        if (version.before(Version.V_2_2_0)) {
            assertThat(doc.rootDoc().getFields("point")[0].stringValue(), equalTo("1.2,1.3"));
        } else if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(Long.parseLong(doc.rootDoc().getFields("point")[0].stringValue()), equalTo(GeoPointField.encodeLatLon(1.2, 1.3)));
        }
        if (version.before(Version.V_2_2_0)) {
            assertThat(doc.rootDoc().getFields("point")[1].stringValue(), equalTo("1.4,1.5"));
        } else if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(Long.parseLong(doc.rootDoc().getFields("point")[1].stringValue()), equalTo(GeoPointField.encodeLatLon(1.4, 1.5)));
        }
    }

    public void testLonLatArray() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "geo_point");
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            xContentBuilder = xContentBuilder.field("lat_lon", true);
        }
        String mapping = xContentBuilder.endObject().endObject().endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point").value(1.3).value(1.2).endArray()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point"), notNullValue());
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
            assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
            if (version.before(Version.V_2_2_0)) {
                assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
            } else {
                assertThat(Long.parseLong(doc.rootDoc().getFields("point")[0].stringValue()), equalTo(GeoPointField.encodeLatLon(1.2, 1.3)));
            }
        }
    }

    public void testLonLatArrayDynamic() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.CURRENT, Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startArray("dynamic_templates").startObject().startObject("point").field("match", "point*")
            .startObject("mapping").field("type", "geo_point");
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            xContentBuilder = xContentBuilder.field("lat_lon", true);
        }
        String mapping = xContentBuilder.endObject().endObject().endObject().endArray().endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point").value(1.3).value(1.2).endArray()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point"), notNullValue());
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
            assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
            if (version.before(Version.V_2_2_0)) {
                assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
            } else {
                assertThat(Long.parseLong(doc.rootDoc().getFields("point")[0].stringValue()), equalTo(GeoPointField.encodeLatLon(1.2, 1.3)));
            }
        }
    }

    public void testLonLatArrayStored() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "geo_point");
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            xContentBuilder = xContentBuilder.field("lat_lon", true);
        }
        String mapping = xContentBuilder.field("store", true).endObject().endObject().endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point").value(1.3).value(1.2).endArray()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point"), notNullValue());
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
            assertThat(doc.rootDoc().getField("point.lat").numericValue().doubleValue(), equalTo(1.2));
            assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
            assertThat(doc.rootDoc().getField("point.lon").numericValue().doubleValue(), equalTo(1.3));
            if (version.before(Version.V_2_2_0)) {
                assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
            } else {
                assertThat(Long.parseLong(doc.rootDoc().getFields("point")[0].stringValue()), equalTo(GeoPointField.encodeLatLon(1.2, 1.3)));
            }
        } else {
            assertThat(doc.rootDoc().getFields("point").length, equalTo(3));
        }
    }

    public void testLonLatArrayArrayStored() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "geo_point");
        if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            xContentBuilder = xContentBuilder.field("lat_lon", true);
        }
        String mapping = xContentBuilder.field("store", true).field("doc_values", false).endObject().endObject()
            .endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point")
                .startArray().value(1.3).value(1.2).endArray()
                .startArray().value(1.5).value(1.4).endArray()
                .endArray()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getFields("point"), notNullValue());
        if (version.before(Version.V_5_0_0_alpha2)) {
            assertThat(doc.rootDoc().getFields("point").length, CoreMatchers.equalTo(2));
            assertThat(doc.rootDoc().getFields("point.lat").length, equalTo(2));
            assertThat(doc.rootDoc().getFields("point.lon").length, equalTo(2));
            assertThat(doc.rootDoc().getFields("point.lat")[0].numericValue().doubleValue(), equalTo(1.2));
            assertThat(doc.rootDoc().getFields("point.lon")[0].numericValue().doubleValue(), equalTo(1.3));
            if (version.before(Version.V_2_2_0)) {
                assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
            } else {
                assertThat(Long.parseLong(doc.rootDoc().getFields("point")[0].stringValue()), equalTo(GeoPointField.encodeLatLon(1.2, 1.3)));
            }
            assertThat(doc.rootDoc().getFields("point.lat")[1].numericValue().doubleValue(), equalTo(1.4));
            assertThat(doc.rootDoc().getFields("point.lon")[1].numericValue().doubleValue(), equalTo(1.5));
            if (version.before(Version.V_2_2_0)) {
                assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
            } else {
                assertThat(Long.parseLong(doc.rootDoc().getFields("point")[1].stringValue()), equalTo(GeoPointField.encodeLatLon(1.4, 1.5)));
            }
        } else if (version.onOrAfter(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            assertThat(doc.rootDoc().getFields("point").length, CoreMatchers.equalTo(4));
        }
    }


    /**
     * Test that expected exceptions are thrown when creating a new index with deprecated options
     */
    public void testOptionDeprecation() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapperParser parser = createIndex("test", settings).mapperService().documentMapperParser();
        // test deprecation exceptions on newly created indexes
        if (version.onOrAfter(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            try {
                String normalizeMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                    .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                    .endObject().endObject().string();
                parser.parse("type", new CompressedXContent(normalizeMapping));
            } catch (MapperParsingException e) {
                assertEquals(e.getMessage(), "Mapping definition for [point] has unsupported parameters:  [lat_lon : true]");
            }
        }

        if (version.onOrAfter(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
            String normalizeMapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("point").field("type", "geo_point").field("geohash", true).endObject().endObject().endObject().endObject()
                    .string();
            Exception e = expectThrows(MapperParsingException.class, () ->
                parser.parse("type", new CompressedXContent(normalizeMapping)));
            assertEquals(e.getMessage(), "Mapping definition for [point] has unsupported parameters:  [geohash : true]");
        }

        {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("point").field("type", "geo_point");
            if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
                xContentBuilder = xContentBuilder.field("lat_lon", true).field("geohash", true);
            }
            String validateMapping = xContentBuilder.field("validate", true).endObject().endObject().endObject().endObject().string();
            Exception e = expectThrows(MapperParsingException.class, () ->
                parser.parse("type", new CompressedXContent(validateMapping)));
            assertEquals(e.getMessage(), "Mapping definition for [point] has unsupported parameters:  [validate : true]");
        }

        {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point");
            if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
                xContentBuilder = xContentBuilder.field("lat_lon", true).field("geohash", true);
            }
            String validateMapping = xContentBuilder.field("validate_lat", true).endObject().endObject().endObject().endObject().string();
            Exception e = expectThrows(MapperParsingException.class, () ->
                parser.parse("type", new CompressedXContent(validateMapping)));
            assertEquals(e.getMessage(), "Mapping definition for [point] has unsupported parameters:  [validate_lat : true]");
        }

        {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point");
            if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
                xContentBuilder = xContentBuilder.field("lat_lon", true).field("geohash", true);
            }
            String validateMapping = xContentBuilder.field("validate_lon", true).endObject().endObject().endObject().endObject().string();
            Exception e = expectThrows(MapperParsingException.class, () ->
                parser.parse("type", new CompressedXContent(validateMapping)));
            assertEquals(e.getMessage(), "Mapping definition for [point] has unsupported parameters:  [validate_lon : true]");
        }

        // test deprecated normalize
        {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point");
            if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
                xContentBuilder = xContentBuilder.field("lat_lon", true).field("geohash", true);
            }
            String normalizeMapping = xContentBuilder.field("normalize", true).endObject().endObject().endObject().endObject().string();
            Exception e = expectThrows(MapperParsingException.class, () ->
                parser.parse("type", new CompressedXContent(normalizeMapping)));
            assertEquals(e.getMessage(), "Mapping definition for [point] has unsupported parameters:  [normalize : true]");
        }

        {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point");
            if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
                xContentBuilder = xContentBuilder.field("lat_lon", true).field("geohash", true);
            }
            String normalizeMapping = xContentBuilder.field("normalize_lat", true).endObject().endObject().endObject().endObject().string();
            Exception e = expectThrows(MapperParsingException.class, () ->
                parser.parse("type", new CompressedXContent(normalizeMapping)));
            assertEquals(e.getMessage(), "Mapping definition for [point] has unsupported parameters:  [normalize_lat : true]");
        }

        {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point");
            if (version.before(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
                xContentBuilder = xContentBuilder.field("lat_lon", true).field("geohash", true);
            }
            String normalizeMapping = xContentBuilder.field("normalize_lon", true).endObject().endObject().endObject().endObject().string();
            Exception e = expectThrows(MapperParsingException.class, () ->
                parser.parse("type", new CompressedXContent(normalizeMapping)));
            assertEquals(e.getMessage(), "Mapping definition for [point] has unsupported parameters:  [normalize_lon : true]");
        }
    }

    public void testLegacyGeoPointMapperMerge() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_5_0_0_alpha5);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        String stage1Mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true)
                .field("geohash", true).endObject().endObject().endObject().endObject().string();
        MapperService mapperService = createIndex("test", settings).mapperService();
        DocumentMapper stage1 = mapperService.merge("type", new CompressedXContent(stage1Mapping), MapperService.MergeReason.MAPPING_UPDATE, false);
        String stage2Mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", false)
                .field("geohash", false).endObject().endObject().endObject().endObject().string();
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            mapperService.merge("type", new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE, false));
        assertThat(e.getMessage(), containsString("mapper [point] has different [lat_lon]"));
        assertThat(e.getMessage(), containsString("mapper [point] has different [geohash]"));
        assertThat(e.getMessage(), containsString("mapper [point] has different [geohash_precision]"));

        // correct mapping and ensure no failures
        String stage2MappingCorrect = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true)
                .field("geohash", true).endObject().endObject().endObject().endObject().string();
        mapperService.merge("type", new CompressedXContent(stage2MappingCorrect), MapperService.MergeReason.MAPPING_UPDATE, false);
    }

    public void testLegacyGeoHashSearch() throws Exception {
        // create a geo_point mapping with geohash enabled and random (between 1 and 12) geohash precision
        int precision = randomIntBetween(1, 12);
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("pin").startObject("properties").startObject("location")
                .field("type", "geo_point").field("geohash", true).field("geohash_precision", precision).field("store", true).endObject()
                .endObject().endObject().endObject().string();

        // create index and add a test point (dr5regy6rc6z)
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_5_0_0_alpha1);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        CreateIndexRequestBuilder mappingRequest = client().admin().indices().prepareCreate("test").setSettings(settings)
                .addMapping("pin", mapping);
        mappingRequest.execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().prepareIndex("test", "pin", "1").setSource(jsonBuilder().startObject().startObject("location").field("lat", 40.7143528)
                .field("lon", -74.0059731).endObject().endObject()).setRefreshPolicy(IMMEDIATE).get();

        // match all search with geohash field
        SearchResponse searchResponse = client().prepareSearch().addStoredField("location.geohash").setQuery(matchAllQuery()).execute().actionGet();
        Map<String, SearchHitField> m = searchResponse.getHits().getAt(0).getFields();

        // ensure single geohash was indexed
        assertEquals("dr5regy6rc6y".substring(0, precision), m.get("location.geohash").value());
    }

    public void testLegacyGeoHashSearchWithPrefix() throws Exception {
        // create a geo_point mapping with geohash enabled and random (between 1 and 12) geohash precision
        int precision = randomIntBetween(1, 12);
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("pin").startObject("properties").startObject("location")
                .field("type", "geo_point").field("geohash_prefix", true).field("geohash_precision", precision).field("store", true)
                .endObject().endObject().endObject().endObject().string();

        // create index and add a test point (dr5regy6rc6z)
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_5_0_0_alpha5);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        CreateIndexRequestBuilder mappingRequest = client().admin().indices().prepareCreate("test").setSettings(settings)
                .addMapping("pin", mapping);
        mappingRequest.execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().prepareIndex("test", "pin", "1").setSource(jsonBuilder().startObject().startObject("location").field("lat", 40.7143528)
                .field("lon", -74.0059731).endObject().endObject()).setRefreshPolicy(IMMEDIATE).get();

        // match all search with geohash field (includes prefixes)
        SearchResponse searchResponse = client().prepareSearch().addStoredField("location.geohash").setQuery(matchAllQuery()).execute().actionGet();
        Map<String, SearchHitField> m = searchResponse.getHits().getAt(0).getFields();

        List<Object> hashes = m.get("location.geohash").values();

        final int numHashes = hashes.size();
        for(int i=0; i<numHashes; ++i) {
            String hash = "dr5regy6rc6y".substring(0, numHashes-i);
            assertEquals(hash, hashes.get(i));
        }
    }

    public void testMultiField() throws Exception {
        int numDocs = randomIntBetween(10, 100);
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("pin").startObject("properties").startObject("location")
            .field("type", "geo_point")
            .startObject("fields")
            .startObject("geohash").field("type", "keyword").endObject()  // test geohash as keyword
            .startObject("latlon").field("type", "string").endObject()  // test geohash as string
            .endObject()
            .endObject().endObject().endObject().endObject().string();
        CreateIndexRequestBuilder mappingRequest = client().admin().indices().prepareCreate("test")
            .addMapping("pin", mapping);
        mappingRequest.execute().actionGet();

        // create index and add random test points
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        for (int i=0; i<numDocs; ++i) {
            final GeoPoint pt = RandomGeoGenerator.randomPoint(random());
            client().prepareIndex("test", "pin").setSource(jsonBuilder().startObject().startObject("location").field("lat", pt.lat())
                .field("lon", pt.lon()).endObject().endObject()).setRefreshPolicy(IMMEDIATE).get();
        }

        // TODO these tests are bogus and need to be Fix
        // query by geohash subfield
        SearchResponse searchResponse = client().prepareSearch().addStoredField("location.geohash").setQuery(matchAllQuery()).execute().actionGet();
        assertEquals(numDocs, searchResponse.getHits().totalHits());

        // query by latlon subfield
        searchResponse = client().prepareSearch().addStoredField("location.latlon").setQuery(matchAllQuery()).execute().actionGet();
        assertEquals(numDocs, searchResponse.getHits().totalHits());
    }


    public void testEmptyName() throws Exception {
        // after 5.x
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("").field("type", "geo_point").endObject().endObject()
            .endObject().endObject().string();

        Version version = Version.CURRENT;
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapperParser parser = createIndex("test", settings).mapperService().documentMapperParser();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));

        // before 5.x
        Version oldVersion = VersionUtils.randomVersionBetween(getRandom(), Version.V_2_0_0, Version.V_2_3_5);
        Settings oldIndexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, oldVersion).build();
        DocumentMapperParser parser2x = createIndex("test_old", oldIndexSettings).mapperService().documentMapperParser();

        DocumentMapper defaultMapper = parser2x.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, defaultMapper.mappingSource().string());
    }
}
