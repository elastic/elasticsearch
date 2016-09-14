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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.BaseGeoPointFieldMapper.LegacyGeoPointFieldType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.util.Collection;

import static org.elasticsearch.common.geo.GeoHashUtils.stringEncode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class LegacyGeohashMappingGeoPointTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testGeoHashValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("geohash", true)
                .endObject().endObject().endObject().endObject().string();

        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_2_4_0);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", stringEncode(1.3, 1.2))
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), nullValue());
        assertThat(doc.rootDoc().getField("point.lon"), nullValue());
        assertThat(doc.rootDoc().getField("point.geohash").stringValue(), equalTo(stringEncode(1.3, 1.2)));
        assertThat(doc.rootDoc().get("point"), notNullValue());
    }

    public void testGeoHashPrecisionAsInteger() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("geohash", true)
                .field("geohash_precision", 10).endObject().endObject().endObject().endObject().string();

        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_2_4_0);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("point");
        assertThat(mapper, instanceOf(BaseGeoPointFieldMapper.class));
        BaseGeoPointFieldMapper geoPointFieldMapper = (BaseGeoPointFieldMapper) mapper;
        assertThat(((LegacyGeoPointFieldType)geoPointFieldMapper.fieldType()).geoHashPrecision(), is(10));
    }

    public void testGeoHashPrecisionAsLength() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("geohash", true)
            .field("geohash_precision", "5m").endObject().endObject()
                .endObject().endObject().string();

        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_2_4_0);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        DocumentMapper defaultMapper = createIndex("test", settings).mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("point");
        assertThat(mapper, instanceOf(BaseGeoPointFieldMapper.class));
        BaseGeoPointFieldMapper geoPointFieldMapper = (BaseGeoPointFieldMapper) mapper;
        assertThat(((LegacyGeoPointFieldType)geoPointFieldMapper.fieldType()).geoHashPrecision(), is(10));
    }
}
