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

package org.elasticsearch.index.mapper.geo;

import org.apache.lucene.util.XGeoHashUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class GeohashMappingGeoPointTests extends ESSingleNodeTestCase {

    @Test
    public void testLatLonValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", false).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        MatcherAssert.assertThat(doc.rootDoc().getField("point.lat"), nullValue());
        MatcherAssert.assertThat(doc.rootDoc().getField("point.lon"), nullValue());
        MatcherAssert.assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
    }

    @Test
    public void testLatLonInOneValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", false).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "1.2,1.3")
                .endObject()
                .bytes());

        MatcherAssert.assertThat(doc.rootDoc().getField("point.lat"), nullValue());
        MatcherAssert.assertThat(doc.rootDoc().getField("point.lon"), nullValue());
        MatcherAssert.assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
    }

    @Test
    public void testGeoHashValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("geohash", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", XGeoHashUtils.stringEncode(1.3, 1.2))
                .endObject()
                .bytes());

        MatcherAssert.assertThat(doc.rootDoc().getField("point.lat"), nullValue());
        MatcherAssert.assertThat(doc.rootDoc().getField("point.lon"), nullValue());
        MatcherAssert.assertThat(doc.rootDoc().get("point.geohash"), equalTo(XGeoHashUtils.stringEncode(1.3, 1.2)));
        MatcherAssert.assertThat(doc.rootDoc().get("point"), notNullValue());
    }

    @Test
    public void testGeoHashPrecisionAsInteger() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("geohash", true).field("geohash_precision", 10).endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("point");
        assertThat(mapper, instanceOf(GeoPointFieldMapper.class));
        GeoPointFieldMapper geoPointFieldMapper = (GeoPointFieldMapper) mapper;
        assertThat(geoPointFieldMapper.fieldType().geohashPrecision(), is(10));
    }

    @Test
    public void testGeoHashPrecisionAsLength() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("geohash", true).field("geohash_precision", "5m").endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("point");
        assertThat(mapper, instanceOf(GeoPointFieldMapper.class));
        GeoPointFieldMapper geoPointFieldMapper = (GeoPointFieldMapper) mapper;
        assertThat(geoPointFieldMapper.fieldType().geohashPrecision(), is(10));
    }

    @Test
    public void testNullValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", (Object) null)
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("point"), nullValue());
    }
}
