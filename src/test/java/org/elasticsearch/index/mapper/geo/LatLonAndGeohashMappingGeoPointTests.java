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

import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class LatLonAndGeohashMappingGeoPointTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testLatLonValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("geohash", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().get("point.geohash"), equalTo(GeoHashUtils.encode(1.2, 1.3)));
    }

    @Test
    public void testLatLonInOneValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("geohash", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "1.2,1.3")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().get("point.geohash"), equalTo(GeoHashUtils.encode(1.2, 1.3)));
    }

    @Test
    public void testGeoHashValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("geohash", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", GeoHashUtils.encode(1.2, 1.3))
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().get("point.geohash"), equalTo(GeoHashUtils.encode(1.2, 1.3)));
    }
}