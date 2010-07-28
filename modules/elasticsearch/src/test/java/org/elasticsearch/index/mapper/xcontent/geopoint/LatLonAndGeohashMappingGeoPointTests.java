/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.xcontent.geopoint;

import org.elasticsearch.common.lucene.geo.GeoHashUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.xcontent.XContentDocumentMapper;
import org.elasticsearch.index.mapper.xcontent.XContentMapperTests;
import org.hamcrest.MatcherAssert;
import org.testng.annotations.Test;

import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class LatLonAndGeohashMappingGeoPointTests {

    @Test public void testLatLonValues() throws Exception {
        String mapping = XContentFactory.contentTextBuilder(XContentType.JSON).startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("geohash", true).endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = XContentMapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 1.3).endObject()
                .endObject()
                .copiedBytes());

        MatcherAssert.assertThat(doc.doc().getField("point.lat"), notNullValue());
        MatcherAssert.assertThat(doc.doc().getField("point.lon"), notNullValue());
        MatcherAssert.assertThat(doc.doc().get("point.geohash"), equalTo(GeoHashUtils.encode(1.2, 1.3)));
    }

    @Test public void testLatLonInOneValue() throws Exception {
        String mapping = XContentFactory.contentTextBuilder(XContentType.JSON).startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("geohash", true).endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = XContentMapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "1.2,1.3")
                .endObject()
                .copiedBytes());

        MatcherAssert.assertThat(doc.doc().getField("point.lat"), notNullValue());
        MatcherAssert.assertThat(doc.doc().getField("point.lon"), notNullValue());
        MatcherAssert.assertThat(doc.doc().get("point.geohash"), equalTo(GeoHashUtils.encode(1.2, 1.3)));
    }

    @Test public void testGeoHashValue() throws Exception {
        String mapping = XContentFactory.contentTextBuilder(XContentType.JSON).startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("geohash", true).endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = XContentMapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", GeoHashUtils.encode(1.2, 1.3))
                .endObject()
                .copiedBytes());

        MatcherAssert.assertThat(doc.doc().getField("point.lat"), notNullValue());
        MatcherAssert.assertThat(doc.doc().getField("point.lon"), notNullValue());
        MatcherAssert.assertThat(doc.doc().get("point.geohash"), equalTo(GeoHashUtils.encode(1.2, 1.3)));
    }
}
