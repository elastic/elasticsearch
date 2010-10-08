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

import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.lucene.geo.GeoHashUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.xcontent.MapperTests;
import org.elasticsearch.index.mapper.xcontent.XContentDocumentMapper;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class LatLonMappingGeoPointTests {

    @Test public void testLatLonValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 1.3).endObject()
                .endObject()
                .copiedBytes());

        assertThat(doc.doc().getField("point.lat"), notNullValue());
        assertThat(doc.doc().getField("point.lat").getBinaryValue(), nullValue());
        assertThat(doc.doc().getField("point.lon"), notNullValue());
        assertThat(doc.doc().getField("point.lon").getBinaryValue(), nullValue());
        assertThat(doc.doc().getField("point.geohash"), nullValue());
    }

    @Test public void testLatLonValuesStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 1.3).endObject()
                .endObject()
                .copiedBytes());

        assertThat(doc.doc().getField("point.lat"), notNullValue());
        assertThat(doc.doc().getField("point.lat").getBinaryValue(), equalTo(Numbers.doubleToBytes(1.2)));
        assertThat(doc.doc().getField("point.lon"), notNullValue());
        assertThat(doc.doc().getField("point.lon").getBinaryValue(), equalTo(Numbers.doubleToBytes(1.3)));
        assertThat(doc.doc().getField("point.geohash"), nullValue());
    }

    @Test public void testArrayLatLonValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point")
                .startObject().field("lat", 1.2).field("lon", 1.3).endObject()
                .startObject().field("lat", 1.4).field("lon", 1.5).endObject()
                .endArray()
                .endObject()
                .copiedBytes());

        assertThat(doc.doc().getFields("point.lat").length, equalTo(2));
        assertThat(doc.doc().getFields("point.lon").length, equalTo(2));
        assertThat(doc.doc().getFields("point.lat")[0].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.2)));
        assertThat(doc.doc().getFields("point.lon")[0].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.3)));
        assertThat(doc.doc().getFields("point.lat")[1].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.4)));
        assertThat(doc.doc().getFields("point.lon")[1].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.5)));

        assertThat(doc.doc().getField("point.geohash"), nullValue());
    }

    @Test public void testLatLonInOneValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "1.2,1.3")
                .endObject()
                .copiedBytes());

        assertThat(doc.doc().getField("point.lat"), notNullValue());
        assertThat(doc.doc().getField("point.lon"), notNullValue());
        assertThat(doc.doc().getField("point.geohash"), nullValue());
    }

    @Test public void testLatLonInOneValueStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "1.2,1.3")
                .endObject()
                .copiedBytes());

        assertThat(doc.doc().getField("point.lat"), notNullValue());
        assertThat(doc.doc().getField("point.lat").getBinaryValue(), equalTo(Numbers.doubleToBytes(1.2)));
        assertThat(doc.doc().getField("point.lon"), notNullValue());
        assertThat(doc.doc().getField("point.lon").getBinaryValue(), equalTo(Numbers.doubleToBytes(1.3)));
        assertThat(doc.doc().getField("point.geohash"), nullValue());
    }

    @Test public void testLatLonInOneValueArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point")
                .value("1.2,1.3")
                .value("1.4,1.5")
                .endArray()
                .endObject()
                .copiedBytes());

        assertThat(doc.doc().getFields("point.lat").length, equalTo(2));
        assertThat(doc.doc().getFields("point.lon").length, equalTo(2));
        assertThat(doc.doc().getFields("point.lat")[0].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.2)));
        assertThat(doc.doc().getFields("point.lon")[0].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.3)));
        assertThat(doc.doc().getFields("point.lat")[1].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.4)));
        assertThat(doc.doc().getFields("point.lon")[1].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.5)));

        assertThat(doc.doc().getField("point.geohash"), nullValue());
    }

    @Test public void testGeoHashValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", GeoHashUtils.encode(1.2, 1.3))
                .endObject()
                .copiedBytes());

        assertThat(doc.doc().getField("point.lat"), notNullValue());
        assertThat(doc.doc().getField("point.lon"), notNullValue());
        assertThat(doc.doc().getField("point.geohash"), nullValue());
    }

    @Test public void testLatLonArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point").value(1.2).value(1.3).endArray()
                .endObject()
                .copiedBytes());

        assertThat(doc.doc().getField("point.lat"), notNullValue());
        assertThat(doc.doc().getField("point.lat").getBinaryValue(), nullValue());
        assertThat(doc.doc().getField("point.lon"), notNullValue());
        assertThat(doc.doc().getField("point.lon").getBinaryValue(), nullValue());
        assertThat(doc.doc().getField("point.geohash"), nullValue());
    }

    @Test public void testLatLonArrayStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point").value(1.2).value(1.3).endArray()
                .endObject()
                .copiedBytes());

        assertThat(doc.doc().getField("point.lat"), notNullValue());
        assertThat(doc.doc().getField("point.lat").getBinaryValue(), equalTo(Numbers.doubleToBytes(1.2)));
        assertThat(doc.doc().getField("point.lon"), notNullValue());
        assertThat(doc.doc().getField("point.lon").getBinaryValue(), equalTo(Numbers.doubleToBytes(1.3)));
        assertThat(doc.doc().getField("point.geohash"), nullValue());
    }

    @Test public void testLatLonArrayArrayStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point")
                .startArray().value(1.2).value(1.3).endArray()
                .startArray().value(1.4).value(1.5).endArray()
                .endArray()
                .endObject()
                .copiedBytes());

        assertThat(doc.doc().getFields("point.lat").length, equalTo(2));
        assertThat(doc.doc().getFields("point.lon").length, equalTo(2));
        assertThat(doc.doc().getFields("point.lat")[0].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.2)));
        assertThat(doc.doc().getFields("point.lon")[0].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.3)));
        assertThat(doc.doc().getFields("point.lat")[1].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.4)));
        assertThat(doc.doc().getFields("point.lon")[1].getBinaryValue(), equalTo(Numbers.doubleToBytes(1.5)));

        assertThat(doc.doc().getField("point.geohash"), nullValue());
    }
}
