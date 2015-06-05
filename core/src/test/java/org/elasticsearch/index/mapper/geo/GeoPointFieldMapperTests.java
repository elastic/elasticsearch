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
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class GeoPointFieldMapperTests extends ElasticsearchSingleNodeTest {
    @Test
    public void testLatLonValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lat").fieldType().stored(), is(false));
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon").fieldType().stored(), is(false));
        assertThat(doc.rootDoc().getField("point.geohash"), nullValue());
        assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
    }

    @Test
    public void testLatLonValuesWithGeohash() throws Exception {
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
    public void testLatLonInOneValueWithGeohash() throws Exception {
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
    public void testGeoHashIndexValue() throws Exception {
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

    @Test
    public void testGeoHashValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", GeoHashUtils.encode(1.2, 1.3))
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().get("point"), notNullValue());
    }

    @Test
    public void testNormalizeLatLonValuesDefault() throws Exception {
        // default to normalize
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 91).field("lon", 181).endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("point"), equalTo("89.0,1.0"));

        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", -91).field("lon", -181).endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("point"), equalTo("-89.0,-1.0"));

        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 181).field("lon", 361).endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("point"), equalTo("-1.0,-179.0"));
    }

    @Test
    public void testValidateLatLonValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("normalize", false).field("validate", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);


        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 90).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", -91).field("lon", 1.3).endObject()
                    .endObject()
                    .bytes());
            fail();
        } catch (MapperParsingException e) {

        }

        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", 91).field("lon", 1.3).endObject()
                    .endObject()
                    .bytes());
            fail();
        } catch (MapperParsingException e) {

        }

        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", 1.2).field("lon", -181).endObject()
                    .endObject()
                    .bytes());
            fail();
        } catch (MapperParsingException e) {

        }

        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("point").field("lat", 1.2).field("lon", 181).endObject()
                    .endObject()
                    .bytes());
            fail();
        } catch (MapperParsingException e) {

        }
    }

    @Test
    public void testNoValidateLatLonValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("normalize", false).field("validate", false).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);


        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 90).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", -91).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 91).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", -181).endObject()
                .endObject()
                .bytes());

        defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 181).endObject()
                .endObject()
                .bytes());
    }

    @Test
    public void testLatLonValuesStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("point").field("lat", 1.2).field("lon", 1.3).endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lat").numericValue().doubleValue(), equalTo(1.2));
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon").numericValue().doubleValue(), equalTo(1.3));
        assertThat(doc.rootDoc().getField("point.geohash"), nullValue());
        assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
    }

    @Test
    public void testArrayLatLonValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point")
                .startObject().field("lat", 1.2).field("lon", 1.3).endObject()
                .startObject().field("lat", 1.4).field("lon", 1.5).endObject()
                .endArray()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getFields("point.lat").length, equalTo(2));
        assertThat(doc.rootDoc().getFields("point.lon").length, equalTo(2));
        assertThat(doc.rootDoc().getFields("point.lat")[0].numericValue().doubleValue(), equalTo(1.2));
        assertThat(doc.rootDoc().getFields("point.lon")[0].numericValue().doubleValue(), equalTo(1.3));
        assertThat(doc.rootDoc().getFields("point")[0].stringValue(), equalTo("1.2,1.3"));
        assertThat(doc.rootDoc().getFields("point.lat")[1].numericValue().doubleValue(), equalTo(1.4));
        assertThat(doc.rootDoc().getFields("point.lon")[1].numericValue().doubleValue(), equalTo(1.5));
        assertThat(doc.rootDoc().getFields("point")[1].stringValue(), equalTo("1.4,1.5"));
    }

    @Test
    public void testLatLonInOneValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "1.2,1.3")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
    }

    @Test
    public void testLatLonInOneValueStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "1.2,1.3")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lat").numericValue().doubleValue(), equalTo(1.2));
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon").numericValue().doubleValue(), equalTo(1.3));
        assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
    }

    @Test
    public void testLatLonInOneValueArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point")
                .value("1.2,1.3")
                .value("1.4,1.5")
                .endArray()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getFields("point.lat").length, equalTo(2));
        assertThat(doc.rootDoc().getFields("point.lon").length, equalTo(2));
        assertThat(doc.rootDoc().getFields("point.lat")[0].numericValue().doubleValue(), equalTo(1.2));
        assertThat(doc.rootDoc().getFields("point.lon")[0].numericValue().doubleValue(), equalTo(1.3));
        assertThat(doc.rootDoc().getFields("point")[0].stringValue(), equalTo("1.2,1.3"));
        assertThat(doc.rootDoc().getFields("point.lat")[1].numericValue().doubleValue(), equalTo(1.4));
        assertThat(doc.rootDoc().getFields("point.lon")[1].numericValue().doubleValue(), equalTo(1.5));
        assertThat(doc.rootDoc().getFields("point")[1].stringValue(), equalTo("1.4,1.5"));
    }

    @Test
    public void testLonLatArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point").value(1.3).value(1.2).endArray()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
    }

    @Test
    public void testLonLatArrayDynamic() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("dynamic_templates").startObject()
                .startObject("point").field("match", "point*").startObject("mapping").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endArray()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point").value(1.3).value(1.2).endArray()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
    }

    @Test
    public void testLonLatArrayStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point").value(1.3).value(1.2).endArray()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("point.lat"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lat").numericValue().doubleValue(), equalTo(1.2));
        assertThat(doc.rootDoc().getField("point.lon"), notNullValue());
        assertThat(doc.rootDoc().getField("point.lon").numericValue().doubleValue(), equalTo(1.3));
        assertThat(doc.rootDoc().get("point"), equalTo("1.2,1.3"));
    }

    @Test
    public void testLonLatArrayArrayStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("store", "yes").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("point")
                .startArray().value(1.3).value(1.2).endArray()
                .startArray().value(1.5).value(1.4).endArray()
                .endArray()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getFields("point.lat").length, equalTo(2));
        assertThat(doc.rootDoc().getFields("point.lon").length, equalTo(2));
        assertThat(doc.rootDoc().getFields("point.lat")[0].numericValue().doubleValue(), equalTo(1.2));
        assertThat(doc.rootDoc().getFields("point.lon")[0].numericValue().doubleValue(), equalTo(1.3));
        assertThat(doc.rootDoc().getFields("point")[0].stringValue(), equalTo("1.2,1.3"));
        assertThat(doc.rootDoc().getFields("point.lat")[1].numericValue().doubleValue(), equalTo(1.4));
        assertThat(doc.rootDoc().getFields("point.lon")[1].numericValue().doubleValue(), equalTo(1.5));
        assertThat(doc.rootDoc().getFields("point")[1].stringValue(), equalTo("1.4,1.5"));
    }

    @Test
    public void testGeoPointMapperMerge() throws Exception {
        String stage1Mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("geohash", true)
                .field("validate", true).endObject().endObject()
                .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper stage1 = parser.parse(stage1Mapping);
        String stage2Mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("geohash", true)
                .field("validate", false).endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper stage2 = parser.parse(stage2Mapping);

        MergeResult mergeResult = stage1.merge(stage2.mapping(), false);
        assertThat(mergeResult.hasConflicts(), equalTo(true));
        assertThat(mergeResult.buildConflicts().length, equalTo(2));
        // todo better way of checking conflict?
        assertThat("mapper [point] has different validate_lat", isIn(new ArrayList<>(Arrays.asList(mergeResult.buildConflicts()))));

        // correct mapping and ensure no failures
        stage2Mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("point").field("type", "geo_point").field("lat_lon", true).field("geohash", true)
                .field("validate", true).field("normalize", true).endObject().endObject()
                .endObject().endObject().string();
        stage2 = parser.parse(stage2Mapping);
        mergeResult = stage1.merge(stage2.mapping(), false);
        assertThat(mergeResult.hasConflicts(), equalTo(false));
    }
}
