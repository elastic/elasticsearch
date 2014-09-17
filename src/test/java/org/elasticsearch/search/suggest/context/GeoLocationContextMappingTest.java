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
package org.elasticsearch.search.suggest.context;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.suggest.context.ContextMapping.ContextConfig;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class GeoLocationContextMappingTest extends ElasticsearchTestCase {

    @Test
    public void testThatParsingGeoPointsWorksWithCoercion() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("lat", "52").field("lon", "4").endObject();
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken();

        HashMap<String, Object> config = new HashMap<>();
        config.put("precision", 12);
        GeolocationContextMapping mapping = GeolocationContextMapping.load("foo", config);
        mapping.parseQuery("foo", parser);
    }
    

    @Test
    public void testUseWithDefaultGeoHash() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("lat", 52d).field("lon", 4d).endObject();
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken();

        String geohash = GeoHashUtils.encode(randomIntBetween(-90, +90), randomIntBetween(-180, +180));
        HashMap<String, Object> config = new HashMap<>();
        config.put("precision", 12);
        config.put("default", geohash);
        GeolocationContextMapping mapping = GeolocationContextMapping.load("foo", config);
        mapping.parseQuery("foo", parser);
    }    
    
    @Test
    public void testUseWithDefaultLatLon() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("lat", 52d).field("lon", 4d).endObject();
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken();

        HashMap<String, Object> config = new HashMap<>();
        config.put("precision", 12);
        HashMap<String, Double> pointAsMap = new HashMap<>();
        pointAsMap.put("lat", 51d);
        pointAsMap.put("lon", 0d);
        config.put("default", pointAsMap);
        GeolocationContextMapping mapping = GeolocationContextMapping.load("foo", config);
        mapping.parseQuery("foo", parser);
    } 
    
    @Test
    public void testUseWithDefaultBadLatLon() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("lat", 52d).field("lon", 4d).endObject();
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken();

        HashMap<String, Object> config = new HashMap<>();
        config.put("precision", 12);
        HashMap<String, Double> pointAsMap = new HashMap<>();
        pointAsMap.put("latitude", 51d); // invalid field names
        pointAsMap.put("longitude", 0d); // invalid field names
        config.put("default", pointAsMap);
        ElasticsearchParseException expected = null;
        try {
            GeolocationContextMapping mapping = GeolocationContextMapping.load("foo", config);
            mapping.parseQuery("foo", parser);

        } catch (ElasticsearchParseException e) {
            expected = e;
        }
        assertNotNull(expected);
    }  
    
    @Test
    public void testUseWithMultiplePrecisions() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("lat", 52d).field("lon", 4d).endObject();
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken();

        HashMap<String, Object> config = new HashMap<>();
        int numElements = randomIntBetween(1, 12);
        ArrayList<Integer> precisions = new ArrayList<>();
        for (int i = 0; i < numElements; i++) {
            precisions.add(randomIntBetween(1, 12));
        }
        config.put("precision", precisions);
        GeolocationContextMapping mapping = GeolocationContextMapping.load("foo", config);
        mapping.parseQuery("foo", parser);
    }
    
    @Test
    public void testHashcode() throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        if (randomBoolean()) {
            config.put("precision", Arrays.asList(1, 2, 3, 4));
        } else {
            config.put("precision", randomIntBetween(1, 12));
        }
        if (randomBoolean()) {
            HashMap<String, Double> pointAsMap = new HashMap<>();
            pointAsMap.put("lat", 51d);
            pointAsMap.put("lon", 0d);
            config.put("default", pointAsMap);
        }
        GeolocationContextMapping mapping = GeolocationContextMapping.load("foo", config);
        GeolocationContextMapping mapping2 = GeolocationContextMapping.load("foo", config);

        assertEquals(mapping, mapping2);
        assertEquals(mapping.hashCode(), mapping2.hashCode());
    }

    @Test
    public void testUseWithBadGeoContext() throws Exception {
        double lon = 4d;
        String badLat = "W";
        XContentBuilder builder = jsonBuilder().startObject().startArray("location").value(4d).value(badLat).endArray().endObject();
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken(); // start of object
        parser.nextToken(); // "location" field name
        parser.nextToken(); // array

        HashMap<String, Object> config = new HashMap<>();
        config.put("precision", randomIntBetween(1, 12));
        GeolocationContextMapping mapping = GeolocationContextMapping.load("foo", config);
        ElasticsearchParseException expected = null;
        try {
            ContextConfig geoconfig = mapping.parseContext(null, parser);
        } catch (ElasticsearchParseException e) {
            expected = e;
        }
        assertNotNull(expected);
    }

    @Test
    public void testUseWithLonLatGeoContext() throws Exception {
        double lon = 4d;
        double lat = 52d;
        XContentBuilder builder = jsonBuilder().startObject().startArray("location").value(lon).value(lat).endArray().endObject();
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken(); // start of object
        parser.nextToken(); // "location" field name
        parser.nextToken(); // array

        HashMap<String, Object> config = new HashMap<>();
        config.put("precision", randomIntBetween(1, 12));
        GeolocationContextMapping mapping = GeolocationContextMapping.load("foo", config);
        mapping.parseContext(null, parser);
    }

    public void testUseWithMultiGeoHashGeoContext() throws Exception {
        String geohash1 = GeoHashUtils.encode(randomIntBetween(-90, +90), randomIntBetween(-180, +180));
        String geohash2 = GeoHashUtils.encode(randomIntBetween(-90, +90), randomIntBetween(-180, +180));
        XContentBuilder builder = jsonBuilder().startObject().startArray("location").value(geohash1).value(geohash2).endArray().endObject();
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken(); // start of object
        parser.nextToken(); // "location" field name
        parser.nextToken(); // array

        HashMap<String, Object> config = new HashMap<>();
        config.put("precision", randomIntBetween(1, 12));
        GeolocationContextMapping mapping = GeolocationContextMapping.load("foo", config);
        ContextConfig parsedContext = mapping.parseContext(null, parser);
    }

}
