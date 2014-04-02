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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

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

}
