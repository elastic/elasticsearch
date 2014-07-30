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


package org.elasticsearch.search.sort;


import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.elasticsearch.test.TestSearchContext;
import org.junit.Test;

public class SortParserTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testGeoDistanceSortParserManyPointsNoException() throws Exception {
        String mapping = "{\"type\": {\"properties\": {\"location\": {\"type\": \"geo_point\"}}}}";
        IndexService indexService = createIndex("testidx", ImmutableSettings.settingsBuilder().build(), "type", mapping);
        TestSearchContext context = (TestSearchContext)createSearchContext(indexService);
        context.setTypes("type");

        String sortString = "{\n" +
                "        \"location\": [\n" +
                "          [\n" +
                "            1.2,\n" +
                "            3\n" +
                "          ],\n" +
                "          [\n" +
                "            5,\n" +
                "            6\n" +
                "          ]\n" +
                "        ],\n" +
                "        \"order\": \"desc\",\n" +
                "        \"unit\": \"km\",\n" +
                "        \"sort_mode\": \"max\"\n" +
                "      }\n" +
                "    }";
        XContentParser parser = XContentHelper.createParser(new BytesArray(sortString));
        parser.nextToken();
        GeoDistanceSortParser geoParser = new GeoDistanceSortParser();
        geoParser.parse(parser, context);

        sortString = "{\n" +
                "        \"location\": [\n" +
                "          {\n" +
                "            \"lat\":1.2,\n" +
                "            \"lon\":3\n" +
                "          },\n" +
                "          {\n" +
                "             \"lat\":1.2,\n" +
                "            \"lon\":3\n" +
                "          }\n" +
                "        ],\n" +
                "        \"order\": \"desc\",\n" +
                "        \"unit\": \"km\",\n" +
                "        \"sort_mode\": \"max\"\n" +
                "    }";
        parser = XContentHelper.createParser(new BytesArray(sortString));
        parser.nextToken();
        geoParser = new GeoDistanceSortParser();
        geoParser.parse(parser, context);

        sortString = "{\n" +
                "        \"location\": [\n" +
                "          \"1,2\",\n" +
                "          \"3,4\"\n" +
                "        ],\n" +
                "        \"order\": \"desc\",\n" +
                "        \"unit\": \"km\",\n" +
                "        \"sort_mode\": \"max\"\n" +
                "    }";
        parser = XContentHelper.createParser(new BytesArray(sortString));
        parser.nextToken();
        geoParser = new GeoDistanceSortParser();
        geoParser.parse(parser, context);

        sortString = "{\n" +
                "        \"location\": [\n" +
                "          \"s3y0zh7w1z0g\",\n" +
                "          \"s6wjr4et3f8v\"\n" +
                "        ],\n" +
                "        \"order\": \"desc\",\n" +
                "        \"unit\": \"km\",\n" +
                "        \"sort_mode\": \"max\"\n" +
                "    }";
        parser = XContentHelper.createParser(new BytesArray(sortString));
        parser.nextToken();
        geoParser = new GeoDistanceSortParser();
        geoParser.parse(parser, context);

        sortString = "{\n" +
                "        \"location\": " +
                "          [\n" +
                "            1.2,\n" +
                "            3\n" +
                "          ],\n" +
                "        \"order\": \"desc\",\n" +
                "        \"unit\": \"km\",\n" +
                "        \"sort_mode\": \"max\"\n" +
                "      }\n" +
                "    }";
        parser = XContentHelper.createParser(new BytesArray(sortString));
        parser.nextToken();
        geoParser = new GeoDistanceSortParser();
        geoParser.parse(parser, context);

        sortString = "{\n" +
                "        \"location\": \n" +
                "          {\n" +
                "            \"lat\":1.2,\n" +
                "            \"lon\":3\n" +
                "          }," +
                "        \"order\": \"desc\",\n" +
                "        \"unit\": \"km\",\n" +
                "        \"sort_mode\": \"max\"\n" +
                "    }";
        parser = XContentHelper.createParser(new BytesArray(sortString));
        parser.nextToken();
        geoParser = new GeoDistanceSortParser();
        geoParser.parse(parser, context);

        sortString = "{\n" +
                "        \"location\": \"1,2\"," +
                "        \"order\": \"desc\",\n" +
                "        \"unit\": \"km\",\n" +
                "        \"sort_mode\": \"max\"\n" +
                "    }";
        parser = XContentHelper.createParser(new BytesArray(sortString));
        parser.nextToken();
        geoParser = new GeoDistanceSortParser();
        geoParser.parse(parser, context);

        sortString = "{\n" +
                "        \"location\": \"s3y0zh7w1z0g\",\n" +
                "        \"order\": \"desc\",\n" +
                "        \"unit\": \"km\",\n" +
                "        \"sort_mode\": \"max\"\n" +
                "    }";
        parser = XContentHelper.createParser(new BytesArray(sortString));
        parser.nextToken();
        geoParser = new GeoDistanceSortParser();
        geoParser.parse(parser, context);
    }
}
