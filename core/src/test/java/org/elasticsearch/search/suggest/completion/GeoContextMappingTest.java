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

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.suggest.completion.context.*;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;

public class GeoContextMappingTest extends ElasticsearchTestCase {

    @Test
    public void testParsingQueryContextBasic() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().value("ezs42e44yx96");
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        ContextMapping.QueryContexts<GeoQueryContext> queryContexts = mapping.parseQueryContext("geo", parser);
        Iterator<GeoQueryContext> iterator = queryContexts.iterator();
        assertTrue(iterator.hasNext());
        GeoQueryContext queryContext = iterator.next();
        assertThat(queryContext.geoHash.toString(), equalTo("ezs42e44yx96"));
        assertThat(queryContext.boost, equalTo(1));
        assertThat(queryContext.neighbours.length, equalTo(0));
    }

    @Test
    public void testParsingQueryContextGeoPoint() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("lat", 23.654242)
                .field("lon", 90.047153)
                .endObject();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        ContextMapping.QueryContexts<GeoQueryContext> queryContexts = mapping.parseQueryContext("geo", parser);
        Iterator<GeoQueryContext> iterator = queryContexts.iterator();
        assertTrue(iterator.hasNext());
        GeoQueryContext queryContext = iterator.next();
        assertThat(queryContext.geoHash.toString(), equalTo("wh0n9447fwrc"));
        assertThat(queryContext.boost, equalTo(1));
        assertThat(queryContext.neighbours.length, equalTo(0));
    }

    @Test
    public void testParsingQueryContextObject() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject("context")
                .field("lat", 23.654242)
                .field("lon", 90.047153)
                .endObject()
                .field("boost", 10)
                .array("neighbours", 1, 2, 3)
                .endObject();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        ContextMapping.QueryContexts<GeoQueryContext> queryContexts = mapping.parseQueryContext("geo", parser);
        Iterator<GeoQueryContext> iterator = queryContexts.iterator();
        assertTrue(iterator.hasNext());
        GeoQueryContext queryContext = iterator.next();
        assertThat(queryContext.geoHash.toString(), equalTo("wh0n9447fwrc"));
        assertThat(queryContext.boost, equalTo(10));
        assertThat(queryContext.neighbours.length, equalTo(3));
    }

    @Test
    public void testParsingQueryContextObjectArray() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startArray()
                .startObject()
                .startObject("context")
                .field("lat", 23.654242)
                .field("lon", 90.047153)
                .endObject()
                .field("boost", 10)
                .array("neighbours", 1, 2, 3)
                .endObject()
                .startObject()
                .startObject("context")
                .field("lat", 22.337374)
                .field("lon", 92.112583)
                .endObject()
                .field("boost", 2)
                .array("neighbours", 3)
                .endObject()
                .endArray();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        ContextMapping.QueryContexts<GeoQueryContext> queryContexts = mapping.parseQueryContext("geo", parser);
        Iterator<GeoQueryContext> iterator = queryContexts.iterator();
        assertTrue(iterator.hasNext());
        GeoQueryContext queryContext = iterator.next();
        assertThat(queryContext.geoHash.toString(), equalTo("wh0n9447fwrc"));
        assertThat(queryContext.boost, equalTo(10));
        assertThat(queryContext.neighbours.length, equalTo(3));
        assertTrue(iterator.hasNext());
        queryContext = iterator.next();
        assertThat(queryContext.geoHash.toString(), equalTo("w5cx046kdu24"));
        assertThat(queryContext.boost, equalTo(2));
        assertThat(queryContext.neighbours.length, equalTo(1));
    }
}
