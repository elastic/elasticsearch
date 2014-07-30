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
package org.elasticsearch.action.percolate;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.*;

/**
 */
public class MultiPercolatorRequestTests extends ElasticsearchTestCase {

    @Test
    public void testParseBulkRequests() throws Exception {
        byte[] data = Streams.copyToBytesFromClasspath("/org/elasticsearch/action/percolate/mpercolate1.json");
        MultiPercolateRequest request = new MultiPercolateRequest().add(data, 0, data.length, false);

        assertThat(request.requests().size(), equalTo(6));
        PercolateRequest percolateRequest = request.requests().get(0);
        assertThat(percolateRequest.indices()[0], equalTo("my-index1"));
        assertThat(percolateRequest.documentType(), equalTo("my-type1"));
        assertThat(percolateRequest.routing(), equalTo("my-routing-1"));
        assertThat(percolateRequest.preference(), equalTo("_local"));
        assertThat(percolateRequest.indicesOptions(), equalTo(IndicesOptions.strictExpandOpenAndForbidClosed()));
        assertThat(percolateRequest.onlyCount(), equalTo(false));
        assertThat(percolateRequest.getRequest(), nullValue());
        assertThat(percolateRequest.source(), notNullValue());
        Map sourceMap = XContentFactory.xContent(percolateRequest.source()).createParser(percolateRequest.source()).map();
        assertThat(sourceMap.get("doc"), equalTo((Object) MapBuilder.newMapBuilder().put("field1", "value1").map()));

        percolateRequest = request.requests().get(1);
        assertThat(percolateRequest.indices()[0], equalTo("my-index2"));
        assertThat(percolateRequest.indices()[1], equalTo("my-index3"));
        assertThat(percolateRequest.documentType(), equalTo("my-type1"));
        assertThat(percolateRequest.routing(), equalTo("my-routing-1"));
        assertThat(percolateRequest.preference(), equalTo("_local"));
        assertThat(percolateRequest.indicesOptions(), equalTo(IndicesOptions.fromOptions(true, true, true, false, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(percolateRequest.onlyCount(), equalTo(false));
        assertThat(percolateRequest.getRequest(), nullValue());
        assertThat(percolateRequest.source(), notNullValue());
        sourceMap =  XContentFactory.xContent(percolateRequest.source()).createParser(percolateRequest.source()).map();
        assertThat(sourceMap.get("doc"), equalTo((Object)MapBuilder.newMapBuilder().put("field1", "value2").map()));

        percolateRequest = request.requests().get(2);
        assertThat(percolateRequest.indices()[0], equalTo("my-index4"));
        assertThat(percolateRequest.indices()[1], equalTo("my-index5"));
        assertThat(percolateRequest.documentType(), equalTo("my-type1"));
        assertThat(percolateRequest.routing(), equalTo("my-routing-1"));
        assertThat(percolateRequest.preference(), equalTo("_local"));
        assertThat(percolateRequest.indicesOptions(), equalTo(IndicesOptions.fromOptions(false, true, true, true, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(percolateRequest.onlyCount(), equalTo(true));
        assertThat(percolateRequest.getRequest(), nullValue());
        assertThat(percolateRequest.source(), notNullValue());
        sourceMap =  XContentFactory.xContent(percolateRequest.source()).createParser(percolateRequest.source()).map();
        assertThat(sourceMap.get("doc"), equalTo((Object)MapBuilder.newMapBuilder().put("field1", "value3").map()));

        percolateRequest = request.requests().get(3);
        assertThat(percolateRequest.indices()[0], equalTo("my-index6"));
        assertThat(percolateRequest.documentType(), equalTo("my-type1"));
        assertThat(percolateRequest.routing(), equalTo("my-routing-1"));
        assertThat(percolateRequest.preference(), equalTo("_local"));
        assertThat(percolateRequest.indicesOptions(), equalTo(IndicesOptions.fromOptions(false, true, true, true, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(percolateRequest.onlyCount(), equalTo(false));
        assertThat(percolateRequest.getRequest(), notNullValue());
        assertThat(percolateRequest.getRequest().id(), equalTo("1"));
        assertThat(percolateRequest.getRequest().type(), equalTo("my-type1"));
        assertThat(percolateRequest.getRequest().index(), equalTo("my-index6"));
        assertThat(percolateRequest.getRequest().routing(), equalTo("my-routing-1"));
        assertThat(percolateRequest.getRequest().preference(), equalTo("_local"));

        percolateRequest = request.requests().get(4);
        assertThat(percolateRequest.indices()[0], equalTo("my-index7"));
        assertThat(percolateRequest.documentType(), equalTo("my-type1"));
        assertThat(percolateRequest.routing(), equalTo("my-routing-1"));
        assertThat(percolateRequest.preference(), equalTo("_local"));
        assertThat(percolateRequest.indicesOptions(), equalTo(IndicesOptions.strictExpandOpenAndForbidClosed()));
        assertThat(percolateRequest.onlyCount(), equalTo(true));
        assertThat(percolateRequest.getRequest(), notNullValue());
        assertThat(percolateRequest.getRequest().id(), equalTo("2"));
        assertThat(percolateRequest.getRequest().type(), equalTo("my-type1"));
        assertThat(percolateRequest.getRequest().index(), equalTo("my-index7"));
        assertThat(percolateRequest.getRequest().routing(), equalTo("my-routing-1"));
        assertThat(percolateRequest.getRequest().preference(), equalTo("_local"));

        percolateRequest = request.requests().get(5);
        assertThat(percolateRequest.indices()[0], equalTo("my-index8"));
        assertThat(percolateRequest.documentType(), equalTo("my-type1"));
        assertThat(percolateRequest.routing(), equalTo("my-routing-1"));
        assertThat(percolateRequest.preference(), equalTo("primary"));
        assertThat(percolateRequest.indicesOptions(), equalTo(IndicesOptions.strictExpandOpenAndForbidClosed()));
        assertThat(percolateRequest.onlyCount(), equalTo(false));
        assertThat(percolateRequest.getRequest(), nullValue());
        assertThat(percolateRequest.source(), notNullValue());
        sourceMap =  XContentFactory.xContent(percolateRequest.source()).createParser(percolateRequest.source()).map();
        assertThat(sourceMap.get("doc"), equalTo((Object)MapBuilder.newMapBuilder().put("field1", "value4").map()));
    }

    @Test
    public void testParseBulkRequests_defaults() throws Exception {
        byte[] data = Streams.copyToBytesFromClasspath("/org/elasticsearch/action/percolate/mpercolate2.json");
        MultiPercolateRequest request = new MultiPercolateRequest();
        request.indices("my-index1").documentType("my-type1").indicesOptions(IndicesOptions.lenientExpandOpen());
        request.add(data, 0, data.length, false);

        assertThat(request.requests().size(), equalTo(3));
        PercolateRequest percolateRequest = request.requests().get(0);
        assertThat(percolateRequest.indices()[0], equalTo("my-index1"));
        assertThat(percolateRequest.documentType(), equalTo("my-type1"));
        assertThat(percolateRequest.routing(), equalTo("my-routing-1"));
        assertThat(percolateRequest.preference(), equalTo("_local"));
        assertThat(percolateRequest.indicesOptions(), equalTo(IndicesOptions.lenientExpandOpen()));
        assertThat(percolateRequest.onlyCount(), equalTo(false));
        assertThat(percolateRequest.getRequest(), nullValue());
        assertThat(percolateRequest.source(), notNullValue());
        Map sourceMap = XContentFactory.xContent(percolateRequest.source()).createParser(percolateRequest.source()).map();
        assertThat(sourceMap.get("doc"), equalTo((Object) MapBuilder.newMapBuilder().put("field1", "value1").map()));

        percolateRequest = request.requests().get(1);
        assertThat(percolateRequest.indices()[0], equalTo("my-index1"));
        assertThat(percolateRequest.documentType(), equalTo("my-type1"));
        assertThat(percolateRequest.routing(), equalTo("my-routing-1"));
        assertThat(percolateRequest.preference(), equalTo("_local"));
        assertThat(percolateRequest.indicesOptions(), equalTo(IndicesOptions.lenientExpandOpen()));
        assertThat(percolateRequest.onlyCount(), equalTo(false));
        assertThat(percolateRequest.getRequest(), nullValue());
        assertThat(percolateRequest.source(), notNullValue());
        sourceMap =  XContentFactory.xContent(percolateRequest.source()).createParser(percolateRequest.source()).map();
        assertThat(sourceMap.get("doc"), equalTo((Object)MapBuilder.newMapBuilder().put("field1", "value2").map()));

        percolateRequest = request.requests().get(2);
        assertThat(percolateRequest.indices()[0], equalTo("my-index1"));
        assertThat(percolateRequest.documentType(), equalTo("my-type1"));
        assertThat(percolateRequest.indicesOptions(), equalTo(IndicesOptions.lenientExpandOpen()));
        assertThat(percolateRequest.onlyCount(), equalTo(false));
        assertThat(percolateRequest.getRequest(), nullValue());
        assertThat(percolateRequest.source(), notNullValue());
        sourceMap =  XContentFactory.xContent(percolateRequest.source()).createParser(percolateRequest.source()).map();
        assertThat(sourceMap.get("doc"), equalTo((Object)MapBuilder.newMapBuilder().put("field1", "value3").map()));
    }

}
