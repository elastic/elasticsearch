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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.graph.GraphExploreRequest;
import org.elasticsearch.client.graph.Hop;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class GraphRequestConvertersTests extends ESTestCase {

    public void testGraphExplore() throws Exception {
        Map<String, String> expectedParams = new HashMap<>();

        GraphExploreRequest graphExploreRequest = new GraphExploreRequest();
        graphExploreRequest.sampleDiversityField("diversity");
        graphExploreRequest.indices("index1", "index2");
        int timeout = randomIntBetween(10000, 20000);
        graphExploreRequest.timeout(TimeValue.timeValueMillis(timeout));
        graphExploreRequest.useSignificance(randomBoolean());
        int numHops = randomIntBetween(1, 5);
        for (int i = 0; i < numHops; i++) {
            int hopNumber = i + 1;
            QueryBuilder guidingQuery = null;
            if (randomBoolean()) {
                guidingQuery = new TermQueryBuilder("field" + hopNumber, "value" + hopNumber);
            }
            Hop hop = graphExploreRequest.createNextHop(guidingQuery);
            hop.addVertexRequest("field" + hopNumber);
            hop.getVertexRequest(0).addInclude("value" + hopNumber, hopNumber);
        }
        Request request = GraphRequestConverters.explore(graphExploreRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/index1,index2/_graph/explore", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertThat(request.getEntity().getContentType().getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        RequestConvertersTests.assertToXContentBody(graphExploreRequest, request.getEntity());
    }
}
