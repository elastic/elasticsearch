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

package org.elasticsearch.search.scroll;

import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RestSearchScrollActionTests extends ESTestCase {

    public void testParseSearchScrollRequestWithInvalidJsonThrowsException() throws Exception {
        RestSearchScrollAction action = new RestSearchScrollAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{invalid_json}"), XContentType.JSON).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse request body"));
    }

    public void testBodyParamsOverrideQueryStringParams() throws Exception {
        NodeClient nodeClient = mock(NodeClient.class);
        doNothing().when(nodeClient).searchScroll(any(), any());

        RestSearchScrollAction action = new RestSearchScrollAction();
        Map<String, String> params = new HashMap<>();
        params.put("scroll_id", "QUERY_STRING");
        params.put("scroll", "1000m");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
                .withParams(params)
                .withContent(new BytesArray("{\"scroll_id\":\"BODY\", \"scroll\":\"1m\"}"), XContentType.JSON).build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        action.handleRequest(request, channel, nodeClient);

        ArgumentCaptor<SearchScrollRequest> argument = ArgumentCaptor.forClass(SearchScrollRequest.class);
        verify(nodeClient).searchScroll(argument.capture(), anyObject());
        SearchScrollRequest searchScrollRequest = argument.getValue();
        assertEquals("BODY", searchScrollRequest.scrollId());
        assertEquals("1m", searchScrollRequest.scroll().keepAlive().getStringRep());
    }
}
