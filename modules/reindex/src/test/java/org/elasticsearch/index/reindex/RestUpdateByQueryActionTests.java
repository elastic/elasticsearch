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

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;

public class RestUpdateByQueryActionTests extends ESTestCase {
    private static NamedXContentRegistry xContentRegistry;
    private static RestUpdateByQueryAction action;

    @BeforeClass
    public static void init() {
        xContentRegistry = new NamedXContentRegistry(new SearchModule(Settings.EMPTY, false, emptyList()).getNamedXContents());
        action = new RestUpdateByQueryAction(Settings.EMPTY, mock(RestController.class));
    }

    @AfterClass
    public static void cleanup() {
        xContentRegistry = null;
        action = null;
    }

    public void testParseEmpty() throws IOException {
        RestUpdateByQueryAction action = new RestUpdateByQueryAction(Settings.EMPTY, mock(RestController.class));
        UpdateByQueryRequest request = action.buildRequest(new FakeRestRequest.Builder(new NamedXContentRegistry(emptyList()))
                .build());
        assertEquals(AbstractBulkByScrollRequest.SIZE_ALL_MATCHES, request.getSize());
        assertEquals(AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE, request.getSearchRequest().source().size());
    }

    public void testParseWithSize() throws IOException {
        {
            FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
                .withPath("index/type/_update_by_query")
                .withParams(singletonMap("size", "2"))
                .build();
            UpdateByQueryRequest request = action.buildRequest(restRequest);
            assertEquals(2, request.getSize());
        }
        {
            final String requestContent = "{\"query\" : {\"match_all\": {}}, \"size\": 2 }";
            FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
                .withMethod(RestRequest.Method.POST)
                .withPath("index/type/_update_by_query")
                .withContent(new BytesArray(requestContent), XContentType.JSON)
                .build();
            UpdateByQueryRequest request = action.buildRequest(restRequest);
            assertEquals(2, request.getSize());
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }
}
