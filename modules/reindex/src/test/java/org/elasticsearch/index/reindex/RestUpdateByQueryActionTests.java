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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;

import org.junit.Before;
import java.io.IOException;

import static java.util.Collections.emptyList;

public class RestUpdateByQueryActionTests extends RestActionTestCase {

    private RestUpdateByQueryAction action;

    @Before
    public void setUpAction() {
        action = new RestUpdateByQueryAction(Settings.EMPTY, controller());
    }

    public void testTypeInPath() throws IOException  {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.POST)
            .withPath("/some_index/some_type/_update_by_query")
            .build();
        dispatchRequest(request);

        // checks the type in the URL is propagated correctly to the request object
        // only works after the request is dispatched, so its params are filled from url.
        UpdateByQueryRequest ubqRequest = action.buildRequest(request);
        assertArrayEquals(new String[]{"some_type"}, ubqRequest.getDocTypes());

        // RestUpdateByQueryAction itself doesn't check for a deprecated type usage
        // checking here for a deprecation from its internal search request
        assertWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testParseEmpty() throws IOException {
        UpdateByQueryRequest request = action.buildRequest(new FakeRestRequest.Builder(new NamedXContentRegistry(emptyList())).build());
        assertEquals(AbstractBulkByScrollRequest.SIZE_ALL_MATCHES, request.getSize());
        assertEquals(AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE, request.getSearchRequest().source().size());
    }
}
