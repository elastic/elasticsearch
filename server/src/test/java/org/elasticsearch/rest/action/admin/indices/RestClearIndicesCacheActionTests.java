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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;

public class RestClearIndicesCacheActionTests extends ESTestCase {

    public void testRequestCacheSet() throws Exception {
        final HashMap<String, String> params = new HashMap<>();
        params.put("request", "true");
        final RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
                                                           .withParams(params).build();
        ClearIndicesCacheRequest cacheRequest = new ClearIndicesCacheRequest();
        cacheRequest = RestClearIndicesCacheAction.fromRequest(restRequest, cacheRequest);
        assertThat(cacheRequest.requestCache(), equalTo(true));
    }
}
