/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        final RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        ClearIndicesCacheRequest cacheRequest = new ClearIndicesCacheRequest();
        cacheRequest = RestClearIndicesCacheAction.fromRequest(restRequest, cacheRequest);
        assertThat(cacheRequest.requestCache(), equalTo(true));
    }
}
