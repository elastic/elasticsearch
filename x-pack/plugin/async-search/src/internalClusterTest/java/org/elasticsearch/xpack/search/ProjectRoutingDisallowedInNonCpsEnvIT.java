/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;

public class ProjectRoutingDisallowedInNonCpsEnvIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), AsyncSearch.class);
    }

    public void testDisallowProjectRouting() throws IOException {
        Request createAsyncRequest = new Request("POST", "/*,*:*/" + randomFrom("_async_search", "_search"));
        createAsyncRequest.setJsonEntity("""
            {
              "project_routing": "_alias:_origin"
            }
            """);

        ResponseException err = expectThrows(ResponseException.class, () -> getRestClient().performRequest(createAsyncRequest));
        assertThat(err.toString(), Matchers.containsString("Unknown key for a VALUE_STRING in [project_routing]"));
    }
}
