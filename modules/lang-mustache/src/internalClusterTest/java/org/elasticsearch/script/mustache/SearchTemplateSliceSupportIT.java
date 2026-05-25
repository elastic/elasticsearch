/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.Collection;
import java.util.List;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SearchTemplateSliceSupportIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MustachePlugin.class);
    }

    public void testSearchTemplateSupportsSliceParam() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/search-template-slice-support-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request index1 = new Request("POST", "/search-template-slice-support-it/_doc/1");
        index1.addParameter(SliceIndexing.PARAM_NAME, "s1");
        index1.setJsonEntity("""
            {
              "field": "a"
            }""");
        getRestClient().performRequest(index1);

        Request index2 = new Request("POST", "/search-template-slice-support-it/_doc/2");
        index2.addParameter(SliceIndexing.PARAM_NAME, "s2");
        index2.setJsonEntity("""
            {
              "field": "b"
            }""");
        getRestClient().performRequest(index2);

        getRestClient().performRequest(new Request("POST", "/search-template-slice-support-it/_refresh"));

        Request searchTemplate = new Request("GET", "/search-template-slice-support-it/_search/template");
        searchTemplate.addParameter(SliceIndexing.PARAM_NAME, "s1");
        searchTemplate.setJsonEntity("""
            {
              "source": {
                "query": {
                  "match_all": {}
                }
              }
            }""");
        Response response = getRestClient().performRequest(searchTemplate);
        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertEquals(1, ((Number) responsePath.evaluate("hits.total.value")).intValue());
        assertEquals("1", responsePath.evaluate("hits.hits.0._id"));
    }
}
