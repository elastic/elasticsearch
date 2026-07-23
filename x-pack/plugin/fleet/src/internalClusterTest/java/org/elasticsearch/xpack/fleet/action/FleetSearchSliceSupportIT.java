/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.fleet.Fleet;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class FleetSearchSliceSupportIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.of(Fleet.class, LocalStateCompositeXPackPlugin.class, IndexLifecycle.class).collect(Collectors.toList());
    }

    public void testFleetSearchSupportsSliceParam() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/fleet-slice-support-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request index1 = new Request("POST", "/fleet-slice-support-it/_doc/1");
        index1.addParameter(SliceIndexing.PARAM_NAME, "s1");
        index1.setJsonEntity("""
            {
              "field": "a"
            }""");
        getRestClient().performRequest(index1);

        Request index2 = new Request("POST", "/fleet-slice-support-it/_doc/2");
        index2.addParameter(SliceIndexing.PARAM_NAME, "s2");
        index2.setJsonEntity("""
            {
              "field": "b"
            }""");
        getRestClient().performRequest(index2);

        getRestClient().performRequest(new Request("POST", "/fleet-slice-support-it/_refresh"));

        Request search = new Request("GET", "/fleet-slice-support-it/_fleet/_fleet_search");
        search.addParameter(SliceIndexing.PARAM_NAME, "s1");
        search.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              }
            }""");
        Response response = getRestClient().performRequest(search);
        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertEquals(1, ((Number) responsePath.evaluate("hits.total.value")).intValue());
        assertEquals("1", responsePath.evaluate("hits.hits.0._id"));
    }
}
