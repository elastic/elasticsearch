/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.fleet.Fleet;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FleetSearchRemoteIndicesDisallowedIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.of(Fleet.class, LocalStateCompositeXPackPlugin.class, IndexLifecycle.class).collect(Collectors.toList());
    }

    public void testEndpointsShouldRejectRemoteIndices() {
        String remoteIndex = randomAlphaOfLength(randomIntBetween(2, 10)) + ":" + randomAlphaOfLength(randomIntBetween(2, 10));
        {
            Request request = new Request("GET", "/" + remoteIndex + "/_fleet/_fleet_search");
            ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(
                responseException.getMessage(),
                Matchers.containsString("Fleet search API does not support remote indices. Found: [" + remoteIndex + "]")
            );
        }

        {
            Request request = new Request("POST", "/" + remoteIndex + "/_fleet/_fleet_msearch");
            request.setJsonEntity("{}\n{}\n");
            ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(
                responseException.getMessage(),
                Matchers.containsString("Fleet search API does not support remote indices. Found: [" + remoteIndex + "]")
            );
        }

        {
            /*
             * It is possible, however, to sneak in multiple indices and a remote index if checkpoints are not specified.
             * Unfortunately, that's the current behaviour and the Fleet team does not want us to touch it.
             */
            Request request = new Request("POST", "/foo,bar:baz/_fleet/_fleet_msearch");
            request.setJsonEntity("{}\n{}\n");
            try {
                getRestClient().performRequest(request);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        {
            // This is fine, there are no remote indices.
            Request request = new Request("POST", "/foo/_fleet/_fleet_msearch");
            request.setJsonEntity("{\"index\": \"bar*\"}\n{}\n");
            try {
                getRestClient().performRequest(request);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        {
            // This is not valid. We shouldn't be passing multiple indices.
            Request request = new Request("POST", "/foo/_fleet/_fleet_msearch");
            request.setJsonEntity("{\"index\": \"bar,baz\", \"wait_for_checkpoints\": 1 }\n{}\n");
            ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(responseException.getMessage(), Matchers.containsString("Fleet search API only supports searching a single index."));
        }

        {
            // This is not valid. We shouldn't be passing remote indices.
            Request request = new Request("POST", "/foo/_fleet/_fleet_msearch");
            request.setJsonEntity("{\"index\": \"bar:baz\", \"wait_for_checkpoints\": 1 }\n{}\n");
            ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(responseException.getMessage(), Matchers.containsString("Fleet search API does not support remote indices. Found:"));
        }
    }
}
