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
    }
}
