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

package org.elasticsearch.test.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.usage.UsageService;
import org.junit.Before;

import java.util.Collections;

import static org.mockito.Mockito.mock;

/**
 * A common base class for Rest*ActionTests. Provides access to a {@link RestController}
 * that can be used to register individual REST actions, and test request handling.
 */
public abstract class RestActionTestCase extends ESTestCase {
    private RestController controller;

    @Before
    public void setUpController() {
        controller = new RestController(Collections.emptySet(), null,
            mock(NodeClient.class),
            new NoneCircuitBreakerService(),
            new UsageService());
    }

    /**
     * A test {@link RestController}. This controller can be used to register and delegate
     * to handlers, but uses a mock client and cannot carry out the full request.
     */
    protected RestController controller() {
        return controller;
    }

    /**
     * Sends the given request to the test controller in {@link #controller()}.
     */
    protected void dispatchRequest(RestRequest request) {
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        controller.dispatchRequest(request, channel, threadContext);
    }
}
