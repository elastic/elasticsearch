/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.usage.UsageService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class RestHttpResponseHeadersTests extends ESTestCase {

    /**
     * For requests to a valid REST endpoint using an unsupported HTTP method,
     * verify that a 405 HTTP response code is returned, and that the response
     * 'Allow' header includes a list of valid HTTP methods for the endpoint
     * (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-10.4.6">HTTP/1.1 -
     * 10.4.6 - 405 Method Not Allowed</a>).
     */
    public void testUnsupportedMethodResponseHttpHeader() throws Exception {

        /*
         * Generate a random set of candidate valid HTTP methods to register
         * with the test RestController endpoint. Enums are returned in the
         * order they are declared, so the first step is to shuffle the HTTP
         * method list, passing in the RandomizedContext's Random instance,
         * before picking out a candidate sublist.
         */
        List<RestRequest.Method> validHttpMethodArray = new ArrayList<>(Arrays.asList(RestRequest.Method.values()));
        validHttpMethodArray.remove(RestRequest.Method.OPTIONS);
        Collections.shuffle(validHttpMethodArray, random());

        /*
         * The upper bound of the potential sublist is one less than the size of
         * the array, so we are guaranteed at least one invalid method to test.
         */
        validHttpMethodArray = validHttpMethodArray.subList(0, randomIntBetween(1, validHttpMethodArray.size() - 1));
        assert (validHttpMethodArray.size() > 0);
        assert (validHttpMethodArray.size() < RestRequest.Method.values().length);

        /*
         * Generate an inverse list of one or more candidate invalid HTTP
         * methods, so we have a candidate method to fire at the test endpoint.
         */
        List<RestRequest.Method> invalidHttpMethodArray = new ArrayList<>(Arrays.asList(RestRequest.Method.values()));
        invalidHttpMethodArray.removeAll(validHttpMethodArray);
        // Remove OPTIONS, or else we'll get a 200 instead of 405
        invalidHttpMethodArray.remove(RestRequest.Method.OPTIONS);
        assert (invalidHttpMethodArray.size() > 0);

        // Initialize test candidate RestController
        CircuitBreakerService circuitBreakerService = new HierarchyCircuitBreakerService(
            Settings.EMPTY,
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        UsageService usageService = new UsageService();
        RestController restController = new RestController(
            Collections.emptySet(),
            null,
            null,
            circuitBreakerService,
            usageService,
            Tracer.NOOP
        );

        // A basic RestHandler handles requests to the endpoint
        RestHandler restHandler = (request, channel, client) -> channel.sendResponse(new RestResponse(RestStatus.OK, ""));

        // Register valid test handlers with test RestController
        for (RestRequest.Method method : validHttpMethodArray) {
            restController.registerHandler(new Route(method, "/"), restHandler);
        }

        // Generate a test request with an invalid HTTP method
        FakeRestRequest.Builder fakeRestRequestBuilder = new FakeRestRequest.Builder(xContentRegistry());
        fakeRestRequestBuilder.withMethod(invalidHttpMethodArray.get(0));
        RestRequest restRequest = fakeRestRequestBuilder.build();

        // Send the request and verify the response status code
        FakeRestChannel restChannel = new FakeRestChannel(restRequest, false, 1);
        restController.dispatchRequest(restRequest, restChannel, new ThreadContext(Settings.EMPTY));
        assertThat(restChannel.capturedResponse().status().getStatus(), is(405));

        /*
         * Verify the response allow header contains the valid methods for the
         * test endpoint
         */
        assertThat(restChannel.capturedResponse().getHeaders().get("Allow"), notNullValue());
        String responseAllowHeader = restChannel.capturedResponse().getHeaders().get("Allow").get(0);
        List<String> responseAllowHeaderArray = Arrays.asList(responseAllowHeader.split(","));
        assertThat(responseAllowHeaderArray.size(), is(validHttpMethodArray.size()));
        assertThat(responseAllowHeaderArray, containsInAnyOrder(getMethodNameStringArray(validHttpMethodArray).toArray()));
    }

    /**
     * Convert an RestRequest.Method array to a String array, so it can be
     * compared with the expected 'Allow' header String array.
     */
    private List<String> getMethodNameStringArray(List<RestRequest.Method> methodArray) {
        return methodArray.stream().map(method -> method.toString()).toList();
    }

}
