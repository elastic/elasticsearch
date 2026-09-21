/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.resourceexhaustion;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Base class for Painless allocation-limit REST tests. Each subclass provides its own
 * {@code @ClassRule} cluster with the relevant
 * {@code script.painless.max_allocation_bytes.context.<name>.limit} node setting, which is
 * {@code NodeScope} and therefore must be set at startup rather than dynamically.
 */
public abstract class ResourceExhaustionPainlessTestCase extends ESRestTestCase {

    /**
     * Asserts that a {@link ResponseException} was produced when a Painless script exceeded
     * its per-execution allocation budget.
     *
     * <p>The {@code painless_error} cause may be nested at different depths depending on the
     * script context: search contexts wrap it inside a {@code search_phase_execution_exception}
     * via {@code failed_shards}, while the update context wraps it inside an
     * {@code illegal_argument_exception}. This method searches the full error body recursively
     * so it works regardless of the outer wrapper.
     */
    protected void assertAllocationLimitExceeded(ResponseException e) throws IOException {
        assertAllocationLimitExceeded(entityAsMap(e.getResponse()));
    }

    /**
     * Variant that accepts a pre-parsed response body. Use this when the response entity must
     * be consumed on a background thread (before the HTTP connection is returned to the pool)
     * and the parsed map is then passed back to the test thread for assertion.
     */
    protected void assertAllocationLimitExceeded(Map<String, Object> body) {
        assertTrue("expected a painless allocation limit error in response body, but got: " + body, containsPainlessAllocationError(body));
    }

    @SuppressWarnings("unchecked")
    private static boolean containsPainlessAllocationError(Object obj) {
        if (obj instanceof Map<?, ?> map) {
            if ("painless_error".equals(map.get("type"))
                && map.get("reason") instanceof String reason
                && reason.contains("script allocation limit exceeded")) {
                return true;
            }
            for (Object value : ((Map<String, Object>) map).values()) {
                if (containsPainlessAllocationError(value)) {
                    return true;
                }
            }
        } else if (obj instanceof List<?> list) {
            for (Object item : list) {
                if (containsPainlessAllocationError(item)) {
                    return true;
                }
            }
        }
        return false;
    }
}
