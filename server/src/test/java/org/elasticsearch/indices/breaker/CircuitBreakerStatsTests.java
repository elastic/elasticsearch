/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentObject;

import static org.hamcrest.Matchers.equalTo;

public class CircuitBreakerStatsTests extends ESTestCase {

    public void testStringRepresentations() {
        final var circuitBreakerStats = new CircuitBreakerStats("t", 1L, 2L, 1.0, 3L);
        assertThat(circuitBreakerStats.toString(), equalTo("[t,limit=1/1b,estimated=2/2b,overhead=1.0,tripped=3]"));
        assertThat(toJson(circuitBreakerStats), equalTo("""
            {"t":{"limit_size_in_bytes":1,"limit_size":"1b","estimated_size_in_bytes":2,"estimated_size":"2b","overhead":1.0,"tripped":3}}\
            """));
    }

    public void testStringRepresentationPermitsNegativeOne() {
        final var circuitBreakerStats = new CircuitBreakerStats("t", -1L, -1L, 1.0, 3L);
        assertThat(circuitBreakerStats.toString(), equalTo("[t,limit=-1/-1b,estimated=-1/-1b,overhead=1.0,tripped=3]"));
        assertThat(toJson(circuitBreakerStats), equalTo("""
            {"t":{"limit_size_in_bytes":-1,"limit_size":"-1b","estimated_size_in_bytes":-1,"estimated_size":"-1b",\
            "overhead":1.0,"tripped":3}}"""));
    }

    public void testStringRepresentationsWithNegativeStats() {
        try {
            HierarchyCircuitBreakerService.permitNegativeValues = true;
            final var circuitBreakerStats = new CircuitBreakerStats("t", -2L, -3L, 1.0, 3L);
            assertThat(circuitBreakerStats.toString(), equalTo("[t,limit=-2,estimated=-3,overhead=1.0,tripped=3]"));
            assertThat(toJson(circuitBreakerStats), equalTo("""
                {"t":{"limit_size_in_bytes":-2,"limit_size":"","estimated_size_in_bytes":-3,"estimated_size":"",\
                "overhead":1.0,"tripped":3}}"""));
        } finally {
            HierarchyCircuitBreakerService.permitNegativeValues = false;
        }
    }

    private static String toJson(CircuitBreakerStats circuitBreakerStats) {
        return Strings.toString((ToXContentObject) (builder, params) -> {
            builder.startObject();
            circuitBreakerStats.toXContent(builder, params);
            builder.endObject();
            return builder;
        }, false, true);
    }
}
