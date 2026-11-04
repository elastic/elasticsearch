/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class FlightCapabilityDeclarationTests extends ESTestCase {

    private static Map<String, Object> valid() {
        Map<String, Object> caps = new HashMap<>();
        caps.put("specVersions", List.of(0));
        caps.put("fragmentFormats", List.of("json"));
        caps.put("plan", List.of("scan", "limit"));
        caps.put("functions", List.of("equals"));
        caps.put("types", List.of("integer"));
        return caps;
    }

    public void testValidDeclarationPasses() {
        FlightConnectorFactory.validateDeclaration(valid());
        Map<String, Object> caps = valid();
        caps.put("limits", Map.of("maxExpressionDepth", 128, "maxFragmentBytes", 2048));
        FlightConnectorFactory.validateDeclaration(caps);
    }

    public void testMissingRequiredListIsRejected() {
        for (String required : List.of("specVersions", "fragmentFormats", "plan", "functions", "types")) {
            Map<String, Object> caps = valid();
            caps.remove(required);
            Exception e = expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
            assertThat(e.getMessage(), containsString(required));
        }
    }

    public void testReservedFunctionIsRejected() {
        Map<String, Object> caps = valid();
        caps.put("functions", List.of("equals", "concat"));
        Exception e = expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
        assertThat(e.getMessage(), containsString("concat"));
    }

    public void testLimitsOutsideTheBoundsAreRejected() {
        for (int depth : new int[] { 0, 129 }) {
            Map<String, Object> caps = valid();
            caps.put("limits", Map.of("maxExpressionDepth", depth));
            expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
        }
        for (int bytes : new int[] { 100, 2 * 1024 * 1024 + 1 }) {
            Map<String, Object> c = valid();
            c.put("limits", Map.of("maxFragmentBytes", bytes));
            expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(c));
        }
        Map<String, Object> caps = valid();
        caps.put("limits", Map.of("maxInListSize", 0));
        expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
    }

    public void testAnOptionNamedLikeAKeyEsSetsItselfIsRejected() {
        for (String name : List.of("target", "endpoint", "schema_resolution", "_datasource")) {
            Map<String, Object> caps = valid();
            caps.put("options", List.of("units", name));
            Exception e = expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
            assertThat(e.getMessage(), containsString("[" + name + "]"));
        }
        Map<String, Object> ok = valid();
        ok.put("options", List.of("units", "adjusted"));
        FlightConnectorFactory.validateDeclaration(ok);
    }
}
