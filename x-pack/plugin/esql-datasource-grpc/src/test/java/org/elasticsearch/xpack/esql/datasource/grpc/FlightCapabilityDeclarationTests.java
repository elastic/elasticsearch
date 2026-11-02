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

    /** Aggregates have their own property in version 1, so the names stay reserved in `functions`, and in a declaration of version 0. */
    public void testAggregateNamesAreNeverFunctionsAndVersionZeroHasNoAggregates() {
        for (String name : List.of("count", "sum", "min", "max", "avg", "bucket")) {
            Map<String, Object> caps = v1();
            caps.put("functions", List.of("equals", name));
            Exception e = expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
            assertThat(e.getMessage(), containsString(name));
        }
        Map<String, Object> operator = valid();
        operator.put("plan", List.of("scan", "aggregate"));
        expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(operator));
        Map<String, Object> property = valid();
        property.put("aggregates", List.of("count"));
        expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(property));
    }

    private static Map<String, Object> v1() {
        Map<String, Object> caps = valid();
        caps.put("specVersions", List.of(0, 1));
        caps.put("plan", List.of("scan", "limit", "aggregate"));
        caps.put("aggregates", List.of("count", "sum", "min", "max"));
        return caps;
    }

    public void testAVersionOneDeclarationMayDeclareAggregates() {
        FlightConnectorFactory.validateDeclaration(v1());
        Map<String, Object> groupByOnly = v1();
        groupByOnly.remove("aggregates");
        FlightConnectorFactory.validateDeclaration(groupByOnly);
        Map<String, Object> unknown = v1();
        unknown.put("aggregates", List.of("count", "x-fancy"));
        FlightConnectorFactory.validateDeclaration(unknown); // neither defined nor reserved: ignored by ES
    }

    public void testReservedAggregatesAndAnAggregateWithoutTheOperatorAreRejected() {
        for (String name : List.of("avg", "count_distinct", "median", "percentile", "std_dev", "values", "top", "sample")) {
            Map<String, Object> caps = v1();
            caps.put("aggregates", List.of("count", name));
            Exception e = expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
            assertThat(e.getMessage(), containsString(name));
        }
        Map<String, Object> noOperator = v1();
        noOperator.put("plan", List.of("scan", "limit"));
        expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(noOperator));
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

    public void testSpecVersionsMustBeANonEmptyArrayOfIntegers() {
        List<Object> bad = new java.util.ArrayList<>();
        bad.add(List.of());
        bad.add(List.of("0"));
        bad.add(List.of(0.5));
        bad.add("0");
        bad.add(null);
        for (Object versions : bad) {
            Map<String, Object> caps = valid();
            caps.put("specVersions", versions);
            expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
        }
        Map<String, Object> ok = valid();
        ok.put("specVersions", List.of(0, 1.0));
        FlightConnectorFactory.validateDeclaration(ok);
    }

    public void testTheHighestSharedVersionIsNegotiated() {
        assertEquals(0, FlightConnectorFactory.negotiatedVersion(valid()));
        Map<String, Object> both = valid();
        both.put("specVersions", List.of(1, 0));
        assertEquals(1, FlightConnectorFactory.negotiatedVersion(both));
        Map<String, Object> later = valid();
        later.put("specVersions", List.of(2, 3));
        assertEquals(-1, FlightConnectorFactory.negotiatedVersion(later));
        later.put("specVersions", List.of(0, 1, 2));
        assertEquals(1, FlightConnectorFactory.negotiatedVersion(later));
    }

    public void testListLimitsAboveAnIntAreRejected() {
        for (String name : List.of("maxInListSize", "maxPatternBytes")) {
            Map<String, Object> caps = valid();
            caps.put("limits", Map.of(name, 1L << 31));
            Exception e = expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
            assertThat(e.getMessage(), containsString(name));
            Map<String, Object> ok = valid();
            ok.put("limits", Map.of(name, Integer.MAX_VALUE));
            FlightConnectorFactory.validateDeclaration(ok);
        }
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
