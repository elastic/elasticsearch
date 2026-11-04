/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.SatelliteFragment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FlightSourceOptionsTests extends ESTestCase {

    private static Map<String, Object> stampDeclaring(String... options) {
        return Map.of(SatelliteFragment.CAPABILITY_KEY, Map.of("options", List.of(options)));
    }

    public void testOptionsAreTheKeysThatAreNeitherOursNorFrameworkKeys() {
        Map<String, Object> config = new HashMap<>();
        config.put("endpoint", "grpc://h:1");
        config.put("target", "t");
        config.put("schema_resolution", "strict");
        config.put("_definition_version", "v1");
        config.put("units", "fahrenheit");
        config.put("a_flag", true);
        Map<String, Object> options = FlightConnectorFactory.optionsOf(config);
        assertThat(options.keySet().stream().toList(), equalTo(List.of("a_flag", "units")));
    }

    public void testNoOptionsNeedNoSatellite() {
        FlightConnectorFactory.checkOptions(Map.of(), null);
    }

    public void testOptionsNeedASatelliteThatImplementsTheProtocol() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> FlightConnectorFactory.checkOptions(Map.of("units", "f"), null));
        assertThat(e.getMessage(), containsString("does not implement the satellite protocol"));
    }

    public void testAnUndeclaredOptionIsRefusedNotIgnored() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> FlightConnectorFactory.checkOptions(Map.of("units", "f", "color", "red"), stampDeclaring("units"))
        );
        assertThat(e.getMessage(), containsString("[color]"));
        assertThat(e.getMessage(), containsString("declares [units]"));
    }

    public void testASatelliteThatDeclaresNoOptionsRefusesAllOfThem() {
        Map<String, Object> stamp = Map.of(SatelliteFragment.CAPABILITY_KEY, Map.of("plan", List.of("scan")));
        Exception e = expectThrows(IllegalArgumentException.class, () -> FlightConnectorFactory.checkOptions(Map.of("units", "f"), stamp));
        assertThat(e.getMessage(), containsString("declares []"));
    }

    public void testDeclaredOptionsPass() {
        FlightConnectorFactory.checkOptions(Map.of("units", "f"), stampDeclaring("units", "other"));
    }

    public void testValidateConfigAcceptsScalarOptionsAndRejectsStructuredOnes() {
        FlightConnectorFactory factory = new FlightConnectorFactory();
        Map<String, Object> ok = new HashMap<>();
        ok.put("endpoint", "grpc://h:1");
        ok.put("target", "t");
        ok.put("units", "f");
        ok.put("limit_rows", 5);
        ok.put("verbose", false);
        ok.put("_datasource", Map.of("anything", "framework"));
        factory.validateConfig("flight://h:1/t", ok);

        Map<String, Object> bad = new HashMap<>(ok);
        bad.put("nested", Map.of("a", 1));
        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.validateConfig("flight://h:1/t", bad));
        assertThat(e.getMessage(), containsString("[nested]"));
    }

    public void testValidatorDerivesEndpointAndTargetAndKeepsOptions() {
        FlightDataSourceValidator validator = new FlightDataSourceValidator();
        Map<String, Object> dataset = validator.validateDataset(
            Map.of(),
            "flight://127.0.0.1:47490/weather",
            Map.of("units", "fahrenheit")
        );
        assertThat(dataset.get("endpoint"), equalTo("flight://127.0.0.1:47490"));
        assertThat(dataset.get("target"), equalTo("weather"));
        assertThat(dataset.get("units"), equalTo("fahrenheit"));
    }

    public void testValidatorAppliesTheDefaultPort() {
        Map<String, Object> dataset = new FlightDataSourceValidator().validateDataset(Map.of(), "grpc://h/t", Map.of());
        assertThat(dataset.get("endpoint"), equalTo("grpc://h:47470"));
    }

    public void testValidatorRefusesSettingsOfOursAndStructuredOnes() {
        FlightDataSourceValidator validator = new FlightDataSourceValidator();
        for (String reserved : List.of("endpoint", "target", "schema_resolution", "_x")) {
            expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "flight://h:1/t", Map.of(reserved, "x")));
        }
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "flight://h:1/t", Map.of("o", List.of(1))));
    }

    public void testValidatorRefusesBadResources() {
        FlightDataSourceValidator validator = new FlightDataSourceValidator();
        for (String resource : List.of(
            "s3://bucket/key",
            "flight://h:1",
            "flight://h:1/",
            "flight://h:1/a/b",
            "flight:///t",
            "not a uri"
        )) {
            expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), resource, Map.of()));
        }
    }

    public void testAFlightDataSourceTakesNoSettings() {
        FlightDataSourceValidator validator = new FlightDataSourceValidator();
        assertTrue(validator.validateDatasource(Map.of()).isEmpty());
        expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("token", "x")));
    }
}
