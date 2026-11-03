/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class FlightHostPolicyTests extends ESTestCase {

    private static void assertAllowed(FlightHostPolicy policy, String... hosts) {
        for (String host : hosts) {
            policy.check(host);
        }
    }

    private static void assertRefused(FlightHostPolicy policy, String... hosts) {
        for (String host : hosts) {
            Exception e = expectThrows(IllegalArgumentException.class, host, () -> policy.check(host));
            assertThat(e.getMessage(), containsString("[" + host + "] is not allowed"));
            assertThat(e.getMessage(), containsString("esql.datasource.flight.allowed_hosts"));
        }
    }

    public void testTheDefaultIsThisMachineOnly() {
        assertAllowed(FlightHostPolicy.DEFAULT, "localhost", "LOCALHOST", "127.0.0.1", "127.255.0.3", "::1", "[::1]");
        assertRefused(
            FlightHostPolicy.DEFAULT,
            "example.com",
            "10.0.0.1",
            "169.254.169.254",
            "0.0.0.0",
            "192.168.1.5",
            "::2",
            "localhost.example.com"
        );
    }

    public void testNamesSuffixesAddressesAndRanges() {
        FlightHostPolicy policy = new FlightHostPolicy(List.of("sat.example.com", "*.internal.example.com", "10.1.0.0/16", "192.0.2.7"));
        assertAllowed(policy, "sat.example.com", "SAT.EXAMPLE.COM", "a.internal.example.com", "a.b.internal.example.com", "10.1.200.9");
        assertAllowed(policy, "192.0.2.7");
        assertRefused(
            policy,
            "internal.example.com",
            "evilinternal.example.com",
            "10.2.0.1",
            "192.0.2.8",
            "localhost",
            "other.example.com"
        );
    }

    public void testARangeCannotBeBypassedByANameAndAnAddressIsNotMatchedByAName() {
        assertRefused(new FlightHostPolicy(List.of("10.0.0.0/8")), "ten.example.com");
        assertRefused(new FlightHostPolicy(List.of("example.com")), "93.184.216.34");
    }

    public void testIpv6Ranges() {
        FlightHostPolicy policy = new FlightHostPolicy(List.of("fd00::/8"));
        assertAllowed(policy, "fd12:3456::1", "[fd12:3456::1]");
        assertRefused(policy, "fe80::1", "127.0.0.1");
    }

    public void testStarAllowsEveryHost() {
        assertAllowed(new FlightHostPolicy(List.of("*")), "example.com", "10.0.0.1", "::2");
    }

    public void testAnEntryThatCannotBeReadIsRefused() {
        expectThrows(IllegalArgumentException.class, () -> new FlightHostPolicy(List.of("")));
        expectThrows(IllegalArgumentException.class, () -> new FlightHostPolicy(List.of("10.0.0.0/99")));
        expectThrows(
            IllegalArgumentException.class,
            () -> GrpcDataSourcePlugin.FLIGHT_ALLOWED_HOSTS.get(
                Settings.builder().put("esql.datasource.flight.allowed_hosts", "10.0.0.0/99").build()
            )
        );
    }

    public void testTheSettingDefaultsToThisMachine() {
        assertEquals(FlightHostPolicy.DEFAULT_ALLOWED, GrpcDataSourcePlugin.FLIGHT_ALLOWED_HOSTS.get(Settings.EMPTY));
    }

    public void testADatasetOnAHostThatIsNotAllowedIsNotRegistered() {
        FlightDataSourceValidator validator = new FlightDataSourceValidator();
        Exception e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "grpc://10.0.0.5:47470/t", Map.of())
        );
        assertThat(e.getMessage(), containsString("is not allowed"));
        validator.validateDataset(Map.of(), "grpc://127.0.0.1:47470/t", Map.of());
        new FlightDataSourceValidator(new FlightHostPolicy(List.of("10.0.0.0/8"))).validateDataset(
            Map.of(),
            "grpc://10.0.0.5:47470/t",
            Map.of()
        );
    }

    /** A dataset registered before the setting was narrowed is stopped at connection time: resolution, discovery and reading. */
    public void testNarrowingTheSettingStopsDatasetsThatAlreadyExist() throws Exception {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer()) {
            String location = "grpc://127.0.0.1:" + server.port() + "/t";
            FlightHostPolicy narrowed = new FlightHostPolicy(List.of("10.0.0.0/8"));
            FlightConnectorFactory factory = new FlightConnectorFactory(
                TimeValue.timeValueSeconds(5),
                new SatelliteDeclarationCache(TimeValue.ZERO),
                narrowed
            );
            Exception e = expectThrows(RuntimeException.class, () -> factory.resolveMetadata(location, Map.of()));
            assertThat(e.getMessage(), containsString("is not allowed"));
            // the same check guards the split provider and the connector, which share the one client builder
            expectThrows(
                IllegalArgumentException.class,
                () -> FlightClients.build(
                    new org.apache.arrow.memory.RootAllocator(),
                    "127.0.0.1",
                    server.port(),
                    FlightClients.Transport.PLAIN,
                    narrowed
                )
            );
            assertNotNull(new FlightConnectorFactory().resolveMetadata(location, Map.of()));
        }
    }
}
