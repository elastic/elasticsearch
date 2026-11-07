/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/** A Flight server that uses TLS is resolved only when the data source gives the CA that issued its certificate. */
public class FlightTlsTests extends ESTestCase {

    private final FlightConnectorFactory factory = new FlightConnectorFactory();

    private static Map<String, Object> dataSource(String ca) {
        return Map.of("_datasource", Map.of("ca_certificate", ca));
    }

    public void testTheTransportFollowsTheSchemeOfTheEndpoint() {
        assertSame(FlightClients.Transport.PLAIN, FlightClients.transport("grpc://h:1", Map.of("ca_certificate", "x")));
        FlightClients.Transport tls = FlightClients.transport("grpc+tls://h:1", Map.of("ca_certificate", "x", "server_name", "n"));
        assertThat(tls, equalTo(new FlightClients.Transport(true, "x", "n")));
    }

    public void testTheSettingsAreReadWhetherFlatOrUnderTheDataSource() {
        assertThat(FlightClients.transport("flight+tls://h:1", Map.of("ca_certificate", "x")).caCertificate(), equalTo("x"));
        assertThat(FlightClients.transport("flight+tls://h:1", dataSource("y")).caCertificate(), equalTo("y"));
        assertNull(FlightClients.transport("flight+tls://h:1", Map.of()).caCertificate());
        assertNull(FlightClients.transport("flight+tls://h:1", null).caCertificate());
    }

    public void testResolvesAServerWhoseCertificateTheDataSourceTrusts() throws IOException {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer(true)) {
            String location = "grpc+tls://127.0.0.1:" + server.port() + "/t";
            SourceMetadata metadata = factory.resolveMetadata(location, dataSource(BlockingSatelliteServer.caCertificate()));
            assertThat(metadata.schema().size(), equalTo(BlockingSatelliteServer.SCHEMA.getFields().size()));
            assertThat(metadata.config().get("endpoint"), equalTo("grpc+tls://127.0.0.1:" + server.port()));
        }
    }

    public void testAnUntrustedCertificateFailsTheResolution() throws IOException {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer(true)) {
            String location = "grpc+tls://127.0.0.1:" + server.port() + "/t";
            Exception e = expectThrows(
                RuntimeException.class,
                () -> factory.resolveMetadata(location, dataSource(BlockingSatelliteServer.otherCaCertificate()))
            );
            assertThat(e, instanceOf(IllegalStateException.class));
        }
    }

    public void testATlsServerIsNotReadInClear() throws IOException {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer(true)) {
            expectThrows(RuntimeException.class, () -> factory.resolveMetadata("grpc://127.0.0.1:" + server.port() + "/t", Map.of()));
        }
    }

    public void testAPlainServerIsNotReadWithTls() throws IOException {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer(false)) {
            expectThrows(
                RuntimeException.class,
                () -> factory.resolveMetadata(
                    "grpc+tls://127.0.0.1:" + server.port() + "/t",
                    dataSource(BlockingSatelliteServer.caCertificate())
                )
            );
        }
    }

    private static Map<String, Object> dataSource(String ca, String token) {
        return Map.of("_datasource", Map.of("ca_certificate", ca, "token", token));
    }

    public void testTheTokenIsSentAndTheRightOneIsAccepted() throws IOException {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer(true)) {
            server.requiredToken = "s3cret";
            String location = "grpc+tls://127.0.0.1:" + server.port() + "/t";
            assertNotNull(factory.resolveMetadata(location, dataSource(BlockingSatelliteServer.caCertificate(), "s3cret")));
            assertEquals(0, server.unauthenticatedCalls.get());
        }
    }

    public void testAMissingOrWrongTokenIsRefusedWithoutEchoingAnything() throws IOException {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer(true)) {
            server.requiredToken = "s3cret";
            String location = "grpc+tls://127.0.0.1:" + server.port() + "/t";
            for (Map<String, Object> config : java.util.List.of(
                dataSource(BlockingSatelliteServer.caCertificate(), "wrong-token"),
                dataSource(BlockingSatelliteServer.caCertificate())
            )) {
                Exception e = expectThrows(IllegalArgumentException.class, () -> factory.resolveMetadata(location, config));
                assertThat(e.getMessage(), containsString("did not accept the credentials"));
                assertThat(e.getMessage(), not(containsString("wrong-token")));
                assertThat(e.getMessage(), not(containsString("no valid token")));
            }
        }
    }

    public void testTheTokenIsNotInTheStringFormOfTheTransport() {
        FlightClients.Transport transport = FlightClients.transport("grpc+tls://h:1", Map.of("token", "s3cret"));
        assertThat(transport.toString(), not(containsString("s3cret")));
        assertEquals("", FlightClients.Transport.PLAIN.cacheScope());
        assertThat(transport.cacheScope(), not(containsString("s3cret")));
        assertNotEquals(transport.cacheScope(), FlightClients.transport("grpc+tls://h:1", Map.of("token", "other")).cacheScope());
    }

    /** A declaration read with one credential is not handed to another: the cache is keyed by who asked. */
    public void testDeclarationsAreNotSharedBetweenCredentials() throws IOException {
        FlightConnectorFactory caching = new FlightConnectorFactory(
            org.elasticsearch.core.TimeValue.timeValueSeconds(30),
            org.elasticsearch.core.TimeValue.timeValueMinutes(5)
        );
        try (BlockingSatelliteServer server = new BlockingSatelliteServer(true)) {
            server.requiredToken = "s3cret";
            server.capabilities = "{\"specVersions\":[0],\"fragmentFormats\":[\"json\"],\"plan\":[\"scan\"],\"functions\":[],"
                + "\"types\":[\"integer\"],\"declarationId\":\"d1\"}";
            server.schemaDeclarationId = "d1";
            String location = "grpc+tls://127.0.0.1:" + server.port() + "/t";
            caching.resolveMetadata(location, dataSource(BlockingSatelliteServer.caCertificate(), "s3cret"));
            expectThrows(
                IllegalArgumentException.class,
                () -> caching.resolveMetadata(location, dataSource(BlockingSatelliteServer.caCertificate(), "wrong"))
            );
        }
    }

    /** The server name is what the certificate is checked against: the fixture certificate names localhost and 127.0.0.1 only. */
    public void testTheServerNameIsWhatTheCertificateIsCheckedAgainst() throws IOException {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer(true)) {
            String location = "grpc+tls://127.0.0.1:" + server.port() + "/t";
            String ca = BlockingSatelliteServer.caCertificate();
            assertNotNull(
                factory.resolveMetadata(location, Map.of("_datasource", Map.of("ca_certificate", ca, "server_name", "localhost")))
            );
            expectThrows(
                RuntimeException.class,
                () -> factory.resolveMetadata(
                    location,
                    Map.of("_datasource", Map.of("ca_certificate", ca, "server_name", "not-the-name.example"))
                )
            );
        }
    }

    public void testAnEncryptedSettingIsNeverTakenForAToken() {
        Object stillEncrypted = new Object();
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> FlightClients.transport("grpc+tls://h:1", Map.of("_datasource", Map.of("token", stillEncrypted)))
        );
        assertThat(e.getMessage(), containsString("has not been decrypted"));
    }

    public void testASchemeThatIsNotFlightIsNotReadInClear() {
        expectThrows(IllegalArgumentException.class, () -> FlightClients.transport("http://h:1", Map.of()));
    }
}
