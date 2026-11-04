/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FlightDataSourceValidatorTests extends ESTestCase {

    private final FlightDataSourceValidator validator = new FlightDataSourceValidator();

    private static String caPem() throws IOException {
        return BlockingSatelliteServer.caCertificate();
    }

    public void testADataSourceWithoutSettingsIsValid() {
        assertThat(validator.validateDatasource(Map.of()).size(), equalTo(0));
        assertThat(validator.validateDatasource(null).size(), equalTo(0));
    }

    public void testTheTlsSettingsAreKeptAndAreNotSecret() throws IOException {
        Map<String, DataSourceSetting> settings = validator.validateDatasource(
            Map.of("ca_certificate", caPem(), "server_name", "satellite.example")
        );
        assertThat(settings.keySet(), equalTo(java.util.Set.of("ca_certificate", "server_name")));
        for (DataSourceSetting setting : settings.values()) {
            assertFalse(setting.secret());
        }
    }

    public void testAnyOtherSettingIsRefused() {
        Exception e = expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("region", "x")));
        assertThat(e.getMessage(), containsString("[region]"));
    }

    public void testACertificateThatIsNotPemIsRefusedWhenTheDataSourceIsRegistered() {
        Exception e = expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("ca_certificate", "nonsense")));
        assertThat(e.getMessage(), containsString("ca_certificate"));
        e = expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("server_name", "")));
        assertThat(e.getMessage(), containsString("non-empty"));
    }

    public void testATlsResourceKeepsItsSchemeInTheEndpoint() {
        for (String scheme : new String[] { "flight+tls", "grpc+tls" }) {
            Map<String, Object> config = validator.validateDataset(Map.of(), scheme + "://127.0.0.1:47471/t", Map.of());
            assertThat(config.get("endpoint"), equalTo(scheme + "://127.0.0.1:47471"));
            assertThat(config.get("target"), equalTo("t"));
        }
    }

    public void testTlsSettingsNeedATlsResource() throws IOException {
        Map<String, DataSourceSetting> settings = Map.of("ca_certificate", new DataSourceSetting(caPem(), false));
        Exception e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(settings, "flight://127.0.0.1:47471/t", Map.of())
        );
        assertThat(e.getMessage(), containsString("flight+tls or grpc+tls"));
        validator.validateDataset(settings, "flight+tls://127.0.0.1:47471/t", Map.of());
    }

    public void testTheTokenIsASecret() {
        Map<String, DataSourceSetting> settings = validator.validateDatasource(Map.of("token", "s3cret"));
        assertTrue(settings.get("token").secret());
        assertThat(new FlightPluginSecrets().names(), equalTo(java.util.Set.of("token", "credentials")));
    }

    public void testATokenIsPrintableAsciiWithoutSpaces() {
        for (String bad : new String[] { "has space", "new\nline", "naïve" }) {
            Exception e = expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("token", bad)));
            assertThat(e.getMessage(), containsString("printable ASCII"));
        }
    }

    public void testATokenNeedsATlsResource() {
        Map<String, DataSourceSetting> settings = Map.of("token", new DataSourceSetting("s3cret", true));
        Exception e = expectThrows(ValidationException.class, () -> validator.validateDataset(settings, "grpc://127.0.0.1:1/t", Map.of()));
        assertThat(e.getMessage(), containsString("[token]"));
        validator.validateDataset(settings, "grpc+tls://127.0.0.1:1/t", Map.of());
    }

    private static final class FlightPluginSecrets {
        java.util.Set<String> names() {
            return new GrpcDataSourcePlugin().datasourceSecretSettingNames();
        }
    }
}
