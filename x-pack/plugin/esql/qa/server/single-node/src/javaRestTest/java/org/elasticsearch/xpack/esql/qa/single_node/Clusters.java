/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.datasources.Federation;

import java.nio.file.Path;

public class Clusters {

    private static final String ENCRYPTION_PASSWORD_ID = "test";
    private static final String ENCRYPTION_PASSWORD = "esql-test-encryption-password";

    /**
     * System property that, when set to {@code "true"}, enables xpack security on the test
     * cluster and provisions a single superuser. The corresponding credentials must also be
     * passed via {@code tests.rest.cluster.username} / {@code tests.rest.cluster.password} so
     * that {@link org.elasticsearch.test.rest.ESRestTestCase#restClientSettings()} picks them
     * up and authenticates the REST client. Defaults to {@code "false"} (security disabled).
     */
    public static final String SECURITY_ENABLED_PROPERTY = "tests.cluster.security.enabled";

    public static final String ADMIN_USER = System.getProperty("tests.rest.cluster.username", "test-admin");
    public static final String ADMIN_PASSWORD = System.getProperty("tests.rest.cluster.password", "x-pack-test-password");

    public static ElasticsearchCluster testCluster() {
        return testCluster(config -> {});
    }

    public static ElasticsearchCluster testCluster(LocalClusterConfigProvider configProvider) {
        return testCluster(CsvTestUtils.createCsvDataDirectory(), configProvider, false);
    }

    public static ElasticsearchCluster testCluster(Path csvDataPath, LocalClusterConfigProvider configProvider, boolean shared) {
        return testCluster(csvDataPath, configProvider, shared, true);
    }

    /**
     * A cluster for the suites that unregister the federation feature on the node JVM with
     * {@link Federation#REGISTER_PROPERTY}. It leaves the federation settings out entirely rather than setting them to
     * a harmless value, because an unregistered feature registers no settings: a node whose {@code elasticsearch.yml}
     * carries one of those keys does not start. A suite that needs the setting for part of its run supplies it itself,
     * as {@link FederationKillSwitchRestartRestIT} does for the phase before the feature is unregistered.
     */
    public static ElasticsearchCluster clusterWithFederationUnregistered(LocalClusterConfigProvider configProvider) {
        return testCluster(CsvTestUtils.createCsvDataDirectory(), configProvider, false, false);
    }

    /**
     * A cluster with the feature registered but no {@link Federation#FEDERATION_ENABLED} in {@code elasticsearch.yml},
     * so the node takes the build default: federation is on in a snapshot build and off in a release build. Used by
     * {@link FederationBuildDefaultRestIT}, whose subject is the default itself. Every other suite here pins the
     * setting, or unregisters the feature, so that its outcome does not depend on how the distribution was built.
     */
    public static ElasticsearchCluster clusterWithoutFederationSettings() {
        return testCluster(CsvTestUtils.createCsvDataDirectory(), config -> {}, false, false);
    }

    private static ElasticsearchCluster testCluster(
        Path csvDataPath,
        LocalClusterConfigProvider configProvider,
        boolean shared,
        boolean federationSettings
    ) {
        boolean securityEnabled = Booleans.parseBoolean(System.getProperty(SECURITY_ENABLED_PROPERTY, "false"));
        var builder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", Boolean.toString(securityEnabled))
            .setting("xpack.license.self_generated.type", "trial")
            .setting("path.repo", csvDataPath::toString)
            .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, ENCRYPTION_PASSWORD)
            .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .configFile("ingest-geoip/GeoLite2-City.mmdb", Resource.fromClasspath("GeoLite2-City.mmdb"))
            .configFile("ingest-geoip/GeoLite2-Country.mmdb", Resource.fromClasspath("GeoLite2-Country.mmdb"))
            .configFile("ingest-geoip/GeoLite2-ASN.mmdb", Resource.fromClasspath("GeoLite2-ASN.mmdb"))
            .setting("ingest.geoip.downloader.enabled", "false");
        if (federationSettings) {
            // Federation is only on by default in snapshot builds; the data source and dataset suites here need it on
            // in a release build too. A test that wants the unavailable surface turns it back off through the config
            // provider applied below. This default is a supplier, not a plain value, so that a config provider can
            // override it with either form: explicit settings win over suppliers regardless of order, and among
            // suppliers the last one applied wins.
            builder.setting(Federation.FEDERATION_ENABLED.getKey(), () -> "true")
                .setting("esql.external.local_allowed_paths", csvDataPath::toString);
        }
        builder.apply(() -> configProvider);
        if (securityEnabled) {
            builder.user(ADMIN_USER, ADMIN_PASSWORD, "superuser", true);
        }
        if (shared) {
            builder.shared(true);
        }
        return builder.build();
    }
}
