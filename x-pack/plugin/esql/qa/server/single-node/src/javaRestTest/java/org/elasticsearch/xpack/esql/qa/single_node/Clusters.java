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
     * A cluster with the ES|QL federation opt-in omitted from the spec entirely, so the node boots with
     * {@value org.elasticsearch.xpack.esql.datasources.Federation#REGISTER_PROPERTY} genuinely unset. That is the
     * shipping default and an explicit {@code "false"} cannot reproduce it: a present-but-false property takes a
     * different path through the parsing in {@code Federation.readEnabled}.
     */
    public static ElasticsearchCluster testClusterWithoutFederation() {
        return testCluster(CsvTestUtils.createCsvDataDirectory(), config -> {}, false, false);
    }

    private static ElasticsearchCluster testCluster(
        Path csvDataPath,
        LocalClusterConfigProvider configProvider,
        boolean shared,
        boolean registerFederation
    ) {
        boolean securityEnabled = Booleans.parseBoolean(System.getProperty(SECURITY_ENABLED_PROPERTY, "false"));
        var builder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", Boolean.toString(securityEnabled))
            .setting("xpack.license.self_generated.type", "trial")
            .setting("path.repo", csvDataPath::toString)
            .setting("esql.datasource.local_allowed_paths", csvDataPath::toString)
            .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, ENCRYPTION_PASSWORD)
            .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .configFile("ingest-geoip/GeoLite2-City.mmdb", Resource.fromClasspath("GeoLite2-City.mmdb"))
            .configFile("ingest-geoip/GeoLite2-Country.mmdb", Resource.fromClasspath("GeoLite2-Country.mmdb"))
            .configFile("ingest-geoip/GeoLite2-ASN.mmdb", Resource.fromClasspath("GeoLite2-ASN.mmdb"))
            .setting("ingest.geoip.downloader.enabled", "false");
        if (registerFederation) {
            // ES|QL federation (external data sources / datasets) is off by default; this shared cluster backs
            // DataSourceCrudRestIT and other suites that register data sources/datasets, so opt in here. Uses the
            // supplier form (a provider), not the plain-value form, so FederationKillSwitchRestartRestIT's later
            // per-cluster provider can still win on restart: resolveSystemProperties() applies plain-value entries
            // after providers, so a plain value here would unconditionally override that test's "false".
            builder.systemProperty(Federation.REGISTER_PROPERTY, () -> "true");
        }
        // Applied last (lazily, at spec build time) so a suite's own config provider still overrides the above.
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
