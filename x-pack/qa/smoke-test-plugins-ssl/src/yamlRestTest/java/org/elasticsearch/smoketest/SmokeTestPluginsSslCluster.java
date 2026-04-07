/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;

/**
 * Shared cluster configuration for smoke testing plugins with SSL/security enabled.
 * This replaces the legacy testClusters DSL configuration from build.gradle.
 */
public final class SmokeTestPluginsSslCluster {

    private SmokeTestPluginsSslCluster() {}

    public static ElasticsearchCluster create() {
        return ElasticsearchCluster.local().distribution(DistributionType.DEFAULT).apply(c -> {
            String pluginNames = System.getProperty("tests.plugin.names");
            if (pluginNames != null && pluginNames.isEmpty() == false) {
                for (String plugin : pluginNames.split(",")) {
                    c.plugin(plugin);
                }
            }
        })
            .configFile("testnode.jks", Resource.fromClasspath("testnode.jks"))
            .configFile("testnode.pem", Resource.fromClasspath("testnode.pem"))
            .configFile("testnode.crt", Resource.fromClasspath("testnode.crt"))
            .configFile("testclient.jks", Resource.fromClasspath("testclient.jks"))
            .configFile("testclient.pem", Resource.fromClasspath("testclient.pem"))
            .configFile("testclient.crt", Resource.fromClasspath("testclient.crt"))
            .setting("xpack.monitoring.collection.interval", "1s")
            .setting("xpack.monitoring.collection.enabled", "false") // will enable in test
            .setting("xpack.monitoring.exporters._http.enabled", "false") // will enable in test
            .setting("xpack.monitoring.exporters._http.type", "http")
            .setting("xpack.monitoring.exporters._http.host", "https://example.com") // will be replaced in test
            .setting("xpack.monitoring.exporters._http.auth.username", "monitoring_agent")
            .setting("xpack.monitoring.exporters._http.ssl.verification_mode", "full")
            .setting("xpack.monitoring.exporters._http.ssl.certificate_authorities", "testnode.crt")
            .keystore("xpack.monitoring.exporters._http.auth.secure_password", "x-pack-test-password")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.security.http.ssl.enabled", "true")
            .setting("xpack.security.http.ssl.key", "testnode.pem")
            .setting("xpack.security.http.ssl.certificate", "testnode.crt")
            .setting("xpack.security.http.ssl.certificate_authorities", "testnode.crt")
            .keystore("xpack.security.http.ssl.secure_key_passphrase", "testnode")
            .setting("xpack.ml.enabled", "false")
            .user("test_user", "x-pack-test-password")
            .user("monitoring_agent", "x-pack-test-password", "remote_monitoring_agent", false)
            .systemProperty("es.queryable_built_in_roles_enabled", "false")
            .build();
    }
}
