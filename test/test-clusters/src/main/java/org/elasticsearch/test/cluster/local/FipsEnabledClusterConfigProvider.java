/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.util.resource.Resource;

import java.nio.file.Path;

public class FipsEnabledClusterConfigProvider implements LocalClusterConfigProvider {

    @Override
    public void apply(LocalClusterSpecBuilder<?> builder) {
        if (isFipsEnabled()) {
            builder.configFile(
                "fips_java.security",
                Resource.fromClasspath(isOracleJvm() ? "fips/fips_java_oracle.security" : "fips/fips_java.security")
            )
                .configFile("cacerts.bcfks", Resource.fromClasspath("fips/cacerts.bcfks"))
                .systemProperty("java.security.properties", "=${ES_PATH_CONF}/fips_java.security")
                .systemProperty("javax.net.ssl.trustStore", "${ES_PATH_CONF}/cacerts.bcfks")
                .systemProperty("javax.net.ssl.trustStorePassword", "password")
                .systemProperty("javax.net.ssl.keyStorePassword", "password")
                .systemProperty("javax.net.ssl.keyStoreType", "BCFKS")
                .systemProperty("org.bouncycastle.fips.approved_only", "true")
                .setting("network.host", "_local:ipv4_")
                .setting("xpack.security.enabled", "false")
                .setting("xpack.security.fips_mode.enabled", "true")
                .setting("xpack.license.self_generated.type", "trial")
                .setting("xpack.security.authc.password_hashing.algorithm", "pbkdf2_stretch")
                .setting("xpack.security.fips_mode.required_providers", () -> "[BCFIPS, BCJSSE]", n -> n.getVersion().onOrAfter("8.13.0"))
                .keystorePassword("keystore-password");

            // Inject SecurityManager policy from the previous major's BWC checkout for old ES versions
            // that still install SecurityManager at bootstrap. The policy file path is provided by the
            // build system via the tests.cluster.fips.policy.path system property, and the checkout is
            // guaranteed to exist via a Gradle dependency on the BWC checkout configuration.
            String fipsPolicyPath = System.getProperty("tests.cluster.fips.policy.path");
            if (fipsPolicyPath != null) {
                builder.configFile("fips_java.policy", Resource.fromFile(Path.of(fipsPolicyPath)));
                builder.systemProperty(
                    "java.security.policy",
                    () -> "=${ES_PATH_CONF}/fips_java.policy",
                    n -> n.getVersion().before("9.0.0")
                );
            }
        }
    }

    private static boolean isFipsEnabled() {
        return Boolean.getBoolean("tests.fips.enabled");
    }

    private static boolean isOracleJvm() {
        return System.getProperty("java.vendor").toLowerCase().contains("oracle");
    }
}
