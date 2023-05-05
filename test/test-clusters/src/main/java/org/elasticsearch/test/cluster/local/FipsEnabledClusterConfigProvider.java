/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.util.resource.Resource;

public class FipsEnabledClusterConfigProvider implements LocalClusterConfigProvider {

    @Override
    public void apply(LocalClusterSpecBuilder<?> builder) {
        if (isFipsEnabled()) {
            builder.configFile(
                "fips_java.security",
                Resource.fromClasspath(isOracleJvm() ? "fips/fips_java_oracle.security" : "fips/fips_java.security")
            )
                .configFile("fips_java.policy", Resource.fromClasspath("fips/fips_java.policy"))
                .configFile("cacerts.bcfks", Resource.fromClasspath("fips/cacerts.bcfks"))
                .systemProperty("java.security.properties", "=${ES_PATH_CONF}/fips_java.security")
                .systemProperty("java.security.policy", "=${ES_PATH_CONF}/fips_java.policy")
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
                .keystorePassword("keystore-password");
        }
    }

    private static boolean isFipsEnabled() {
        return Boolean.getBoolean("tests.fips.enabled");
    }

    private static boolean isOracleJvm() {
        return System.getProperty("java.vendor").toLowerCase().contains("oracle");
    }
}
