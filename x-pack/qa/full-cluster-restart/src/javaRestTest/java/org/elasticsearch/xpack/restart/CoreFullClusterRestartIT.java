/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.restart;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.upgrades.FullClusterRestartIT;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class CoreFullClusterRestartIT extends FullClusterRestartIT {

    static {
        clusterConfig = c -> c.setting("xpack.security.enabled", "true")
            .setting("xpack.security.transport.ssl.enabled", "true")
            .setting("xpack.security.transport.ssl.key", "testnode.pem")
            .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.watcher.encrypt_sensitive_data", "true")
            .setting("xpack.security.authc.api_key.enabled", "true")
            .configFile("testnode.pem", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .configFile("testnode.crt", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .keystore("xpack.watcher.encryption_key", Resource.fromClasspath("system_key"))
            .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode");
    }

    public CoreFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

}
