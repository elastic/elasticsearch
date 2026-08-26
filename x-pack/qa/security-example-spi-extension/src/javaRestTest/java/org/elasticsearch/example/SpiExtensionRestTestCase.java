/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.example.realm.CustomRealm;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

/**
 * Base test case for the SPI extension plugin integration tests.
 * Provides shared cluster configuration and authentication settings.
 */
public abstract class SpiExtensionRestTestCase extends ESRestTestCase {

    public static final String REALM_USERNAME = "test_user";
    public static final String REALM_PASSWORD = "secret_password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .plugin("spi-extension")
        .setting("xpack.security.authc.realms.custom.my_realm.order", "0")
        .setting("xpack.security.authc.realms.custom.my_realm.filtered_setting", "should be filtered")
        .setting("xpack.security.authc.realms.custom.my_realm.username", REALM_USERNAME)
        .keystore("xpack.security.authc.realms.custom.my_realm.password", REALM_PASSWORD)
        .setting("xpack.security.authc.realms.file.esusers.order", "1")
        .setting("xpack.security.authc.realms.native.native.order", "2")
        .setting("xpack.security.authc.realms.custom_role_mapping.role_map.order", "3")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + "." + CustomRealm.USER_HEADER, REALM_USERNAME)
            .put(ThreadContext.PREFIX + "." + CustomRealm.PW_HEADER, REALM_PASSWORD)
            .build();
    }
}
