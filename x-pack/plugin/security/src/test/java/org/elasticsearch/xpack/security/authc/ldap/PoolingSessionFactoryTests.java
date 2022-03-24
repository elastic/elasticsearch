/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.ldap;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;

public class PoolingSessionFactoryTests extends ESTestCase {

    protected static final RealmConfig.RealmIdentifier REALM_IDENTIFIER = new RealmConfig.RealmIdentifier("ldap", "ldap1");

    public void testConst() {
        Settings globalSettings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.enabled", false)
            .put("xpack.security.transport.ssl.certificate_authorities", "")
            .build();

        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(REALM_IDENTIFIER, PoolingSessionFactorySettings.BIND_DN), "cn=Horatio Hornblower,ou=people,o=sevenSeas")
            .build();
        RealmConfig config = new RealmConfig(
            REALM_IDENTIFIER,
            settings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(globalSettings)
        );

        new LdapUserSearchSessionFactory(config, null, threadPool);
    }
}
