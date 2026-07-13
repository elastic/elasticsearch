/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmConfig.RealmIdentifier;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class KerberosRealmSettingsTests extends ESTestCase {

    public void testKerberosRealmSettings() throws IOException {
        final Path dir = createTempDir();
        Path configDir = dir.resolve("config");
        if (Files.exists(configDir) == false) {
            configDir = Files.createDirectory(configDir);
        }
        final String keytabPathConfig = "config" + dir.getFileSystem().getSeparator() + "http.keytab";
        KerberosRealmTestCase.writeKeyTab(dir.resolve(keytabPathConfig), null);
        final Integer maxUsers = randomInt();
        final String cacheTTL = randomLongBetween(10L, 100L) + "m";
        final boolean enableDebugLogs = randomBoolean();
        final boolean removeRealmName = randomBoolean();
        final Settings settings = KerberosRealmTestCase.buildKerberosRealmSettings(
            KerberosRealmTestCase.REALM_NAME,
            keytabPathConfig,
            maxUsers,
            cacheTTL,
            enableDebugLogs,
            removeRealmName
        );
        final RealmIdentifier identifier = new RealmIdentifier(KerberosRealmSettings.TYPE, KerberosRealmTestCase.REALM_NAME);
        final RealmConfig config = new RealmConfig(
            identifier,
            settings,
            TestEnvironment.newEnvironment(settings),
            new ThreadContext(settings)
        );

        assertThat(config.getSetting(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH), equalTo(keytabPathConfig));
        assertThat(
            config.getSetting(KerberosRealmSettings.CACHE_TTL_SETTING),
            equalTo(TimeValue.parseTimeValue(cacheTTL, KerberosRealmSettings.CACHE_TTL_SETTING.getKey()))
        );
        assertThat(config.getSetting(KerberosRealmSettings.CACHE_MAX_USERS_SETTING), equalTo(maxUsers));
        assertThat(config.getSetting(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE), is(enableDebugLogs));
        assertThat(config.getSetting(KerberosRealmSettings.SETTING_REMOVE_REALM_NAME), is(removeRealmName));
    }

}
