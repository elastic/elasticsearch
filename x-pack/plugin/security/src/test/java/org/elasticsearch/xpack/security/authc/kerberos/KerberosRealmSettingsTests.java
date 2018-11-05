/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
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
        final Settings settings = KerberosRealmTestCase.buildKerberosRealmSettings(keytabPathConfig, maxUsers, cacheTTL, enableDebugLogs,
                removeRealmName);

        assertThat(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(settings), equalTo(keytabPathConfig));
        assertThat(KerberosRealmSettings.CACHE_TTL_SETTING.get(settings),
                equalTo(TimeValue.parseTimeValue(cacheTTL, KerberosRealmSettings.CACHE_TTL_SETTING.getKey())));
        assertThat(KerberosRealmSettings.CACHE_MAX_USERS_SETTING.get(settings), equalTo(maxUsers));
        assertThat(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(settings), is(enableDebugLogs));
        assertThat(KerberosRealmSettings.SETTING_REMOVE_REALM_NAME.get(settings), is(removeRealmName));
    }

}
