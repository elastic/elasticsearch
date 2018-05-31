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
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class KerberosRealmSettingsTests extends ESTestCase {

    public void testKerberosRealmSettings() throws IOException {
        final Path dir = createTempDir();
        Path configDir = dir.resolve("config");
        if (Files.exists(configDir) == false) {
            configDir = Files.createDirectory(configDir);
        }
        KerberosTestCase.writeKeyTab(dir, "config" + dir.getFileSystem().getSeparator() + "http.keytab", null);
        final Integer maxUsers = randomInt();
        final String cacheTTL = randomLongBetween(10L, 100L) + "m";
        final Settings settings = KerberosTestCase.buildKerberosRealmSettings("config" + dir.getFileSystem().getSeparator() + "http.keytab",
                maxUsers, cacheTTL, true);

        assertEquals("config" + dir.getFileSystem().getSeparator() + "http.keytab",
                KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(settings));
        assertEquals(TimeValue.parseTimeValue(cacheTTL, KerberosRealmSettings.CACHE_TTL_SETTING.getKey()),
                KerberosRealmSettings.CACHE_TTL_SETTING.get(settings));
        assertEquals(maxUsers, KerberosRealmSettings.CACHE_MAX_USERS_SETTING.get(settings));
        assertEquals(Boolean.TRUE, KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(settings));
    }

}
