/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import joptsimple.internal.Strings;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class KerberosTestUtil {

    public static Path writeKeyTab(final Path dir, final String name, final String content) throws IOException {
        final Path path = dir.resolve(name);
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(path, StandardCharsets.US_ASCII)) {
            bufferedWriter.write(Strings.isNullOrEmpty(content) ? "test-content" : content);
        }
        return path;
    }

    public static Settings buildKerberosRealmSettings(final String keytabPath) {
        return buildKerberosRealmSettings(keytabPath, 100, "10m", true);
    }

    public static Settings buildKerberosRealmSettings(final String keytabPath, final int maxUsersInCache,
            final String cacheTTL, final boolean enableDebugging) {
        Settings.Builder builder = Settings.builder()
                .put(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.getKey(), keytabPath)
                .put(KerberosRealmSettings.CACHE_MAX_USERS_SETTING.getKey(), maxUsersInCache)
                .put(KerberosRealmSettings.CACHE_TTL_SETTING.getKey(), cacheTTL)
                .put(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.getKey(), enableDebugging);
        return builder.build();
    }
}
