/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This class is used to perform bootstrap checks for kerberos realm.
 */
public class KerberosRealmBootstrapCheck implements BootstrapCheck {
    private final Environment env;

    public KerberosRealmBootstrapCheck(final Environment env) {
        this.env = env;
    }

    @Override
    public BootstrapCheckResult check(final BootstrapContext context) {
        final Settings realmsSettings = RealmSettings.get(context.settings);
        boolean isKerberosRealmConfigured = false;
        for (final String name : realmsSettings.names()) {
            final Settings realmSettings = realmsSettings.getAsSettings(name);
            final String type = realmSettings.get("type");
            if (Strings.hasText(type) == false) {
                return BootstrapCheckResult.failure("missing realm type for [" + name + "] realm");
            }
            if (KerberosRealmSettings.TYPE.equals(type)) {
                if (isKerberosRealmConfigured) {
                    return BootstrapCheckResult.failure(
                            "multiple [" + type + "] realms are configured. [" + type + "] can only have one such realm configured");
                }
                isKerberosRealmConfigured = true;

                final Path keytabPath = env.configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(realmSettings));
                if (Files.exists(keytabPath) == false) {
                    return BootstrapCheckResult.failure("configured service key tab file [" + keytabPath + "] does not exist");
                }
            }
        }
        return BootstrapCheckResult.success();
    }

    @Override
    public boolean alwaysEnforce() {
        return true;
    }
}
