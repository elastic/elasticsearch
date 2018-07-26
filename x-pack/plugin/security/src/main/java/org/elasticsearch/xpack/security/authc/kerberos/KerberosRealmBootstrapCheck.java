/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * This class is used to perform bootstrap checks for kerberos realm.
 * <p>
 * We use service keytabs for validating incoming kerberos tickets and is a
 * required configuration. Due to JVM wide system properties for Kerberos we
 * cannot support multiple Kerberos realms. This class adds checks for node to
 * fail if service keytab does not exist or multiple kerberos realms have been
 * configured.
 */
public class KerberosRealmBootstrapCheck implements BootstrapCheck {
    private final Environment env;

    public KerberosRealmBootstrapCheck(final Environment env) {
        this.env = env;
    }

    @Override
    public BootstrapCheckResult check(final BootstrapContext context) {
        final Map<RealmConfig.RealmIdentifier, Settings> realmsSettings = RealmSettings.getRealmSettings(context.settings);
        boolean isKerberosRealmConfigured = false;
        for (final RealmConfig.RealmIdentifier realmId : realmsSettings.keySet()) {
            if (KerberosRealmSettings.TYPE.equals(realmId.getType())) {
                if (isKerberosRealmConfigured) {
                    return BootstrapCheckResult.failure("multiple [" + KerberosRealmSettings.TYPE + "] realms are configured. ["
                        + KerberosRealmSettings.TYPE + "] can only have one such realm configured");
                }
                isKerberosRealmConfigured = true;

                final Setting<String> keyTabSetting = KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.getConcreteSetting(realmId.getName());
                final Path keytabPath = env.configFile().resolve(keyTabSetting.get(context.settings));
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
