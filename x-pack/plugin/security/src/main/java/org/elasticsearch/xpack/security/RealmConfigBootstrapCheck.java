/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;

import java.util.HashSet;
import java.util.Set;

/**
 * Performs bootstrap checks required across security realms.
 */
public class RealmConfigBootstrapCheck implements BootstrapCheck {

    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        BootstrapCheckResult result = BootstrapCheckResult.success();
        try {
            final Settings realmsSettings = RealmSettings.get(context.settings);
            final Set<String> registeredRealmTypes = new HashSet<>();
            final Set<String> internalTypes = new HashSet<>();
            for (String name : realmsSettings.names()) {
                final Settings realmSettings = realmsSettings.getAsSettings(name);
                final String type = realmSettings.get("type");
                if (Strings.isNullOrEmpty(type)) {
                    throw new IllegalArgumentException("missing realm type for [" + name + "] realm");
                }
                if (FileRealmSettings.TYPE.equals(type) || NativeRealmSettings.TYPE.equals(type)) {
                    // this is an internal realm factory, let's make sure we didn't already
                    // registered one (there can only be one instance of an internal realm)
                    if (internalTypes.contains(type)) {
                        throw new IllegalArgumentException("multiple [" + type + "] realms are configured. [" + type
                                + "] is an internal realm and therefore there can only be one such realm configured");
                    }
                    internalTypes.add(type);
                }
                if (KerberosRealmSettings.TYPE.equals(type) && registeredRealmTypes.contains(type)) {
                    // Kerberos realm can have only one instance.
                    throw new IllegalArgumentException("multiple [" + type + "] realms are configured. For [" + type
                            + "] there can only be one such realm configured");
                }
                registeredRealmTypes.add(type);
            }
        } catch (Exception e) {
            result = BootstrapCheckResult.failure(e.getMessage());
        }
        return result;
    }
}
