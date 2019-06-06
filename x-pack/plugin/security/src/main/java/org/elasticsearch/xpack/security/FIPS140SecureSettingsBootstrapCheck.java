/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;

import java.io.IOException;
import java.io.UncheckedIOException;

public class FIPS140SecureSettingsBootstrapCheck implements BootstrapCheck {

    private final boolean fipsModeEnabled;
    private final Environment environment;

    FIPS140SecureSettingsBootstrapCheck(Settings settings, Environment environment) {
        this.fipsModeEnabled = XPackSettings.FIPS_MODE_ENABLED.get(settings);
        this.environment = environment;
    }

    /**
     * Test if the node fails the check.
     *
     * @param context the bootstrap context
     * @return the result of the bootstrap check
     */
    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        if (fipsModeEnabled) {
            try (KeyStoreWrapper secureSettings = KeyStoreWrapper.load(environment.configFile())) {
                if (secureSettings != null && secureSettings.getFormatVersion() < 3) {
                    return BootstrapCheckResult.failure("Secure settings store is not of the appropriate version. Please use " +
                        "bin/elasticsearch-keystore create to generate a new secure settings store and migrate the secure settings there.");
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return BootstrapCheckResult.success();
    }

    @Override
    public boolean alwaysEnforce() {
        return fipsModeEnabled;
    }
}
