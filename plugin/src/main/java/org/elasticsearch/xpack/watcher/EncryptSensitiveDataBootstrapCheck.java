/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.XPackPlugin;

import java.nio.file.Files;
import java.nio.file.Path;

final class EncryptSensitiveDataBootstrapCheck implements BootstrapCheck {

    private final Settings settings;
    private final Environment environment;

    EncryptSensitiveDataBootstrapCheck(Settings settings, Environment environment) {
        this.settings = settings;
        this.environment = environment;
    }

    @Override
    public boolean check() {
        return Watcher.ENCRYPT_SENSITIVE_DATA_SETTING.get(settings) && Watcher.ENCRYPTION_KEY_SETTING.exists(settings) == false;
    }

    @Override
    public String errorMessage() {
        final Path sysKeyPath = environment.configFile().resolve(XPackPlugin.NAME).resolve("system_key").toAbsolutePath();
        if (Files.exists(sysKeyPath)) {
            return "Encryption of sensitive data requires the key to be placed in the secure setting store. Run " +
                    "'bin/elasticsearch-keystore add-file " + Watcher.ENCRYPTION_KEY_SETTING.getKey() + " " +
                    environment.configFile().resolve(XPackPlugin.NAME).resolve("system_key").toAbsolutePath() +
                    "' to import the file.\nAfter importing, the system_key file should be removed from the " +
                    "filesystem.\nRepeat this on every node in the cluster.";
        } else {
            return "Encryption of sensitive data requires a key to be placed in the secure setting store. First run the " +
                    "bin/x-pack/syskeygen tool to generate a key file.\nThen run 'bin/elasticsearch-keystore add-file " +
                    Watcher.ENCRYPTION_KEY_SETTING.getKey() + " " +
                    environment.configFile().resolve(XPackPlugin.NAME).resolve("system_key").toAbsolutePath() + "' to import the key into" +
                    " the secure setting store. Finally, remove the system_key file from the filesystem.\n" +
                    "Repeat this on every node in the cluster";
        }
    }

    @Override
    public boolean alwaysEnforce() {
        return true;
    }
}
