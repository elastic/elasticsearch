/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.util.KeyValuePair;

import org.elasticsearch.Build;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.InternalSettingsPreparer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** A cli command which requires an {@link org.elasticsearch.env.Environment} to use current paths and settings. */
public abstract class EnvironmentAwareCommand extends Command {

    // the env var prefix used for passing settings in docker
    private static final String DOCKER_SETTING_PREFIX = "ES_SETTING_";

    private final OptionSpec<KeyValuePair> settingOption;

    /**
     * Construct the command with the specified command description. This command will have logging configured without reading Elasticsearch
     * configuration files.
     *
     * @param description the command description
     */
    public EnvironmentAwareCommand(final String description) {
        this(description, CommandLoggingConfigurator::configureLoggingWithoutConfig);
    }

    /**
     * Construct the command with the specified command description and runnable to execute before main is invoked. Commands constructed
     * with this constructor must take ownership of configuring logging.
     *
     * @param description the command description
     * @param beforeMain the before-main runnable
     */
    public EnvironmentAwareCommand(final String description, final Runnable beforeMain) {
        super(description, beforeMain);
        this.settingOption = parser.accepts("E", "Configure a setting").withRequiredArg().ofType(KeyValuePair.class);
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        execute(terminal, options, createEnv(options));
    }

    // Note, isUpperCase is used so that non-letters are considered lowercase
    private static boolean isLowerCase(String s) {
        for (int i = 0; i < s.length(); ++i) {
            if (Character.isUpperCase(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private void putDockerEnvSettings(Map<String, String> settings, Map<String, String> envVars) {
        for (var envVar : envVars.entrySet()) {
            String key = envVar.getKey();
            if (isLowerCase(key)) {
                // all lowercase, like cluster.name, so just put directly
                settings.put(key, envVar.getValue());
            } else if (key.startsWith(DOCKER_SETTING_PREFIX)) {
                // remove prefix
                key = key.substring(DOCKER_SETTING_PREFIX.length());
                // insert dots for underscores
                key = key.replace('_', '.');
                // unescape double dots, which were originally double underscores
                key = key.replace("..", "_");
                // lowercase the whole thing
                key = key.toLowerCase(Locale.ROOT);

                settings.put(key, envVar.getValue());
            }
        }
    }

    /** Create an {@link Environment} for the command to use. Overrideable for tests. */
    protected Environment createEnv(OptionSet options) throws UserException {
        final Map<String, String> settings = new HashMap<>();
        for (final KeyValuePair kvp : settingOption.values(options)) {
            if (kvp.value.isEmpty()) {
                throw new UserException(ExitCodes.USAGE, "setting [" + kvp.key + "] must not be empty");
            }
            if (settings.containsKey(kvp.key)) {
                final String message = String.format(
                    Locale.ROOT,
                    "setting [%s] already set, saw [%s] and [%s]",
                    kvp.key,
                    settings.get(kvp.key),
                    kvp.value
                );
                throw new UserException(ExitCodes.USAGE, message);
            }
            settings.put(kvp.key, kvp.value);
        }

        if (getBuildType() == Build.Type.DOCKER) {
            putDockerEnvSettings(settings, envVars);
        }

        putSystemPropertyIfSettingIsMissing(sysprops, settings, "path.data", "es.path.data");
        putSystemPropertyIfSettingIsMissing(sysprops, settings, "path.home", "es.path.home");
        putSystemPropertyIfSettingIsMissing(sysprops, settings, "path.logs", "es.path.logs");

        final String esPathConf = sysprops.get("es.path.conf");
        if (esPathConf == null) {
            throw new UserException(ExitCodes.CONFIG, "the system property [es.path.conf] must be set");
        }
        return InternalSettingsPreparer.prepareEnvironment(
            Settings.EMPTY,
            settings,
            getConfigPath(esPathConf),
            // HOSTNAME is set by elasticsearch-env and elasticsearch-env.bat so it is always available
            () -> envVars.get("HOSTNAME")
        );
    }

    // protected to allow tests to override
    protected Build.Type getBuildType() {
        return Build.CURRENT.type();
    }

    @SuppressForbidden(reason = "need path to construct environment")
    private static Path getConfigPath(final String pathConf) {
        return Paths.get(pathConf);
    }

    /** Ensure the given setting exists, reading it from system properties if not already set. */
    private static void putSystemPropertyIfSettingIsMissing(
        final Map<String, String> sysprops,
        final Map<String, String> settings,
        final String setting,
        final String key
    ) {
        final String value = sysprops.get(key);
        if (value != null) {
            if (settings.containsKey(setting)) {
                final String message = String.format(
                    Locale.ROOT,
                    "duplicate setting [%s] found via command-line [%s] and system property [%s]",
                    setting,
                    settings.get(setting),
                    value
                );
                throw new IllegalArgumentException(message);
            } else {
                settings.put(setting, value);
            }
        }
    }

    /** Execute the command with the initialized {@link Environment}. */
    protected abstract void execute(Terminal terminal, OptionSet options, Environment env) throws Exception;

}
