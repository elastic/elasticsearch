/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.util.KeyValuePair;

import org.elasticsearch.Build;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
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
import java.util.regex.Pattern;

/** A cli command which requires an {@link org.elasticsearch.env.Environment} to use current paths and settings. */
public abstract class EnvironmentAwareCommand extends Command {

    private static final String DOCKER_UPPERCASE_SETTING_PREFIX = "ES_SETTING_";
    private static final Pattern DOCKER_LOWERCASE_SETTING_REGEX = Pattern.compile("[-a-z0-9_]+(\\.[-a-z0-9_]+)+");

    private final OptionSpec<KeyValuePair> settingOption;

    /**
     * Construct the command with the specified command description. This command will have logging configured without reading Elasticsearch
     * configuration files.
     *
     * @param description the command description
     */
    public EnvironmentAwareCommand(final String description) {
        super(description);
        this.settingOption = parser.accepts("E", "Configure a setting").withRequiredArg().ofType(KeyValuePair.class);
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        execute(terminal, options, createEnv(options, processInfo), processInfo);
    }

    private static void putDockerEnvSettings(Map<String, String> settings, Map<String, String> envVars) {
        for (var envVar : envVars.entrySet()) {
            String key = envVar.getKey();
            if (DOCKER_LOWERCASE_SETTING_REGEX.matcher(key).matches()) {
                // all lowercase, like cluster.name, so just put directly
                settings.put(key, envVar.getValue());
            } else if (key.startsWith(DOCKER_UPPERCASE_SETTING_PREFIX)) {
                // remove prefix
                key = key.substring(DOCKER_UPPERCASE_SETTING_PREFIX.length());
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
    protected Environment createEnv(OptionSet options, ProcessInfo processInfo) throws UserException {
        final Map<String, String> settings = new HashMap<>();
        for (final KeyValuePair kvp : settingOption.values(options)) {
            if (kvp.value.isEmpty()) {
                throw new UserException(ExitCodes.USAGE, "setting [" + kvp.key + "] must not be empty");
            }
            if (settings.containsKey(kvp.key)) {
                final String message = String.format(Locale.ROOT, "setting [%s] set twice via command line -E", kvp.key);
                throw new UserException(ExitCodes.USAGE, message);
            }
            settings.put(kvp.key, kvp.value);
        }

        if (getBuildType() == Build.Type.DOCKER) {
            putDockerEnvSettings(settings, processInfo.envVars());
        }

        putSystemPropertyIfSettingIsMissing(processInfo.sysprops(), settings, "path.data", "es.path.data");
        putSystemPropertyIfSettingIsMissing(processInfo.sysprops(), settings, "path.home", "es.path.home");
        putSystemPropertyIfSettingIsMissing(processInfo.sysprops(), settings, "path.logs", "es.path.logs");

        final String esPathConf = processInfo.sysprops().get("es.path.conf");
        if (esPathConf == null) {
            throw new UserException(ExitCodes.CONFIG, "the system property [es.path.conf] must be set");
        }
        return InternalSettingsPreparer.prepareEnvironment(
            Settings.EMPTY,
            settings,
            getConfigPath(esPathConf),
            // HOSTNAME is set by elasticsearch-env and elasticsearch-env.bat so it is always available
            () -> processInfo.envVars().get("HOSTNAME")
        );
    }

    // protected to allow tests to override
    protected Build.Type getBuildType() {
        return Build.current().type();
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
    ) throws UserException {
        final String value = sysprops.get(key);
        if (value != null) {
            if (settings.containsKey(setting)) {
                final String message = String.format(
                    Locale.ROOT,
                    "setting [%s] found via command-line -E and system property [%s]",
                    setting,
                    key
                );
                throw new UserException(ExitCodes.USAGE, message);
            } else {
                settings.put(setting, value);
            }
        }
    }

    /** Execute the command with the initialized {@link Environment}. */
    public abstract void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception;

}
