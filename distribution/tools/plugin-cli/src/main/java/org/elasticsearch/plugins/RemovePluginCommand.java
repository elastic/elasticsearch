/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A command for the plugin CLI to remove plugins from Elasticsearch.
 */
class RemovePluginCommand extends EnvironmentAwareCommand {
    private final OptionSpec<Void> purgeOption;
    private final OptionSpec<String> arguments;

    RemovePluginCommand() {
        super("removes plugins from Elasticsearch");
        this.purgeOption = parser.acceptsAll(Arrays.asList("p", "purge"), "Purge plugin configuration files");
        this.arguments = parser.nonOptions("plugin id");
    }

    @Override
    protected void execute(final Terminal terminal, final OptionSet options, final Environment env) throws Exception {
        final Path pluginsDescriptor = env.configFile().resolve("elasticsearch-plugins.yml");
        if (Files.exists(pluginsDescriptor)) {
            throw new UserException(1, "Plugins descriptor [" + pluginsDescriptor + "] exists, please use [elasticsearch-plugin sync] instead");
        }

        final List<PluginDescriptor> plugins = arguments.values(options).stream().map(PluginDescriptor::new).collect(Collectors.toList());

        final RemovePluginAction action = new RemovePluginAction(terminal, env, options.has(purgeOption));
        action.execute(plugins);
    }

    /**
     * Remove the plugin specified by {@code pluginName}.
     *
     * @param terminal   the terminal to use for input/output
     * @param env        the environment for the local node
     * @param pluginIds  the IDs of the plugins to remove
     * @param purge      if true, plugin configuration files will be removed, if false they are preserved
     * @throws IOException   if any I/O exception occurs while performing a file operation
     * @throws UserException if pluginIds is null or empty
     * @throws UserException if plugin directory does not exist
     * @throws UserException if the plugin bin directory is not a directory
     */
    void execute(Terminal terminal, Environment env, List<String> pluginIds, boolean purge) throws IOException, UserException {
        if (pluginIds == null || pluginIds.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "At least one plugin ID is required");
        }

        ensurePluginsNotUsedByOtherPlugins(env, pluginIds);

        for (String pluginId : pluginIds) {
            checkCanRemove(env, pluginId, purge);
        }

        for (String pluginId : pluginIds) {
            removePlugin(env, terminal, pluginId, purge);
        }
    }

    private void ensurePluginsNotUsedByOtherPlugins(Environment env, List<String> pluginIds) throws IOException, UserException {
        // First make sure nothing extends this plugin
        final Map<String, List<String>> usedBy = new HashMap<>();
        Set<PluginsService.Bundle> bundles = PluginsService.getPluginBundles(env.pluginsFile());
        for (PluginsService.Bundle bundle : bundles) {
            for (String extendedPlugin : bundle.plugin.getExtendedPlugins()) {
                for (String pluginId : pluginIds) {
                    if (extendedPlugin.equals(pluginId)) {
                        usedBy.computeIfAbsent(bundle.plugin.getName(), (_key -> new ArrayList<>())).add(pluginId);
                    }
                }
            }
        }
        if (usedBy.isEmpty()) {
            return;
        }

        final StringJoiner message = new StringJoiner("\n");
        message.add("Cannot remove plugins because the following are extended by other plugins:");
        usedBy.forEach((key, value) -> {
            String s = "\t" + key + " used by " + value;
            message.add(s);
        });

        final RemovePluginAction action = new RemovePluginAction(terminal, env, options.has(purgeOption));
        action.execute(plugins);
    }
}
