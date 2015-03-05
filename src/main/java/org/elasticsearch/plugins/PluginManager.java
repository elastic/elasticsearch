/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.repository.*;

import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;
import static org.elasticsearch.common.cli.Terminal.Verbosity.*;
import static org.elasticsearch.plugins.repository.ChainedPluginRepository.Builder.chain;
import static org.elasticsearch.plugins.repository.LocalPluginRepository.Builder.localRepository;
import static org.elasticsearch.plugins.repository.PluginDescriptor.Builder.pluginDescriptor;
import static org.elasticsearch.plugins.repository.URLPluginRepository.Builder.urlRepository;

public class PluginManager extends CliTool {

    private static final CliToolConfig CONFIG = CliToolConfig.config("plugin", PluginManager.class)
            .cmds(List.CMD, Install.CMD, Uninstall.CMD)
            .build();

    public static void main(String[] args) {
        PluginManager manager = new PluginManager();

        // TODO Remove in 2.0.0
        checkDeprecations(manager.terminal, args);

        int status = manager.execute(args);
        System.exit(status);
    }

    public PluginManager() {
        super(CONFIG);
    }

    public PluginManager(Terminal terminal) {
        super(CONFIG, terminal);
    }

    @Override
    protected Command parse(String cmdName, CommandLine cli) throws Exception {
        switch (cmdName.toLowerCase(Locale.ROOT)) {
            case Install.NAME:
                return Install.parse(terminal, cli);
            case List.NAME:
                return List.parse(terminal, cli);
            case Uninstall.NAME:
                return Uninstall.parse(terminal, cli);
            default:
                assert false : "can't get here as cmd name is validated before this method is called";
                return exitCmd(ExitStatus.USAGE);
        }
    }

    /**
     * Checks for deprecated commands or options and prints a warning for every
     * deprecated option found.
     */
    @Deprecated
    private static void checkDeprecations(Terminal terminal, String[] args) {
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                String current = args[i];
                String replacement = null;

                switch (current) {
                    case "url":
                    case "-url":
                        replacement = "--url";
                        break;
                    case "verbose":
                    case "-verbose":
                        replacement = "--verbose";
                        break;
                    case "silent":
                    case "-silent":
                        replacement = "--silent";
                        break;
                    case "timeout":
                    case "-timeout":
                        replacement = "--timeout";
                        break;
                    case "-i":
                    case "--install":
                    case "-install":
                        replacement = "install";
                        break;
                    case "-r":
                    case "--remove":
                    case "-remove":
                    case "remove":
                        replacement = "uninstall";
                        break;
                    case "-l":
                    case "--list":
                        replacement = "list";
                        break;
                }

                if (replacement != null) {
                    terminal.printError("WARN: option [%s] has been deprecated by [%s]. Please update your scripts.", current, replacement);
                    args[i] = replacement;
                }
            }
        }
    }

    /**
     * List all installed plugins
     */
    static class List extends CliTool.Command {

        private static final String NAME = "list";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, List.class).build();

        public static Command parse(Terminal terminal, CommandLine cli) {
            return new List(terminal);
        }

        List(Terminal terminal) {
            super(terminal);
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            Collection<PluginDescriptor> plugins = installedPluginsRepository(env, settings).list();
            if (plugins != null) {
                for (PluginDescriptor plugin : plugins) {
                    terminal.println("%s (%s)", plugin.name(), checkVersion(plugin.version()));
                    terminal.println(VERBOSE, "\torganisation: %s", plugin.organisation());
                    terminal.println(VERBOSE, "\tname        : %s", plugin.name());
                    terminal.println(VERBOSE, "\tversion     : %s", plugin.version());
                    terminal.println(VERBOSE, "\tdescription : %s", plugin.description());

                    Set<PluginDescriptor> dependencies = plugin.dependencies();
                    if ((dependencies != null) && (!dependencies.isEmpty())) {
                        terminal.println(VERBOSE, "\tdepends on  :");

                        for (PluginDescriptor dependency : dependencies) {
                            terminal.println(VERBOSE, "\t\t- %s:%s:%s", dependency.organisation(), dependency.name(), dependency.version());
                        }
                    }
                }
            }
            return ExitStatus.OK;
        }
    }

    /**
     * Uninstall a plugin
     */
    static class Uninstall extends CliTool.Command {

        private static final String NAME = "uninstall";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Uninstall.class).build();

        public static Command parse(Terminal terminal, CommandLine cli) {
            String[] args = cli.getArgs();
            if (args.length == 0) {
                return exitCmd(ExitStatus.USAGE, terminal, "plugin name is missing (type -h for help)");
            }
            return new Uninstall(terminal, args[0]);
        }

        final String fqn;

        Uninstall(Terminal terminal, String fqn) {
            super(terminal);
            this.fqn = fqn;
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            final PluginDescriptor removable = pluginDescriptor(fqn).build();
            validatePluginName(removable.name());
            terminal.println("Uninstalling plugin [%s]...", removable.name());

            // Lists all installed plugins with same name & version
            PluginRepository local = installedPluginsRepository(env, settings);
            Collection<PluginDescriptor> plugins = local.list(new PluginRepository.Filter() {
                @Override
                public boolean accept(PluginDescriptor plugin) {
                    boolean match = StringUtils.equals(removable.name(), plugin.name());
                    if (Strings.hasText(removable.version())) {
                        return match && StringUtils.equals(removable.version(), plugin.version());
                    }
                    return match;
                }
            });

            if ((plugins == null) || (plugins.isEmpty())) {
                terminal.println("Plugin [%s] was not found. (run `plugin list` to get the list of installed plugins)", fqn);
                return ExitStatus.IO_ERROR;
            }

            for (PluginDescriptor plugin : plugins) {
                terminal.println(VERBOSE, "Found %s (%s)", plugin.name(), checkVersion(plugin.version()));
            }

            if (terminal.verbosity().enabled(NORMAL)) {
                boolean required = false;

                // Iterates over all installed plugins to check if the plugin to uninstall is required by another plugin
                for (PluginDescriptor plugin : local.list()) {
                    Set<PluginDescriptor> dependencies = plugin.dependencies();
                    if (dependencies != null) {
                        for (PluginDescriptor dependency : dependencies) {
                            if (StringUtils.equals(removable.name(), dependency.name())) {

                                if (!required) {
                                    required = true;
                                    terminal.println("The following plugin(s) depends on the plugin about to be removed:");
                                }
                                terminal.println("\t %s -> %s", plugin, dependency);
                            }
                        }
                    }
                }

                if (required) {
                    String confirm = terminal.readText("Confirm removal of plugin %s? [yes/no]", removable.name());
                    if (StringUtils.equalsIgnoreCase(confirm, "yes") || StringUtils.equalsIgnoreCase(confirm, "y")) {
                        terminal.println(VERBOSE, "Removal confirmed");
                    } else {
                        terminal.println(VERBOSE, "Removal canceled");
                        return ExitStatus.OK;
                    }
                }
            }

            try {
                // Remove the plugin from the local repository
                local.remove(removable);
            } catch (Exception e) {
                terminal.printError("%s", e.getMessage());
                return ExitStatus.DATA_ERROR;

            }

            terminal.println("Removed %s", removable.name());
            return ExitStatus.OK;
        }
    }

    /**
     * Installs a plugin
     */
    static class Install extends Command {

        private static final String NAME = "install";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Install.class)
                .options(option("u", "url").required(false).hasArg(false))
                .options(option("t", "timeout").required(false).hasArg(false))
                .build();

        static Command parse(Terminal terminal, CommandLine cli) {
            String[] args = cli.getArgs();
            if ((args == null) || (args.length == 0)) {
                return exitCmd(ExitStatus.USAGE, terminal, "plugin name is missing (type -h for help)");
            }

            String name = args[0];
            TimeValue timeout = TimeValue.parseTimeValue(cli.getOptionValue("t"), null);
            String url = cli.getOptionValue("u");

            return new Install(terminal, name, url, timeout);
        }

        final String fqn;
        final String urls;
        final TimeValue timeout;

        Install(Terminal terminal, String fqn, String urls, TimeValue timeout) {
            super(terminal);
            this.fqn = fqn;
            this.urls = urls;
            this.timeout = timeout;
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            final PluginDescriptor installable = pluginDescriptor(fqn).build();
            validatePluginName(installable.name());
            terminal.println("Installing plugin [%s]...", installable.name());

            // Local repository
            PluginRepository local = installedPluginsRepository(env, settings);

            // Find all installed plugins with same name
            Collection<PluginDescriptor> existing = local.find(installable.name());
            if ((existing != null) && (!existing.isEmpty())) {
                terminal.printError("Plugin(s) with same name are already installed:");
                for (PluginDescriptor plugin : existing) {
                    terminal.printError("%s (%s)", plugin.name(), checkVersion(plugin.version()));
                }
                terminal.printError("To update the plugin, uninstall it first (see -h for help)");
                return ExitStatus.CODE_ERROR;
            }

            // Listener used to monitor the downloads
            PluginDownloader.Listener listener = null;
            if (terminal.verbosity() != SILENT) {
                listener = new PluginDownloader.Listener() {

                    boolean connected = false;

                    @Override
                    public void onDownloadBegin(String url) {
                        terminal.print(VERBOSE, "Connecting to [%s]...", url);
                        connected = false;
                    }

                    @Override
                    public void onDownloadTick(String url, long total, long downloaded) {
                        if (!connected) {
                            terminal.println(VERBOSE, " connected.");
                            terminal.print(VERBOSE, "Downloading");

                            if (total > 0) {
                                ByteSizeValue size = ByteSizeValue.parseBytesSizeValue(String.valueOf(total));
                                terminal.print(VERBOSE, " (%s) ", size.toString());
                            }
                            connected = true;
                        } else {
                            terminal.print(VERBOSE, ".");
                        }
                    }

                    @Override
                    public void onDownloadEnd(String url, Path dest) {
                        terminal.println(VERBOSE, " done.");
                    }

                    @Override
                    public void onDownloadTimeout(String url, TimeValue timeout) {
                        terminal.println(VERBOSE, " timeout.");
                        terminal.println(VERBOSE, "Timed out on [%s], skipping...", url);
                    }

                    @Override
                    public void onDownloadRedirection(String url, URL redirection) {
                        terminal.println(VERBOSE, " redirected.");
                        terminal.print(VERBOSE, "Following redirection %s -> %s", url, redirection);
                    }

                    @Override
                    public void onDownloadError(String url, Exception exception) {
                        if (exception instanceof FileNotFoundException) {
                            terminal.println(VERBOSE, " not found.");
                        } else if (exception instanceof UnknownHostException) {
                            terminal.println(VERBOSE, " unknown host (%s).", exception.getMessage());
                        } else if (exception instanceof MalformedURLException) {
                            terminal.println(VERBOSE, " malformed url.");
                        } else {
                            terminal.printError("Download failed (%s)", exception.getMessage());
                        }
                    }
                };
            }

            // Default User-Agent and Version to use when downloading a plugin
            String userAgent = PluginManager.class.getSimpleName();
            String esVersion = Version.CURRENT.toString();

            // Plugin repository to use to find the plugin to install
            PluginRepository repository;
            if (Strings.hasText(urls)) {
                // Use the provided URLs to resolve the plugin
                URLPluginRepository.Builder builder = urlRepository("provided").downloadDir(env.pluginsFile()).timeout(timeout).listener(listener);
                for (String url : Strings.delimitedListToStringArray(urls, ",")) {
                    builder.url(url);
                }
                repository = builder.build();
            } else {
                // Use the default repository
                repository = defaultPluginRepository(env.pluginsFile(), timeout, listener, userAgent, esVersion);
            }

            // Resolves the plugin with its dependencies
            PluginResolver resolver = new PluginResolver(local, repository);
            PluginResolver.Resolution resolution = resolver.resolve(installable);

            PluginDescriptor resolved = resolution.resolved();
            if (resolved == null) {
                terminal.printError("Failed to resolve plugin [%s] out of all possible locations...%s", installable,
                        terminal.verbosity() == VERBOSE ? "" : " Use -v to get detailed information");
                return ExitStatus.UNAVAILABLE;
            }

            // If there's any conflict, print message and exit
            if (!resolution.conflicts().isEmpty()) {
                terminal.printError("Unable to install the plugin %s due to the following conflicts:", resolved.name());

                for (Tuple<PluginDescriptor, PluginDescriptor> conflict : resolution.conflicts()) {
                    terminal.printError("\t %s (%s) -> %s (%s)", conflict.v1().name(), checkVersion(conflict.v1().version()),
                            conflict.v2().name(), checkVersion(conflict.v2().version()));
                }
                terminal.printError("Please resolve conflicts manually before installing the plugin");
                return ExitStatus.UNAVAILABLE;
            }

            terminal.println(VERBOSE, "Plugin %s (%s) has %d dependencies",
                    resolved.name(), checkVersion(resolved.version()), (resolved.dependencies() == null ? 0 : resolved.dependencies().size()));


            if (!resolution.missings().isEmpty()) {
                terminal.printError("Unable to install the plugin %s (%s) due to the following missing dependencies:", resolved.name(), checkVersion(resolved.version()));
                for (PluginDescriptor plugin : resolution.missings()) {
                    terminal.printError("\t %s (%s)", plugin.name(), checkVersion(plugin.version()));
                }
                terminal.printError("Please try to manually install the missing dependencies.");
                return ExitStatus.UNAVAILABLE;
            }

            // Ask for a confirmation if more than 1 plugin need to be installed
            if ((terminal.verbosity().enabled(NORMAL)) && (resolution.additions().size() > 1)) {
                terminal.println("The following plugins will be installed:");

                for (PluginDescriptor plugin : resolution.additions()) {
                    terminal.println("\t%s (%s)", plugin.name(), checkVersion(plugin.version()));
                }
                String confirm = terminal.readText("Continue? [yes/no]");
                if (StringUtils.equalsIgnoreCase(confirm, "yes") || StringUtils.equalsIgnoreCase(confirm, "y")) {
                    terminal.println(VERBOSE, "Installation confirmed");
                } else {
                    terminal.println(VERBOSE, "Installation canceled");

                    // Deletes the artifacts
                    for (PluginDescriptor plugin : resolution.additions()) {
                        Files.deleteIfExists(plugin.artifact());
                    }
                    return ExitStatus.OK;
                }
            }

            // Installs all required plugins in the local repository
            for (PluginDescriptor install : resolution.additions()) {
                try {
                    terminal.println(VERBOSE, "Unpacking %s (%s)", install.name(), checkVersion(install.version()));
                    local.install(install);

                } catch (Exception e) {
                    terminal.printError("%s", e.getMessage());
                    return ExitStatus.CANT_CREATE;

                } finally {
                    // Deletes the artifact
                    Files.deleteIfExists(install.artifact());
                }
            }

            terminal.println("Plugin installed successfully!");
            return ExitStatus.OK;
        }
    }

    private static void validatePluginName(String pluginName) {
        if (!Strings.hasText(pluginName)) {
            throw new ElasticsearchIllegalArgumentException("Plugin's name cannot be null or empty");
        }
        if (!pluginName.matches("[a-zA-Z0-9_\\-]+")) {
            throw new ElasticsearchIllegalArgumentException("Plugin's name is invalid");
        }

        for (String name : LocalPluginRepository.FORBIDDEN_NAMES) {
            if (name.equalsIgnoreCase(pluginName)) {
                throw new ElasticsearchIllegalArgumentException("Plugin's name cannot be equal to '" + pluginName + "'");
            }
        }
    }

    /**
     * Prints "no version" when the version is null or empty.
     *
     * @param version the plugin's version
     * @return plugin's version or "no version"
     */
    private static String checkVersion(String version) {
        return Strings.hasText(version) ? version : "no version";
    }

    protected static PluginRepository installedPluginsRepository(Environment env, Settings settings) {
        return localRepository("local").environment(env).settings(settings).build();
    }

    protected static PluginRepository defaultPluginRepository(Path downloadDir, TimeValue timeout, PluginDownloader.Listener listener, String userAgent, String esVersion) {
        return chain("default")
                .addRepository(
                        urlRepository("elasticsearch")
                                .url("http://download.elasticsearch.org/[organisation]/[name]/[name]-[version].zip")
                                .url("http://download.elasticsearch.org/[organisation]/elasticsearch-[name]/elasticsearch-[name]-[version].zip")
                                .url("http://download.elasticsearch.org/elasticsearch/[name]/[name]-latest.zip")
                                .downloadDir(downloadDir)
                                .listener(listener)
                                .timeout(timeout)
                                .userAgent(userAgent)
                                .elasticsearchVersion(esVersion)
                                .build()
                )
                .addRepository(
                        urlRepository("central")
                                .url("https://repo1.maven.org/maven2/[organisation]/[name]/[version]/[name]-[version].zip")
                                .url("https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-[name]/[version]/elasticsearch-[name]-[version].zip")
                                .url("http://search.maven.org/remotecontent?filepath=[organisation]/[name]/[version]/[name]-[version].zip")
                                .url("http://search.maven.org/remotecontent?filepath=org/elasticsearch/elasticsearch-[name]/[version]/elasticsearch-[name]-[version].zip")
                                .downloadDir(downloadDir)
                                .listener(listener)
                                .timeout(timeout)
                                .build()
                )
                .addRepository(
                        urlRepository("sonatype")
                                .url("https://oss.sonatype.org/service/local/repositories/releases/content/[organisation]/[name]/[version]/[name]-[version].zip")
                                .url("https://oss.sonatype.org/service/local/repositories/releases/content/[organisation]/elasticsearch-[name]/[version]/elasticsearch-[name]-[version].zip")
                                .downloadDir(downloadDir)
                                .listener(listener)
                                .timeout(timeout)
                                .build()
                )
                .addRepository(
                        urlRepository("github")
                                .url("https://github.com/[organisation]/[name]/archive/[version].zip")
                                .url("https://github.com/[organisation]/[name]/archive/master.zip")
                                .url("https://github.com/[organisation]/elasticsearch-[name]/archive/[version].zip")
                                .url("https://github.com/[organisation]/elasticsearch-[name]/archive/master.zip")
                                .downloadDir(downloadDir)
                                .listener(listener)
                                .timeout(timeout)
                                .build()
                )
                .build();
    }
}
