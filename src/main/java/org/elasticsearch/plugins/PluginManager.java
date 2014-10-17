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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.commons.cli.CommandLine;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.http.client.HttpDownloadHelper;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;
import static org.elasticsearch.common.cli.Terminal.Verbosity.SILENT;
import static org.elasticsearch.common.cli.Terminal.Verbosity.VERBOSE;
import static org.elasticsearch.common.io.FileSystemUtils.moveFilesWithoutOverwriting;

/**
 *
 */
public class PluginManager extends CliTool {

    public static void main(String[] args) throws Exception {
        PluginManager pluginManager = new PluginManager();
        // TODO Remove in 2.0.0
        if (args != null) {
            // We try to check if we have any deprecated option
            for (int c = 0; c < args.length; c++) {
                String command = args[c];
                switch (command) {
                    case "url":
                    case "-url":
                        args[c] = reportDeprecated(pluginManager.terminal, command, "--url");
                        break;
                    case "verbose":
                    case "-verbose":
                        args[c] = reportDeprecated(pluginManager.terminal, command, "--verbose");
                        break;
                    case "silent":
                    case "-silent":
                        args[c] = reportDeprecated(pluginManager.terminal, command, "--silent");
                        break;
                    case "timeout":
                    case "-timeout":
                        args[c] = reportDeprecated(pluginManager.terminal, command, "--timeout");
                        break;
                    case "-i":
                    case "--install":
                    case "-install":
                        args[c] = reportDeprecated(pluginManager.terminal, command, "install");
                        break;
                    case "-r":
                    case "--remove":
                    case "-remove":
                    case "remove":
                        args[c] = reportDeprecated(pluginManager.terminal, command, "uninstall");
                        break;
                    case "-l":
                    case "--list":
                        args[c] = reportDeprecated(pluginManager.terminal, command, "list");
                        break;
                }
            }
        }

        pluginManager.execute(args);
    }

    @Deprecated
    private static String reportDeprecated(Terminal terminal, String oldOption, String newOption) {
        terminal.printError("WARN: option [%s] has been deprecated by [%s]. Please update your scripts.",
                oldOption, newOption);
        return newOption;
    }

    private static final CliToolConfig CONFIG = CliToolConfig.config("plugin", PluginManager.class)
            .cmds(Install.CMD, Uninstall.CMD, ListPlugins.CMD)
            .build();

    public PluginManager(Terminal terminal) {
        super(CONFIG, terminal);
    }

    public PluginManager() {
        super(CONFIG);
    }

    @Override
    protected Command parse(String cmdName, CommandLine cli) throws Exception {
        switch (cmdName.toLowerCase(Locale.ROOT)) {
            case Install.NAME: return Install.parse(terminal, cli);
            case Uninstall.NAME: return Uninstall.parse(terminal, cli);
            case ListPlugins.NAME: return ListPlugins.parse(terminal, cli);
            default:
                assert false : "can't get here as cmd name is validated before this method is called";
                return exitCmd(ExitStatus.CODE_ERROR);
        }
    }

    static class Install extends Command {

        private static final String NAME = "install";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Install.class)
                .options(option("u", "url").required(false).hasArg(false))
                .options(option("t", "timeout").required(false).hasArg(false))
                .build();

        static Command parse(Terminal terminal, CommandLine cli) {
            TimeValue timeout = TimeValue.parseTimeValue(cli.getOptionValue("t"), null);
            String url = cli.getOptionValue("u");
            String[] args = cli.getArgs();
            if (args.length == 0) {
                return exitCmd(ExitStatus.USAGE, terminal, "plugin name is missing (type -h for help)");
            }
            String name = args[0];
            return new Install(terminal, url, name, timeout);
        }

        final String url;
        final String name;
        final PluginHandle handle;
        final TimeValue timeout;

        final HttpDownloadHelper downloadHelper;

        Install(Terminal terminal, String url, String name, TimeValue timeout) {
            this(terminal, url, name, timeout, new HttpDownloadHelper());
        }

        Install(Terminal terminal, String url, String name, TimeValue timeout, HttpDownloadHelper downloadHelper) {
            super(terminal);
            this.url = url;
            this.name = name;
            this.handle = PluginHandle.parse(name);
            this.timeout = timeout != null ? timeout : TimeValue.timeValueMillis(0);
            this.downloadHelper = downloadHelper;
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            Files.createDirectories(env.pluginsFile());
            if (!Files.isWritable(env.pluginsFile())) {
                throw new IOException("No write permissions on plugin directory [" + env.pluginsFile() + "]");
            }

            // extract the plugin
            final Path extractLocation = handle.extractedDir(env);
            if (Files.exists(extractLocation)) {
                throw new IOException("plugin directory [" + extractLocation.toAbsolutePath()
                        + "] already exists. To update the plugin, uninstall it first (see -h for help)");
            }

            checkForForbiddenName(handle.name);
            Path pluginFile = handle.distroFile(env);

            // first, downloading the plugin from all possible URL locations
            Downloader downloader = new Downloader(terminal, downloadHelper, timeout);
            if (url != null) {
                downloader.add(url);
            }
            downloader.addAll(handle.urls());

            if (!downloader.download(pluginFile)) {
                terminal.printError("failed to download plugin [%s] out of all possible locations...%s",
                        name, terminal.verbosity() == VERBOSE ? "" : " Use -v to get detailed information");
                return ExitStatus.UNAVAILABLE;
            }

            // now, extracting the content of the downloaded plugin bundle
            try (FileSystem zipFile = FileSystems.newFileSystem(pluginFile, null)) {
                for (final Path root : zipFile.getRootDirectories() ) {
                    final Path[] topLevelFiles = FileSystemUtils.files(root);
                    //we check whether we need to remove the top-level folder while extracting
                    //sometimes (e.g. github) the downloaded archive contains a top-level folder which needs to be removed
                    final boolean stripTopLevelDirectory;
                    if (topLevelFiles.length == 1 && Files.isDirectory(topLevelFiles[0])) {
                        // valid names if the zip has only one top level directory
                        switch (topLevelFiles[0].getFileName().toString()) {
                            case  "_site/":
                            case  "bin/":
                            case  "config/":
                            case  "_dict/":
                                stripTopLevelDirectory = false;
                                break;
                            default:
                                stripTopLevelDirectory = true;
                        }
                    } else {
                        stripTopLevelDirectory = false;
                    }
                    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            Path target =  FileSystemUtils.append(extractLocation, file, stripTopLevelDirectory ? 1 : 0);
                            Files.createDirectories(target);
                            Files.copy(file, target, StandardCopyOption.REPLACE_EXISTING);
                            return FileVisitResult.CONTINUE;
                        }

                    });
                }
                terminal.println("Installed [%s] into [%s]", name, extractLocation.toAbsolutePath());
            } catch (Exception e) {
                terminal.printError("failed to extract plugin [%s]: %s", pluginFile, ExceptionsHelper.detailedMessage(e));
                throw e;
            } finally {
                Files.delete(pluginFile);
            }

            if (FileSystemUtils.hasExtensions(extractLocation, ".java")) {
                terminal.printError("Plugin installation assumed to be site plugin, but contains source code, aborting installation...");
                try {
                    IOUtils.rm(extractLocation);
                } catch(Exception e) {
                    terminal.print("Failed to remove site plugin from path [%s]: %s", extractLocation, ExceptionsHelper.detailedMessage(e));
                }
                return ExitStatus.DATA_ERROR;
            }

            // It could potentially be a non explicit _site plugin
            boolean potentialSitePlugin = true;

            // moving the plugin's bin dir if exists
            Path binFile = extractLocation.resolve("bin");
            if (Files.exists(binFile) && Files.isDirectory(binFile)) {
                Path toLocation = handle.binDir(env);
                terminal.println(VERBOSE, "Found bin, moving to [%s]", toLocation.toAbsolutePath());
                if (Files.exists(toLocation)) {
                    IOUtils.rm(toLocation);
                }
                try {
                    Files.move(binFile, toLocation, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    terminal.printError("Could not move [%s] to [%s]", binFile.toAbsolutePath(), toLocation.toAbsolutePath());
                    return ExitStatus.CANT_CREATE;
                }
                if (Files.getFileStore(toLocation).supportsFileAttributeView(PosixFileAttributeView.class)) {
                    final Set<PosixFilePermission> perms = new HashSet<>();
                    perms.add(PosixFilePermission.OWNER_EXECUTE);
                    perms.add(PosixFilePermission.GROUP_EXECUTE);
                    perms.add(PosixFilePermission.OTHERS_EXECUTE);
                    Files.walkFileTree(toLocation, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            if (attrs.isRegularFile()) {

                                Files.setPosixFilePermissions(file, perms);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } else {
                    terminal.println(VERBOSE, "Skipping posix permissions - filestore doesn't support posix permission");
                }
                terminal.println(VERBOSE, "Installed [%s] into [%s]", name, toLocation.toAbsolutePath());
                potentialSitePlugin = false;
            }

            // moving the plugin config dir if exists without overwriting existing config files
            Path configFile = extractLocation.resolve("config");
            if (Files.exists(configFile) && Files.isDirectory(configFile)) {
                Path toLocation = handle.configDir(env);
                terminal.println(VERBOSE, "Found config, moving to " + toLocation.toAbsolutePath());
                moveFilesWithoutOverwriting(configFile, toLocation, ".new");
                terminal.println(VERBOSE, "Installed " + name + " into " + toLocation.toAbsolutePath());
                potentialSitePlugin = false;
            }

            // try and identify the plugin type, see if it has no .class or .jar files in it
            // so its probably a _site, and it it does not have a _site in it, move everything to _site
            if (potentialSitePlugin
                    && !Files.exists(extractLocation.resolve("_site"))
                    && !FileSystemUtils.hasExtensions(extractLocation, ".class", ".jar")) {
                terminal.println("Identified as a _site plugin, moving to _site structure ...");
                Path site = extractLocation.resolve("_site");
                Path tmpLocation = env.pluginsFile().resolve(extractLocation.getFileName() + ".tmp");
                try {
                    Files.move(extractLocation, tmpLocation);
                } catch (IOException e) {
                    terminal.printError("Could not move [%s] to [%s]", extractLocation.toAbsolutePath(), tmpLocation.toAbsolutePath());
                    return ExitStatus.CANT_CREATE;
                }
                Files.createDirectories(extractLocation);
                try {
                    Files.move(tmpLocation, site);
                } catch (IOException e) {
                    terminal.printError("Could not move [%s] to [%s]", tmpLocation.toAbsolutePath(), site.toAbsolutePath());
                    return ExitStatus.CANT_CREATE;
                }
                terminal.println(VERBOSE, "Installed [%s] into [%s]", name, site.toAbsolutePath());
            }

            terminal.println("Plugin [" + name + "] installed successfully!");
            return ExitStatus.OK;
        }
    }

    static class Uninstall extends Command {

        static enum Result {
            MISSING, FAILED, DELETED;

            public Result add(Result result) {
                if (result == FAILED || this == FAILED) {
                    return FAILED;
                }
                if (result == DELETED || this == DELETED) {
                    return DELETED;
                }
                return MISSING;
            }
        }

        private static final String NAME = "uninstall";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Uninstall.class).build();

        static Command parse(Terminal terminal, CommandLine cli) {
            String[] args = cli.getArgs();
            if (args.length == 0) {
                return exitCmd(ExitStatus.USAGE, terminal, "plugin name is missing (type -h for help)");
            }
            String name = args[0];
            return new Uninstall(terminal, name);
        }

        final String name;
        final PluginHandle handle;

        Uninstall(Terminal terminal, String name) {
            super(terminal);
            this.name = name;
            this.handle = PluginHandle.parse(name);
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            checkForForbiddenName(handle.name);

            // removing extracted dir if exists
            Path fileToDelete = handle.extractedDir(env);
            Result result = delete(fileToDelete);

            // removing plugin's distro file if exists
            fileToDelete = handle.distroFile(env);
            result = result.add(delete(fileToDelete));

            // removing plugin's bin dir if exists
            fileToDelete = handle.binDir(env);
            result = result.add(delete(fileToDelete));

            switch (result) {

                case DELETED:
                    terminal.println("Plugin [%s] was uninstalled successfully!", name);
                    return ExitStatus.OK;

                case FAILED:
                    terminal.println("Plugin [%s] was partially uninstalled!", name);
                    return ExitStatus.IO_ERROR;

                default: // MISSING
                    terminal.println("Plugin [%s] was not found. (run `plugin list` to get list of installed plugins)", name);
                    return ExitStatus.IO_ERROR;

            }
        }

        Result delete(Path path) {
            if (!Files.exists(path)) {
                return Result.MISSING;
            }
            terminal.print(VERBOSE, "Removing [%s]...", path.toAbsolutePath());
            try {
                IOUtils.rm(path);
            } catch (Exception ex) {
                terminal.printError("Failed to remove [%s]! (check file permissions)", path.toAbsolutePath());
                return Result.FAILED;
            }
            terminal.println(VERBOSE, "Done!");
            return Result.DELETED;
        }
    }

    static class ListPlugins extends Command {

        private static final String NAME = "list";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, ListPlugins.class).build();

        static Command parse(Terminal terminal, CommandLine cli) {
            String[] args = cli.getArgs();
            if (args.length > 0) {
                return exitCmd(ExitStatus.USAGE, terminal, "no command option expected (type -h for help)");
            }

            return new ListPlugins(terminal);
        }

        ListPlugins(Terminal terminal) {
            super(terminal);
        }

        public Path[] getListInstalledPlugins(Environment env) throws IOException {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(env.pluginsFile())) {
                return Iterators.toArray(stream.iterator(), Path.class);
            }
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            Path[] plugins = getListInstalledPlugins(env);
            if (plugins == null || plugins.length == 0) {
                terminal.println("No plugin detected in " + env.pluginsFile().toAbsolutePath());
                return ExitStatus.OK;
            }
            terminal.println("total " + plugins.length);
            for (int i = 0; i < plugins.length; i++) {
                terminal.println(" - " + plugins[i].getFileName());
            }
            return ExitStatus.OK;
        }
    }

    private static final ImmutableSet<Object> FORBIDDEN_NAMES = ImmutableSet.builder()
            .add("elasticsearch",
                    "elasticsearch.bat",
                    "elasticsearch.in.sh",
                    "plugin",
                    "plugin.bat",
                    "service.bat").build();

    // Valid directory names for plugin ZIP files when it has only one single dir
    private static final ImmutableSet<Object> VALID_TOP_LEVEL_PLUGIN_DIRS = ImmutableSet.builder()
            .add("_site",
                    "bin",
                    "config",
                    "_dict").build();

    private static void checkForForbiddenName(String name) {
        if (!hasLength(name) || FORBIDDEN_NAMES.contains(name.toLowerCase(Locale.ROOT))) {
            throw new ElasticsearchIllegalArgumentException("Illegal plugin name [" + name + "]");
        }
    }

    /**
     * Helper class to extract properly user name, repository name, version and plugin name
     * from plugin name given by a user.
     */
    static class PluginHandle {

        final String name;
        final String version;
        final String user;
        final String repo;

        PluginHandle(String name, String version, String user, String repo) {
            this.name = name;
            this.version = version;
            this.user = user;
            this.repo = repo;
        }

        List<String> urls() {
            List<String> urls = new ArrayList<>();
            if (version != null) {
                // Elasticsearch download service
                urls.add("http://download.elasticsearch.org/" + user + "/" + repo + "/" + repo + "-" + version + ".zip");
                // Maven central repository
                urls.add("http://search.maven.org/remotecontent?filepath=" + user.replace('.', '/') + "/" + repo + "/" + version + "/" + repo + "-" + version + ".zip");
                // Sonatype repository
                urls.add("https://oss.sonatype.org/service/local/repositories/releases/content/" + user.replace('.', '/') + "/" + repo + "/" + version + "/" + repo + "-" + version + ".zip");
                // Github repository
                urls.add("https://github.com/" + user + "/" + repo + "/archive/" + version + ".zip");
            }
            // Github repository for master branch (assume site)
            if (user != null) {
                urls.add("https://github.com/" + user + "/" + repo + "/archive/master.zip");
            }
            return urls;
        }

        Path distroFile(Environment env) {
            return env.pluginsFile().resolve(name + ".zip");
        }

        Path extractedDir(Environment env) {
            return env.pluginsFile().resolve(name);
        }

        Path binDir(Environment env) {
            return env.homeFile().resolve("bin").resolve(name);
        }

        Path configDir(Environment env) {
            return env.configFile().resolve(name);
        }

        static PluginHandle parse(String name) {
            if (!Strings.hasText(name)) {
                throw new ElasticsearchIllegalArgumentException("a plugin name must be provided");
            }

            String[] elements = name.split("/");

            // We first consider the simplest form: plugin_name
            String repo = elements[0];
            String user = null;
            String version = null;

            // We consider the form: username/plugin_name
            if (elements.length > 1) {
                user = elements[0];
                repo = elements[1];

                // We consider the form: username/plugin_name/version
                if (elements.length > 2) {
                    version = elements[2];
                }
            }

            if (repo.startsWith("elasticsearch-")) {
                // remove elasticsearch- prefix
                String endname = repo.substring("elasticsearch-".length());
                return new PluginHandle(endname, version, user, repo);
            }

            if (name.startsWith("es-")) {
                // remove es- prefix
                String endname = repo.substring("es-".length());
                return new PluginHandle(endname, version, user, repo);
            }

            return new PluginHandle(repo, version, user, repo);
        }
    }

    private static class Downloader {

        private final List<URL> urls = new ArrayList<>();
        private final Terminal terminal;
        private final HttpDownloadHelper downloadHelper;
        private final HttpDownloadHelper.DownloadProgress progress;
        private final TimeValue timeout;

        public Downloader(
                Terminal terminal,
                HttpDownloadHelper downloadHelper,
                TimeValue timeout) {

            this.terminal = terminal;
            this.downloadHelper = downloadHelper;
            this.timeout = timeout;
            this.progress = terminal.verbosity() == SILENT ?
                    new HttpDownloadHelper.NullProgress() :
                    new HttpDownloadHelper.VerboseProgress(terminal.writer());
        }

        public void add(String url) {
            try {
                urls.add(new URL(url));
            } catch (MalformedURLException e) {
                terminal.println(VERBOSE, "Malformed URL [%s], skipping...", url);
            }
        }

        public void addAll(Collection<String> urls) {
            for (String url : urls) {
                add(url);
            }
        }

        // returns true if the download finished normally, false if it was timed out
        public boolean download(Path dest) throws Exception {
            terminal.println(VERBOSE, "Trying to download from %s location(s).", urls.size());
            for (URL url : urls) {
                terminal.println("Trying " + url.toExternalForm() + "...");
                try {
                    if (downloadHelper.download(url, dest, progress, this.timeout)) {
                        return true;
                    }
                } catch (ElasticsearchTimeoutException e) {
                    terminal.printError("Timed out on [%s], skipping...", url);
                } catch (FileNotFoundException e) {
                    terminal.println(VERBOSE, "Failed: %s", ExceptionsHelper.detailedMessage(e));
                } catch (IOException e) {
                    if (e.getCause() instanceof FileNotFoundException) {
                        // ignore 404 errors
                        terminal.println(VERBOSE, "Failed: %s", ExceptionsHelper.detailedMessage(e));
                    } else {
                        terminal.printError(e);
                    }
                }
            }
            return false;
        }
    }
}
