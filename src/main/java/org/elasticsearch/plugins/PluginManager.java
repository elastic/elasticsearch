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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.*;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.http.client.HttpDownloadHelper;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.common.io.FileSystemUtils.moveFilesWithoutOverwriting;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 *
 */
public class PluginManager {
    public static final class ACTION {
        public static final int NONE = 0;
        public static final int INSTALL = 1;
        public static final int REMOVE = 2;
        public static final int LIST = 3;
    }

    public enum OutputMode {
        DEFAULT, SILENT, VERBOSE
    }

    // By default timeout is 0 which means no timeout
    public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueMillis(0);

    private static final ImmutableSet<Object> BLACKLIST = ImmutableSet.builder()
            .add("elasticsearch",
                    "elasticsearch.bat",
                    "elasticsearch.in.sh",
                    "plugin",
                    "plugin.bat",
                    "service.bat").build();

    private final Environment environment;
    private String url;
    private OutputMode outputMode;
    private TimeValue timeout;

    public PluginManager(Environment environment, String url, OutputMode outputMode, TimeValue timeout) {
        this.environment = environment;
        this.url = url;
        this.outputMode = outputMode;
        this.timeout = timeout;

        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    @Override
                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        // Install the all-trusting trust manager
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to install all-trusting trust manager", e);
        }
    }

    public void downloadAndExtract(String name) throws IOException {
        if (name == null) {
            throw new ElasticsearchIllegalArgumentException("plugin name must be supplied with --install [name].");
        }
        HttpDownloadHelper downloadHelper = new HttpDownloadHelper();
        boolean downloaded = false;
        HttpDownloadHelper.DownloadProgress progress;
        if (outputMode == OutputMode.SILENT) {
            progress = new HttpDownloadHelper.NullProgress();
        } else {
            progress = new HttpDownloadHelper.VerboseProgress(SysOut.getOut());
        }

        if (!Files.isWritable(environment.pluginsFile())) {
            throw new IOException("plugin directory " + environment.pluginsFile() + " is read only");
        }

        PluginHandle pluginHandle = PluginHandle.parse(name);
        checkForForbiddenName(pluginHandle.name);

        Path pluginFile = pluginHandle.distroFile(environment);
        // extract the plugin
        final Path extractLocation = pluginHandle.extractedDir(environment);
        if (Files.exists(extractLocation)) {
            throw new IOException("plugin directory " + extractLocation.toAbsolutePath() + " already exists. To update the plugin, uninstall it first using --remove " + name + " command");
        }

        // first, try directly from the URL provided
        if (url != null) {
            URL pluginUrl = new URL(url);
            log("Trying " + pluginUrl.toExternalForm() + "...");
            try {
                downloadHelper.download(pluginUrl, pluginFile, progress, this.timeout);
                downloaded = true;
            } catch (ElasticsearchTimeoutException e) {
                throw e;
            } catch (Exception e) {
                // ignore
                log("Failed: " + ExceptionsHelper.detailedMessage(e));
            }
        }

        if (!downloaded) {
            // We try all possible locations
            for (URL url : pluginHandle.urls()) {
                log("Trying " + url.toExternalForm() + "...");
                try {
                    downloadHelper.download(url, pluginFile, progress, this.timeout);
                    downloaded = true;
                    break;
                } catch (ElasticsearchTimeoutException e) {
                    throw e;
                } catch (Exception e) {
                    debug("Failed: " + ExceptionsHelper.detailedMessage(e));
                }
            }
        }

        if (!downloaded) {
            throw new IOException("failed to download out of all possible locations..., use --verbose to get detailed information");
        }
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
            log("Installed " + name + " into " + extractLocation.toAbsolutePath());
        } catch (Exception e) {
            log("failed to extract plugin [" + pluginFile + "]: " + ExceptionsHelper.detailedMessage(e));
            return;
        } finally {
            try {
                Files.delete(pluginFile);
            } catch (Exception ex) {
                log("Failed to delete plugin file" + pluginFile + " " + ex);
            }
        }

        if (FileSystemUtils.hasExtensions(extractLocation, ".java")) {
            debug("Plugin installation assumed to be site plugin, but contains source code, aborting installation...");
            try {
                IOUtils.rm(extractLocation);
            } catch(Exception ex) {
                debug("Failed to remove site plugin from path " + extractLocation + " - " + ex.getMessage());
            }
            throw new IllegalArgumentException("Plugin installation assumed to be site plugin, but contains source code, aborting installation.");
        }

        // It could potentially be a non explicit _site plugin
        boolean potentialSitePlugin = true;
        Path binFile = extractLocation.resolve("bin");
        if (Files.isDirectory(binFile)) {
            Path toLocation = pluginHandle.binDir(environment);
            debug("Found bin, moving to " + toLocation.toAbsolutePath());
            if (Files.exists(toLocation)) {
                IOUtils.rm(toLocation);
            }
            try {
                FileSystemUtils.move(binFile, toLocation);
            } catch (IOException e) {
                throw new IOException("Could not move [" + binFile + "] to [" + toLocation + "]", e);
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
                debug("Skipping posix permissions - filestore doesn't support posix permission");
            }
            debug("Installed " + name + " into " + toLocation.toAbsolutePath());
            potentialSitePlugin = false;
        }

        Path configFile = extractLocation.resolve("config");
        if (Files.isDirectory(configFile)) {
            Path configDestLocation = pluginHandle.configDir(environment);
            debug("Found config, moving to " + configDestLocation.toAbsolutePath());
            moveFilesWithoutOverwriting(configFile, configDestLocation, ".new");
            debug("Installed " + name + " into " + configDestLocation.toAbsolutePath());
            potentialSitePlugin = false;
        }

        // try and identify the plugin type, see if it has no .class or .jar files in it
        // so its probably a _site, and it it does not have a _site in it, move everything to _site
        if (!Files.exists(extractLocation.resolve("_site"))) {
            if (potentialSitePlugin && !FileSystemUtils.hasExtensions(extractLocation, ".class", ".jar")) {
                log("Identified as a _site plugin, moving to _site structure ...");
                Path site = extractLocation.resolve("_site");
                Path tmpLocation = environment.pluginsFile().resolve(extractLocation.getFileName() + ".tmp");
                Files.move(extractLocation, tmpLocation);
                Files.createDirectories(extractLocation);
                Files.move(tmpLocation, site);
                debug("Installed " + name + " into " + site.toAbsolutePath());
            }
        }
    }

    public void removePlugin(String name) throws IOException {
        if (name == null) {
            throw new ElasticsearchIllegalArgumentException("plugin name must be supplied with --remove [name].");
        }
        PluginHandle pluginHandle = PluginHandle.parse(name);
        boolean removed = false;

        checkForForbiddenName(pluginHandle.name);
        Path pluginToDelete = pluginHandle.extractedDir(environment);
        if (Files.exists(pluginToDelete)) {
            debug("Removing: " + pluginToDelete);
            try {
                IOUtils.rm(pluginToDelete);
            } catch (IOException ex){
                throw new IOException("Unable to remove " + pluginHandle.name + ". Check file permissions on " +
                        pluginToDelete.toString(), ex);
            }
            removed = true;
        }
        pluginToDelete = pluginHandle.distroFile(environment);
        if (Files.exists(pluginToDelete)) {
            debug("Removing: " + pluginToDelete);
            try {
                Files.delete(pluginToDelete);
            } catch (Exception ex) {
                throw new IOException("Unable to remove " + pluginHandle.name + ". Check file permissions on " +
                        pluginToDelete.toString(), ex);
            }
            removed = true;
        }
        Path binLocation = pluginHandle.binDir(environment);
        if (Files.exists(binLocation)) {
            debug("Removing: " + binLocation);
            try {
                IOUtils.rm(binLocation);
            } catch (IOException ex){
                throw new IOException("Unable to remove " + pluginHandle.name + ". Check file permissions on " +
                        binLocation.toString(), ex);
            }
            removed = true;
        }

        if (removed) {
            log("Removed " + name);
        } else {
            log("Plugin " + name + " not found. Run plugin --list to get list of installed plugins.");
        }
    }

    private static void checkForForbiddenName(String name) {
        if (!hasLength(name) || BLACKLIST.contains(name.toLowerCase(Locale.ROOT))) {
            throw new ElasticsearchIllegalArgumentException("Illegal plugin name: " + name);
        }
    }

    public Path[] getListInstalledPlugins() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(environment.pluginsFile())) {
            return Iterators.toArray(stream.iterator(), Path.class);
        }
    }

    public void listInstalledPlugins() throws IOException {
        Path[] plugins = getListInstalledPlugins();
        log("Installed plugins in " + environment.pluginsFile().toAbsolutePath() + ":");
        if (plugins == null || plugins.length == 0) {
            log("    - No plugin detected");
        } else {
            for (int i = 0; i < plugins.length; i++) {
                log("    - " + plugins[i].getFileName());
            }
        }
    }

    private static final int EXIT_CODE_OK = 0;
    private static final int EXIT_CODE_CMD_USAGE = 64;
    private static final int EXIT_CODE_IO_ERROR = 74;
    private static final int EXIT_CODE_ERROR = 70;

    public static void main(String[] args) {
        Tuple<Settings, Environment> initialSettings = InternalSettingsPreparer.prepareSettings(EMPTY_SETTINGS, true);

        try {
            Files.createDirectories(initialSettings.v2().pluginsFile());
        } catch (IOException e) {
            displayHelp("Unable to create plugins dir: " + initialSettings.v2().pluginsFile());
            System.exit(EXIT_CODE_ERROR);
        }

        String url = null;
        OutputMode outputMode = OutputMode.DEFAULT;
        String pluginName = null;
        TimeValue timeout = DEFAULT_TIMEOUT;
        int action = ACTION.NONE;

        if (args.length < 1) {
            displayHelp(null);
        }

        try {
            for (int c = 0; c < args.length; c++) {
                String command = args[c];
                switch (command) {
                    case "-u":
                    case "--url":
                    // deprecated versions:
                    case "url":
                    case "-url":
                        url = getCommandValue(args, ++c, "--url");
                        // Until update is supported, then supplying a URL implies installing
                        // By specifying this action, we also avoid silently failing without
                        //  dubious checks.
                        action = ACTION.INSTALL;
                        break;
                    case "-v":
                    case "--verbose":
                    // deprecated versions:
                    case "verbose":
                    case "-verbose":
                        outputMode = OutputMode.VERBOSE;
                        break;
                    case "-s":
                    case "--silent":
                    // deprecated versions:
                    case "silent":
                    case "-silent":
                        outputMode = OutputMode.SILENT;
                        break;
                    case "-i":
                    case "--install":
                    // deprecated versions:
                    case "install":
                    case "-install":
                        pluginName = getCommandValue(args, ++c, "--install");
                        action = ACTION.INSTALL;
                        break;
                    case "-r":
                    case "--remove":
                    // deprecated versions:
                    case "remove":
                    case "-remove":
                        pluginName = getCommandValue(args, ++c, "--remove");
                        action = ACTION.REMOVE;
                        break;
                    case "-t":
                    case "--timeout":
                    // deprecated versions:
                    case "timeout":
                    case "-timeout":
                        String timeoutValue = getCommandValue(args, ++c, "--timeout");
                        timeout = TimeValue.parseTimeValue(timeoutValue, DEFAULT_TIMEOUT);
                        break;
                    case "-l":
                    case "--list":
                        action = ACTION.LIST;
                        break;
                    case "-h":
                    case "--help":
                        displayHelp(null);
                        break;
                    default:
                        displayHelp("Command [" + command + "] unknown.");
                        // Unknown command. We break...
                        System.exit(EXIT_CODE_CMD_USAGE);
                }
            }
        } catch (Throwable e) {
            displayHelp("Error while parsing options: " + e.getClass().getSimpleName() +
                    ": " + e.getMessage());
            System.exit(EXIT_CODE_CMD_USAGE);
        }

        if (action > ACTION.NONE) {
            int exitCode = EXIT_CODE_ERROR; // we fail unless it's reset
            PluginManager pluginManager = new PluginManager(initialSettings.v2(), url, outputMode, timeout);
            switch (action) {
                case ACTION.INSTALL:
                    try {
                        pluginManager.log("-> Installing " + Strings.nullToEmpty(pluginName) + "...");
                        pluginManager.downloadAndExtract(pluginName);
                        exitCode = EXIT_CODE_OK;
                    } catch (IOException e) {
                        exitCode = EXIT_CODE_IO_ERROR;
                        pluginManager.log("Failed to install " + pluginName + ", reason: " + e.getMessage());
                    } catch (Throwable e) {
                        exitCode = EXIT_CODE_ERROR;
                        displayHelp("Error while installing plugin, reason: " + e.getClass().getSimpleName() +
                                ": " + e.getMessage());
                    }
                    break;
                case ACTION.REMOVE:
                    try {
                        pluginManager.log("-> Removing " + Strings.nullToEmpty(pluginName) + "...");
                        pluginManager.removePlugin(pluginName);
                        exitCode = EXIT_CODE_OK;
                    } catch (ElasticsearchIllegalArgumentException e) {
                        exitCode = EXIT_CODE_CMD_USAGE;
                        pluginManager.log("Failed to remove " + pluginName + ", reason: " + e.getMessage());
                    } catch (IOException e) {
                        exitCode = EXIT_CODE_IO_ERROR;
                        pluginManager.log("Failed to remove " + pluginName + ", reason: " + e.getMessage());
                    } catch (Throwable e) {
                        exitCode = EXIT_CODE_ERROR;
                        displayHelp("Error while removing plugin, reason: " + e.getClass().getSimpleName() +
                                ": " + e.getMessage());
                    }
                    break;
                case ACTION.LIST:
                    try {
                        pluginManager.listInstalledPlugins();
                        exitCode = EXIT_CODE_OK;
                    } catch (Throwable e) {
                        displayHelp("Error while listing plugins, reason: " + e.getClass().getSimpleName() +
                                ": " + e.getMessage());
                    }
                    break;

                default:
                    pluginManager.log("Unknown Action [" + action + "]");
                    exitCode = EXIT_CODE_ERROR;

            }
            System.exit(exitCode); // exit here!
        }
    }

    /**
     * Get the value for the {@code flag} at the specified {@code arg} of the command line {@code args}.
     * <p />
     * This is useful to avoid having to check for multiple forms of unset (e.g., "   " versus "" versus {@code null}).
     * @param args Incoming command line arguments.
     * @param arg Expected argument containing the value.
     * @param flag The flag whose value is being retrieved.
     * @return Never {@code null}. The trimmed value.
     * @throws NullPointerException if {@code args} is {@code null}.
     * @throws ArrayIndexOutOfBoundsException if {@code arg} is negative.
     * @throws ElasticsearchIllegalStateException if {@code arg} is &gt;= {@code args.length}.
     * @throws ElasticsearchIllegalArgumentException if the value evaluates to blank ({@code null} or only whitespace)
     */
    private static String getCommandValue(String[] args, int arg, String flag) {
        if (arg >= args.length) {
            throw new ElasticsearchIllegalStateException("missing value for " + flag + ". Usage: " + flag + " [value]");
        }

        // avoid having to interpret multiple forms of unset
        String trimmedValue = Strings.emptyToNull(args[arg].trim());

        // If we had a value that is blank, then fail immediately
        if (trimmedValue == null) {
            throw new ElasticsearchIllegalArgumentException(
                    "value for " + flag + "('" + args[arg] + "') must be set. Usage: " + flag + " [value]");
        }

        return trimmedValue;
    }

    private static void displayHelp(String message) {
        SysOut.println("Usage:");
        SysOut.println("    -u, --url     [plugin location]   : Set exact URL to download the plugin from");
        SysOut.println("    -i, --install [plugin name]       : Downloads and installs listed plugins [*]");
        SysOut.println("    -t, --timeout [duration]          : Timeout setting: 30s, 1m, 1h... (infinite by default)");
        SysOut.println("    -r, --remove  [plugin name]       : Removes listed plugins");
        SysOut.println("    -l, --list                        : List installed plugins");
        SysOut.println("    -v, --verbose                     : Prints verbose messages");
        SysOut.println("    -s, --silent                      : Run in silent mode");
        SysOut.println("    -h, --help                        : Prints this help message");
        SysOut.newline();
        SysOut.println(" [*] Plugin name could be:");
        SysOut.println("     elasticsearch/plugin/version for official elasticsearch plugins (download from download.elasticsearch.org)");
        SysOut.println("     groupId/artifactId/version   for community plugins (download from maven central or oss sonatype)");
        SysOut.println("     username/repository          for site plugins (download from github master)");

        if (message != null) {
            SysOut.newline();
            SysOut.println("Message:");
            SysOut.println("   " + message);
        }
    }

    private void debug(String line) {
        if (outputMode == OutputMode.VERBOSE) SysOut.println(line);
    }

    private void log(String line) {
        if (outputMode != OutputMode.SILENT) SysOut.println(line);
    }

    @SuppressForbidden(reason = "System#out")
    static class SysOut {

        public static void newline() {
            System.out.println();
        }
        public static void println(String msg) {
            System.out.println(msg);
        }

        public static PrintStream getOut() {
            return System.out;
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

        List<URL> urls() {
            List<URL> urls = new ArrayList<>();
            if (version != null) {
                // Elasticsearch download service
                addUrl(urls, "http://download.elasticsearch.org/" + user + "/" + repo + "/" + repo + "-" + version + ".zip");
                // Maven central repository
                addUrl(urls, "http://search.maven.org/remotecontent?filepath=" + user.replace('.', '/') + "/" + repo + "/" + version + "/" + repo + "-" + version + ".zip");
                // Sonatype repository
                addUrl(urls, "https://oss.sonatype.org/service/local/repositories/releases/content/" + user.replace('.', '/') + "/" + repo + "/" + version + "/" + repo + "-" + version + ".zip");
                // Github repository
                addUrl(urls, "https://github.com/" + user + "/" + repo + "/archive/" + version + ".zip");
            }
            // Github repository for master branch (assume site)
            addUrl(urls, "https://github.com/" + user + "/" + repo + "/archive/master.zip");
            return urls;
        }

        private static void addUrl(List<URL> urls, String url) {
            try {
                urls.add(new URL(url));
            } catch (MalformedURLException e) {
                // We simply ignore malformed URL
            }
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
            String[] elements = name.split("/");
            // We first consider the simplest form: pluginname
            String repo = elements[0];
            String user = null;
            String version = null;

            // We consider the form: username/pluginname
            if (elements.length > 1) {
                user = elements[0];
                repo = elements[1];

                // We consider the form: username/pluginname/version
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

}
