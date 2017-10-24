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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.env.Environment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;

/**
 * A command for the plugin cli to install a plugin into elasticsearch.
 *
 * The install command takes a plugin id, which may be any of the following:
 * <ul>
 * <li>An official elasticsearch plugin name</li>
 * <li>Maven coordinates to a plugin zip</li>
 * <li>A URL to a plugin zip</li>
 * </ul>
 *
 * Plugins are packaged as zip files. Each packaged plugin must contain a
 * plugin properties file. See {@link PluginInfo}.
 * <p>
 * The installation process first extracts the plugin files into a temporary
 * directory in order to verify the plugin satisfies the following requirements:
 * <ul>
 * <li>Jar hell does not exist, either between the plugin's own jars, or with elasticsearch</li>
 * <li>The plugin is not a module already provided with elasticsearch</li>
 * <li>If the plugin contains extra security permissions, the policy file is validated</li>
 * </ul>
 * <p>
 * A plugin may also contain an optional {@code bin} directory which contains scripts. The
 * scripts will be installed into a subdirectory of the elasticsearch bin directory, using
 * the name of the plugin, and the scripts will be marked executable.
 * <p>
 * A plugin may also contain an optional {@code config} directory which contains configuration
 * files specific to the plugin. The config files be installed into a subdirectory of the
 * elasticsearch config directory, using the name of the plugin. If any files to be installed
 * already exist, they will be skipped.
 */
class InstallPluginCommand extends EnvironmentAwareCommand {

    private static final String PROPERTY_STAGING_ID = "es.plugins.staging";

    // exit codes for install
    /** A plugin with the same name is already installed. */
    static final int PLUGIN_EXISTS = 1;
    /** The plugin zip is not properly structured. */
    static final int PLUGIN_MALFORMED = 2;


    /** The builtin modules, which are plugins, but cannot be installed or removed. */
    static final Set<String> MODULES;
    static {
        try (InputStream stream = InstallPluginCommand.class.getResourceAsStream("/modules.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            Set<String> modules = new HashSet<>();
            String line = reader.readLine();
            while (line != null) {
                modules.add(line.trim());
                line = reader.readLine();
            }
            MODULES = Collections.unmodifiableSet(modules);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** The official plugins that can be installed simply by name. */
    static final Set<String> OFFICIAL_PLUGINS;
    static {
        try (InputStream stream = InstallPluginCommand.class.getResourceAsStream("/plugins.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            Set<String> plugins = new TreeSet<>(); // use tree set to get sorting for help command
            String line = reader.readLine();
            while (line != null) {
                plugins.add(line.trim());
                line = reader.readLine();
            }
            plugins.add("x-pack");
            OFFICIAL_PLUGINS = Collections.unmodifiableSet(plugins);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final OptionSpec<Void> batchOption;
    private final OptionSpec<String> arguments;

    static final Set<PosixFilePermission> BIN_DIR_PERMS;
    static final Set<PosixFilePermission> BIN_FILES_PERMS;
    static final Set<PosixFilePermission> CONFIG_DIR_PERMS;
    static final Set<PosixFilePermission> CONFIG_FILES_PERMS;
    static final Set<PosixFilePermission> PLUGIN_DIR_PERMS;
    static final Set<PosixFilePermission> PLUGIN_FILES_PERMS;

    static {
        // Bin directory get chmod 755
        BIN_DIR_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rwxr-xr-x"));

        // Bin files also get chmod 755
        BIN_FILES_PERMS = BIN_DIR_PERMS;

        // Config directory get chmod 750
        CONFIG_DIR_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rwxr-x---"));

        // Config files get chmod 660
        CONFIG_FILES_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rw-rw----"));

        // Plugin directory get chmod 755
        PLUGIN_DIR_PERMS = BIN_DIR_PERMS;

        // Plugins files get chmod 644
        PLUGIN_FILES_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rw-r--r--"));
    }

    InstallPluginCommand() {
        super("Install a plugin");
        this.batchOption = parser.acceptsAll(Arrays.asList("b", "batch"),
                "Enable batch mode explicitly, automatic confirmation of security permission");
        this.arguments = parser.nonOptions("plugin id");
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("The following official plugins may be installed by name:");
        for (String plugin : OFFICIAL_PLUGINS) {
            terminal.println("  " + plugin);
        }
        terminal.println("");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        String pluginId = arguments.value(options);
        boolean isBatch = options.has(batchOption) || System.console() == null;
        execute(terminal, pluginId, isBatch, env);
    }

    // pkg private for testing
    void execute(Terminal terminal, String pluginId, boolean isBatch, Environment env) throws Exception {
        if (pluginId == null) {
            throw new UserException(ExitCodes.USAGE, "plugin id is required");
        }

        Path pluginZip = download(terminal, pluginId, env.tmpFile());
        Path extractedZip = unzip(pluginZip, env.pluginsFile());
        install(terminal, isBatch, extractedZip, env);
    }

    /** Downloads the plugin and returns the file it was downloaded to. */
    private Path download(Terminal terminal, String pluginId, Path tmpDir) throws Exception {
        if (OFFICIAL_PLUGINS.contains(pluginId)) {
            final String url = getElasticUrl(terminal, getStagingHash(), Version.CURRENT, pluginId, Platforms.PLATFORM_NAME);
            terminal.println("-> Downloading " + pluginId + " from elastic");
            return downloadZipAndChecksum(terminal, url, tmpDir, false);
        }

        // now try as maven coordinates, a valid URL would only have a colon and slash
        String[] coordinates = pluginId.split(":");
        if (coordinates.length == 3 && pluginId.contains("/") == false) {
            String mavenUrl = getMavenUrl(terminal, coordinates, Platforms.PLATFORM_NAME);
            terminal.println("-> Downloading " + pluginId + " from maven central");
            return downloadZipAndChecksum(terminal, mavenUrl, tmpDir, true);
        }

        // fall back to plain old URL
        if (pluginId.contains(":/") == false) {
            // definitely not a valid url, so assume it is a plugin name
            List<String> plugins = checkMisspelledPlugin(pluginId);
            String msg = "Unknown plugin " + pluginId;
            if (plugins.isEmpty() == false) {
                msg += ", did you mean " + (plugins.size() == 1 ? "[" + plugins.get(0) + "]": "any of " + plugins.toString()) + "?";
            }
            throw new UserException(ExitCodes.USAGE, msg);
        }
        terminal.println("-> Downloading " + URLDecoder.decode(pluginId, "UTF-8"));
        return downloadZip(terminal, pluginId, tmpDir);
    }

    // pkg private so tests can override
    String getStagingHash() {
        return System.getProperty(PROPERTY_STAGING_ID);
    }

    /** Returns the url for an official elasticsearch plugin. */
    private String getElasticUrl(Terminal terminal, String stagingHash, Version version,
                                        String pluginId, String platform) throws IOException {
        final String baseUrl;
        if (stagingHash != null) {
            baseUrl = String.format(Locale.ROOT,
                "https://staging.elastic.co/%s-%s/downloads/elasticsearch-plugins/%s", version, stagingHash, pluginId);
        } else {
            baseUrl = String.format(Locale.ROOT,
                "https://artifacts.elastic.co/downloads/elasticsearch-plugins/%s", pluginId);
        }
        final String platformUrl = String.format(Locale.ROOT, "%s/%s-%s-%s.zip", baseUrl, pluginId, platform, version);
        if (urlExists(terminal, platformUrl)) {
            return platformUrl;
        }
        return String.format(Locale.ROOT, "%s/%s-%s.zip", baseUrl, pluginId, version);
    }

    /** Returns the url for an elasticsearch plugin in maven. */
    private String getMavenUrl(Terminal terminal, String[] coordinates, String platform) throws IOException {
        final String groupId = coordinates[0].replace(".", "/");
        final String artifactId = coordinates[1];
        final String version = coordinates[2];
        final String baseUrl = String.format(Locale.ROOT, "https://repo1.maven.org/maven2/%s/%s/%s", groupId, artifactId, version);
        final String platformUrl = String.format(Locale.ROOT, "%s/%s-%s-%s.zip", baseUrl, artifactId, platform, version);
        if (urlExists(terminal, platformUrl)) {
            return platformUrl;
        }
        return String.format(Locale.ROOT, "%s/%s-%s.zip", baseUrl, artifactId, version);
    }

    /**
     * Returns {@code true} if the given url exists, and {@code false} otherwise.
     *
     * The given url must be {@code https} and existing means a {@code HEAD} request returns 200.
     */
    // pkg private for tests to manipulate
    @SuppressForbidden(reason = "Make HEAD request using URLConnection.connect()")
    boolean urlExists(Terminal terminal, String urlString) throws IOException {
        terminal.println(VERBOSE, "Checking if url exists: " + urlString);
        URL url = new URL(urlString);
        assert "https".equals(url.getProtocol()) : "Only http urls can be checked";
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.addRequestProperty("User-Agent", "elasticsearch-plugin-installer");
        urlConnection.setRequestMethod("HEAD");
        urlConnection.connect();
        return urlConnection.getResponseCode() == 200;
    }

    /** Returns all the official plugin names that look similar to pluginId. **/
    private List<String> checkMisspelledPlugin(String pluginId) {
        LevensteinDistance ld = new LevensteinDistance();
        List<Tuple<Float, String>> scoredKeys = new ArrayList<>();
        for (String officialPlugin : OFFICIAL_PLUGINS) {
            float distance = ld.getDistance(pluginId, officialPlugin);
            if (distance > 0.7f) {
                scoredKeys.add(new Tuple<>(distance, officialPlugin));
            }
        }
        CollectionUtil.timSort(scoredKeys, (a, b) -> b.v1().compareTo(a.v1()));
        return scoredKeys.stream().map((a) -> a.v2()).collect(Collectors.toList());
    }

    /** Downloads a zip from the url, into a temp file under the given temp dir. */
    // pkg private for tests
    @SuppressForbidden(reason = "We use getInputStream to download plugins")
    Path downloadZip(Terminal terminal, String urlString, Path tmpDir) throws IOException {
        terminal.println(VERBOSE, "Retrieving zip from " + urlString);
        URL url = new URL(urlString);
        Path zip = Files.createTempFile(tmpDir, null, ".zip");
        URLConnection urlConnection = url.openConnection();
        urlConnection.addRequestProperty("User-Agent", "elasticsearch-plugin-installer");
        int contentLength = urlConnection.getContentLength();
        try (InputStream in = new TerminalProgressInputStream(urlConnection.getInputStream(), contentLength, terminal)) {
            // must overwrite since creating the temp file above actually created the file
            Files.copy(in, zip, StandardCopyOption.REPLACE_EXISTING);
        }
        return zip;
    }

    /**
     * content length might be -1 for unknown and progress only makes sense if the content length is greater than 0
     */
    private class TerminalProgressInputStream extends ProgressInputStream {

        private final Terminal terminal;
        private int width = 50;
        private final boolean enabled;

        TerminalProgressInputStream(InputStream is, int expectedTotalSize, Terminal terminal) {
            super(is, expectedTotalSize);
            this.terminal = terminal;
            this.enabled = expectedTotalSize > 0;
        }

        @Override
        public void onProgress(int percent) {
            if (enabled) {
                int currentPosition = percent * width / 100;
                StringBuilder sb = new StringBuilder("\r[");
                sb.append(String.join("=", Collections.nCopies(currentPosition, "")));
                if (currentPosition > 0 && percent < 100) {
                    sb.append(">");
                }
                sb.append(String.join(" ", Collections.nCopies(width - currentPosition, "")));
                sb.append("] %s   ");
                if (percent == 100) {
                    sb.append("\n");
                }
                terminal.print(Terminal.Verbosity.NORMAL, String.format(Locale.ROOT, sb.toString(), percent + "%"));
            }
        }
    }

    /** Downloads a zip from the url, as well as a SHA512 (or SHA1) checksum, and checks the checksum. */
    // pkg private for tests
    @SuppressForbidden(reason = "We use openStream to download plugins")
    private Path downloadZipAndChecksum(Terminal terminal, String urlString, Path tmpDir, boolean allowSha1) throws Exception {
        Path zip = downloadZip(terminal, urlString, tmpDir);
        pathsToDeleteOnShutdown.add(zip);
        String checksumUrlString = urlString + ".sha512";
        URL checksumUrl = openUrl(checksumUrlString);
        String digestAlgo = "SHA-512";
        if (checksumUrl == null && allowSha1) {
            // fallback to sha1, until 7.0, but with warning
            terminal.println("Warning: sha512 not found, falling back to sha1. This behavior is deprecated and will be removed in a " +
                             "future release. Please update the plugin to use a sha512 checksum.");
            checksumUrlString = urlString + ".sha1";
            checksumUrl = openUrl(checksumUrlString);
            digestAlgo = "SHA-1";
        }
        if (checksumUrl == null) {
            throw new UserException(ExitCodes.IO_ERROR, "Plugin checksum missing: " + checksumUrlString);
        }
        final String expectedChecksum;
        try (InputStream in = checksumUrl.openStream()) {
            /*
             * The supported format of the SHA-1 files is a single-line file containing the SHA-1. The supported format of the SHA-512 files
             * is a single-line file containing the SHA-512 and the filename, separated by two spaces. For SHA-1, we verify that the hash
             * matches, and that the file contains a single line. For SHA-512, we verify that the hash and the filename match, and that the
             * file contains a single line.
             */
            if (digestAlgo.equals("SHA-1")) {
                final BufferedReader checksumReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                expectedChecksum = checksumReader.readLine();
                if (checksumReader.readLine() != null) {
                    throw new UserException(ExitCodes.IO_ERROR, "Invalid checksum file at " + checksumUrl);
                }
            } else {
                final BufferedReader checksumReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                final String checksumLine = checksumReader.readLine();
                final String[] fields = checksumLine.split(" {2}");
                if (fields.length != 2) {
                    throw new UserException(ExitCodes.IO_ERROR, "Invalid checksum file at " + checksumUrl);
                }
                expectedChecksum = fields[0];
                final String[] segments = URI.create(urlString).getPath().split("/");
                final String expectedFile = segments[segments.length - 1];
                if (fields[1].equals(expectedFile) == false) {
                    final String message = String.format(
                            Locale.ROOT,
                            "checksum file at [%s] is not for this plugin, expected [%s] but was [%s]",
                            checksumUrl,
                            expectedFile,
                            fields[1]);
                    throw new UserException(ExitCodes.IO_ERROR, message);
                }
                if (checksumReader.readLine() != null) {
                    throw new UserException(ExitCodes.IO_ERROR, "Invalid checksum file at " + checksumUrl);
                }
            }
        }

        byte[] zipbytes = Files.readAllBytes(zip);
        String gotChecksum = MessageDigests.toHexString(MessageDigest.getInstance(digestAlgo).digest(zipbytes));
        if (expectedChecksum.equals(gotChecksum) == false) {
            throw new UserException(ExitCodes.IO_ERROR,
                digestAlgo + " mismatch, expected " + expectedChecksum + " but got " + gotChecksum);
        }

        return zip;
    }

    /**
     * Creates a URL and opens a connection.
     *
     * If the URL returns a 404, {@code null} is returned, otherwise the open URL opject is returned.
     */
    // pkg private for tests
    URL openUrl(String urlString) throws Exception {
        URL checksumUrl = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection)checksumUrl.openConnection();
        if (connection.getResponseCode() == 404) {
            return null;
        }
        return checksumUrl;
    }

    private Path unzip(Path zip, Path pluginsDir) throws IOException, UserException {
        // unzip plugin to a staging temp dir

        final Path target = stagingDirectory(pluginsDir);
        pathsToDeleteOnShutdown.add(target);

        boolean hasEsDir = false;
        try (ZipInputStream zipInput = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            byte[] buffer = new byte[8192];
            while ((entry = zipInput.getNextEntry()) != null) {
                if (entry.getName().startsWith("elasticsearch/") == false) {
                    // only extract the elasticsearch directory
                    continue;
                }
                hasEsDir = true;
                Path targetFile = target.resolve(entry.getName().substring("elasticsearch/".length()));

                // Using the entry name as a path can result in an entry outside of the plugin dir,
                // either if the name starts with the root of the filesystem, or it is a relative
                // entry like ../whatever. This check attempts to identify both cases by first
                // normalizing the path (which removes foo/..) and ensuring the normalized entry
                // is still rooted with the target plugin directory.
                if (targetFile.normalize().startsWith(target) == false) {
                    throw new UserException(PLUGIN_MALFORMED, "Zip contains entry name '" +
                        entry.getName() + "' resolving outside of plugin directory");
                }

                // be on the safe side: do not rely on that directories are always extracted
                // before their children (although this makes sense, but is it guaranteed?)
                if (!Files.isSymbolicLink(targetFile.getParent())) {
                    Files.createDirectories(targetFile.getParent());
                }
                if (entry.isDirectory() == false) {
                    try (OutputStream out = Files.newOutputStream(targetFile)) {
                        int len;
                        while ((len = zipInput.read(buffer)) >= 0) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
                zipInput.closeEntry();
            }
        }
        Files.delete(zip);
        if (hasEsDir == false) {
            IOUtils.rm(target);
            throw new UserException(PLUGIN_MALFORMED,
                                    "`elasticsearch` directory is missing in the plugin zip");
        }
        return target;
    }

    private Path stagingDirectory(Path pluginsDir) throws IOException {
        try {
            return Files.createTempDirectory(pluginsDir, ".installing-", PosixFilePermissions.asFileAttribute(PLUGIN_DIR_PERMS));
        } catch (IllegalArgumentException e) {
            // Jimfs throws an IAE where it should throw an UOE
            // remove when google/jimfs#30 is integrated into Jimfs
            // and the Jimfs test dependency is upgraded to include
            // this pull request
            final StackTraceElement[] elements = e.getStackTrace();
            if (elements.length >= 1 &&
                elements[0].getClassName().equals("com.google.common.jimfs.AttributeService") &&
                elements[0].getMethodName().equals("setAttributeInternal")) {
                return stagingDirectoryWithoutPosixPermissions(pluginsDir);
            } else {
                throw e;
            }
        } catch (UnsupportedOperationException e) {
            return stagingDirectoryWithoutPosixPermissions(pluginsDir);
        }
    }

    private Path stagingDirectoryWithoutPosixPermissions(Path pluginsDir) throws IOException {
        return Files.createTempDirectory(pluginsDir, ".installing-");
    }

    /** Load information about the plugin, and verify it can be installed with no errors. */
    private PluginInfo verify(Terminal terminal, Path pluginRoot, boolean isBatch, Environment env) throws Exception {
        // read and validate the plugin descriptor
        PluginInfo info = PluginInfo.readFromProperties(pluginRoot);

        // checking for existing version of the plugin
        final Path destination = env.pluginsFile().resolve(info.getName());
        if (Files.exists(destination)) {
            final String message = String.format(
                Locale.ROOT,
                "plugin directory [%s] already exists; if you need to update the plugin, " +
                    "uninstall it first using command 'remove %s'",
                destination.toAbsolutePath(),
                info.getName());
            throw new UserException(PLUGIN_EXISTS, message);
        }

        PluginsService.checkForFailedPluginRemovals(env.pluginsFile());

        terminal.println(VERBOSE, info.toString());

        // don't let user install plugin as a module...
        // they might be unavoidably in maven central and are packaged up the same way)
        if (MODULES.contains(info.getName())) {
            throw new UserException(ExitCodes.USAGE, "plugin '" + info.getName() +
                "' cannot be installed like this, it is a system module");
        }

        // check for jar hell before any copying
        jarHellCheck(pluginRoot, env.pluginsFile());

        // read optional security policy (extra permissions)
        // if it exists, confirm or warn the user
        Path policy = pluginRoot.resolve(PluginInfo.ES_PLUGIN_POLICY);
        if (Files.exists(policy)) {
            PluginSecurity.readPolicy(info, policy, terminal, env::tmpFile, isBatch);
        }

        return info;
    }

    /** check a candidate plugin for jar hell before installing it */
    void jarHellCheck(Path candidate, Path pluginsDir) throws Exception {
        // create list of current jars in classpath
        final Set<URL> jars = new HashSet<>(JarHell.parseClassPath());

        // read existing bundles. this does some checks on the installation too.
        PluginsService.getPluginBundles(pluginsDir);

        // add plugin jars to the list
        Path pluginJars[] = FileSystemUtils.files(candidate, "*.jar");
        for (Path jar : pluginJars) {
            if (jars.add(jar.toUri().toURL()) == false) {
                throw new IllegalStateException("jar hell! duplicate plugin jar: " + jar);
            }
        }
        // TODO: no jars should be an error
        // TODO: verify the classname exists in one of the jars!

        // check combined (current classpath + new jars to-be-added)
        JarHell.checkJarHell(jars);
    }

    /**
     * Installs the plugin from {@code tmpRoot} into the plugins dir.
     * If the plugin has a bin dir and/or a config dir, those are copied.
     */
    private void install(Terminal terminal, boolean isBatch, Path tmpRoot, Environment env) throws Exception {
        List<Path> deleteOnFailure = new ArrayList<>();
        deleteOnFailure.add(tmpRoot);

        try {
            PluginInfo info = verify(terminal, tmpRoot, isBatch, env);
            final Path destination = env.pluginsFile().resolve(info.getName());

            Path tmpBinDir = tmpRoot.resolve("bin");
            if (Files.exists(tmpBinDir)) {
                Path destBinDir = env.binFile().resolve(info.getName());
                deleteOnFailure.add(destBinDir);
                installBin(info, tmpBinDir, destBinDir);
            }

            Path tmpConfigDir = tmpRoot.resolve("config");
            if (Files.exists(tmpConfigDir)) {
                // some files may already exist, and we don't remove plugin config files on plugin removal,
                // so any installed config files are left on failure too
                installConfig(info, tmpConfigDir, env.configFile().resolve(info.getName()));
            }

            Files.move(tmpRoot, destination, StandardCopyOption.ATOMIC_MOVE);
            Files.walkFileTree(destination, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                    if ("bin".equals(file.getParent().getFileName().toString())) {
                        setFileAttributes(file, BIN_FILES_PERMS);
                    } else {
                        setFileAttributes(file, PLUGIN_FILES_PERMS);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                    setFileAttributes(dir, PLUGIN_DIR_PERMS);
                    return FileVisitResult.CONTINUE;
                }
            });

            if (info.requiresKeystore()) {
                KeyStoreWrapper keystore = KeyStoreWrapper.load(env.configFile());
                if (keystore == null) {
                    terminal.println("Elasticsearch keystore is required by plugin [" + info.getName() + "], creating...");
                    keystore = KeyStoreWrapper.create(new char[0]);
                    keystore.save(env.configFile());
                }
            }

            terminal.println("-> Installed " + info.getName());

        } catch (Exception installProblem) {
            try {
                IOUtils.rm(deleteOnFailure.toArray(new Path[0]));
            } catch (IOException exceptionWhileRemovingFiles) {
                installProblem.addSuppressed(exceptionWhileRemovingFiles);
            }
            throw installProblem;
        }
    }

    /** Copies the files from {@code tmpBinDir} into {@code destBinDir}, along with permissions from dest dirs parent. */
    private void installBin(PluginInfo info, Path tmpBinDir, Path destBinDir) throws Exception {
        if (Files.isDirectory(tmpBinDir) == false) {
            throw new UserException(PLUGIN_MALFORMED, "bin in plugin " + info.getName() + " is not a directory");
        }
        Files.createDirectory(destBinDir);
        setFileAttributes(destBinDir, BIN_DIR_PERMS);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tmpBinDir)) {
            for (Path srcFile : stream) {
                if (Files.isDirectory(srcFile)) {
                    throw new UserException(PLUGIN_MALFORMED, "Directories not allowed in bin dir " +
                        "for plugin " + info.getName() + ", found " + srcFile.getFileName());
                }

                Path destFile = destBinDir.resolve(tmpBinDir.relativize(srcFile));
                Files.copy(srcFile, destFile);
                setFileAttributes(destFile, BIN_FILES_PERMS);
            }
        }
        IOUtils.rm(tmpBinDir); // clean up what we just copied
    }

    /**
     * Copies the files from {@code tmpConfigDir} into {@code destConfigDir}.
     * Any files existing in both the source and destination will be skipped.
     */
    private void installConfig(PluginInfo info, Path tmpConfigDir, Path destConfigDir) throws Exception {
        if (Files.isDirectory(tmpConfigDir) == false) {
            throw new UserException(PLUGIN_MALFORMED,
                "config in plugin " + info.getName() + " is not a directory");
        }

        Files.createDirectories(destConfigDir);
        setFileAttributes(destConfigDir, CONFIG_DIR_PERMS);
        final PosixFileAttributeView destConfigDirAttributesView =
            Files.getFileAttributeView(destConfigDir.getParent(), PosixFileAttributeView.class);
        final PosixFileAttributes destConfigDirAttributes =
            destConfigDirAttributesView != null ? destConfigDirAttributesView.readAttributes() : null;
        if (destConfigDirAttributes != null) {
            setOwnerGroup(destConfigDir, destConfigDirAttributes);
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tmpConfigDir)) {
            for (Path srcFile : stream) {
                if (Files.isDirectory(srcFile)) {
                    throw new UserException(PLUGIN_MALFORMED,
                        "Directories not allowed in config dir for plugin " + info.getName());
                }

                Path destFile = destConfigDir.resolve(tmpConfigDir.relativize(srcFile));
                if (Files.exists(destFile) == false) {
                    Files.copy(srcFile, destFile);
                    setFileAttributes(destFile, CONFIG_FILES_PERMS);
                    if (destConfigDirAttributes != null) {
                        setOwnerGroup(destFile, destConfigDirAttributes);
                    }
                }
            }
        }
        IOUtils.rm(tmpConfigDir); // clean up what we just copied
    }

    private static void setOwnerGroup(final Path path, final PosixFileAttributes attributes) throws IOException {
        Objects.requireNonNull(attributes);
        PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        assert fileAttributeView != null;
        fileAttributeView.setOwner(attributes.owner());
        fileAttributeView.setGroup(attributes.group());
    }

    /**
     * Sets the attributes for a path iff posix attributes are supported
     */
    private static void setFileAttributes(final Path path, final Set<PosixFilePermission> permissions) throws IOException {
        PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (fileAttributeView != null) {
            Files.setPosixFilePermissions(path, permissions);
        }
    }

    private final List<Path> pathsToDeleteOnShutdown = new ArrayList<>();

    @Override
    public void close() throws IOException {
        IOUtils.rm(pathsToDeleteOnShutdown.toArray(new Path[pathsToDeleteOnShutdown.size()]));
    }

}
