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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.SettingCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

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
class InstallPluginCommand extends SettingCommand {

    private static final String PROPERTY_STAGING_ID = "es.plugins.staging";

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

    public static final Set<PosixFilePermission> DIR_AND_EXECUTABLE_PERMS;
    public static final Set<PosixFilePermission> FILE_PERMS;

    static {
        Set<PosixFilePermission> dirAndExecutablePerms = new HashSet<>(7);
        // Directories and executables get chmod 755
        dirAndExecutablePerms.add(PosixFilePermission.OWNER_EXECUTE);
        dirAndExecutablePerms.add(PosixFilePermission.OWNER_READ);
        dirAndExecutablePerms.add(PosixFilePermission.OWNER_WRITE);
        dirAndExecutablePerms.add(PosixFilePermission.GROUP_EXECUTE);
        dirAndExecutablePerms.add(PosixFilePermission.GROUP_READ);
        dirAndExecutablePerms.add(PosixFilePermission.OTHERS_READ);
        dirAndExecutablePerms.add(PosixFilePermission.OTHERS_EXECUTE);
        DIR_AND_EXECUTABLE_PERMS = Collections.unmodifiableSet(dirAndExecutablePerms);

        Set<PosixFilePermission> filePerms = new HashSet<>(4);
        // Files get chmod 644
        filePerms.add(PosixFilePermission.OWNER_READ);
        filePerms.add(PosixFilePermission.OWNER_WRITE);
        filePerms.add(PosixFilePermission.GROUP_READ);
        filePerms.add(PosixFilePermission.OTHERS_READ);
        FILE_PERMS = Collections.unmodifiableSet(filePerms);
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
    protected void execute(Terminal terminal, OptionSet options, Map<String, String> settings) throws Exception {
        // TODO: in jopt-simple 5.0 we can enforce a min/max number of positional args
        List<String> args = arguments.values(options);
        if (args.size() != 1) {
            throw new UserError(ExitCodes.USAGE, "Must supply a single plugin id argument");
        }
        String pluginId = args.get(0);
        boolean isBatch = options.has(batchOption) || System.console() == null;
        execute(terminal, pluginId, isBatch, settings);
    }

    // pkg private for testing
    void execute(Terminal terminal, String pluginId, boolean isBatch, Map<String, String> settings) throws Exception {
        final Environment env = InternalSettingsPreparer.prepareEnvironment(Settings.EMPTY, terminal, settings);
        // TODO: remove this leniency!! is it needed anymore?
        if (Files.exists(env.pluginsFile()) == false) {
            terminal.println("Plugins directory [" + env.pluginsFile() + "] does not exist. Creating...");
            Files.createDirectory(env.pluginsFile());
        }

        Path pluginZip = download(terminal, pluginId, env.tmpFile());
        Path extractedZip = unzip(pluginZip, env.pluginsFile());
        install(terminal, isBatch, extractedZip, env);
    }

    /** Downloads the plugin and returns the file it was downloaded to. */
    private Path download(Terminal terminal, String pluginId, Path tmpDir) throws Exception {
        if (OFFICIAL_PLUGINS.contains(pluginId)) {
            final String version = Version.CURRENT.toString();
            final String url;
            final String stagingHash = System.getProperty(PROPERTY_STAGING_ID);
            if (stagingHash != null) {
                url = String.format(
                        Locale.ROOT,
                        "https://download.elastic.co/elasticsearch/staging/%1$s-%2$s/org/elasticsearch/plugin/%3$s/%1$s/%3$s-%1$s.zip",
                        version,
                        stagingHash,
                        pluginId);
            } else {
                url = String.format(
                        Locale.ROOT,
                        "https://download.elastic.co/elasticsearch/release/org/elasticsearch/plugin/%1$s/%2$s/%1$s-%2$s.zip",
                        pluginId,
                        version);
            }
            terminal.println("-> Downloading " + pluginId + " from elastic");
            return downloadZipAndChecksum(terminal, url, tmpDir);
        }

        // now try as maven coordinates, a valid URL would only have a colon and slash
        String[] coordinates = pluginId.split(":");
        if (coordinates.length == 3 && pluginId.contains("/") == false) {
            String mavenUrl = String.format(Locale.ROOT, "https://repo1.maven.org/maven2/%1$s/%2$s/%3$s/%2$s-%3$s.zip",
                    coordinates[0].replace(".", "/") /* groupId */, coordinates[1] /* artifactId */, coordinates[2] /* version */);
            terminal.println("-> Downloading " + pluginId + " from maven central");
            return downloadZipAndChecksum(terminal, mavenUrl, tmpDir);
        }

        // fall back to plain old URL
        if (pluginId.contains(":/") == false) {
            // definitely not a valid url, so assume it is a plugin name
            List<String> plugins = checkMisspelledPlugin(pluginId);
            String msg = "Unknown plugin " + pluginId;
            if (plugins.isEmpty() == false) {
                msg += ", did you mean " + (plugins.size() == 1 ? "[" + plugins.get(0) + "]": "any of " + plugins.toString()) + "?";
            }
            throw new UserError(ExitCodes.USAGE, msg);
        }
        terminal.println("-> Downloading " + URLDecoder.decode(pluginId, "UTF-8"));
        return downloadZip(terminal, pluginId, tmpDir);
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
    private Path downloadZip(Terminal terminal, String urlString, Path tmpDir) throws IOException {
        terminal.println(VERBOSE, "Retrieving zip from " + urlString);
        URL url = new URL(urlString);
        Path zip = Files.createTempFile(tmpDir, null, ".zip");
        try (InputStream in = url.openStream()) {
            // must overwrite since creating the temp file above actually created the file
            Files.copy(in, zip, StandardCopyOption.REPLACE_EXISTING);
        }
        return zip;
    }

    /** Downloads a zip from the url, as well as a SHA1 checksum, and checks the checksum. */
    private Path downloadZipAndChecksum(Terminal terminal, String urlString, Path tmpDir) throws Exception {
        Path zip = downloadZip(terminal, urlString, tmpDir);

        URL checksumUrl = new URL(urlString + ".sha1");
        final String expectedChecksum;
        try (InputStream in = checksumUrl.openStream()) {
            BufferedReader checksumReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            expectedChecksum = checksumReader.readLine();
            if (checksumReader.readLine() != null) {
                throw new UserError(ExitCodes.IO_ERROR, "Invalid checksum file at " + checksumUrl);
            }
        }

        byte[] zipbytes = Files.readAllBytes(zip);
        String gotChecksum = MessageDigests.toHexString(MessageDigests.sha1().digest(zipbytes));
        if (expectedChecksum.equals(gotChecksum) == false) {
            throw new UserError(ExitCodes.IO_ERROR, "SHA1 mismatch, expected " + expectedChecksum + " but got " + gotChecksum);
        }

        return zip;
    }

    private Path unzip(Path zip, Path pluginsDir) throws IOException, UserError {
        // unzip plugin to a staging temp dir

        final Path target = stagingDirectory(pluginsDir);

        boolean hasEsDir = false;
        // TODO: we should wrap this in a try/catch and try deleting the target dir on failure?
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

                // Using the entry name as a path can result in an entry outside of the plugin dir, either if the
                // name starts with the root of the filesystem, or it is a relative entry like ../whatever.
                // This check attempts to identify both cases by first normalizing the path (which removes foo/..)
                // and ensuring the normalized entry is still rooted with the target plugin directory.
                if (targetFile.normalize().startsWith(target) == false) {
                    throw new IOException("Zip contains entry name '" + entry.getName() + "' resolving outside of plugin directory");
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
            throw new UserError(ExitCodes.DATA_ERROR, "`elasticsearch` directory is missing in the plugin zip");
        }
        return target;
    }

    private Path stagingDirectory(Path pluginsDir) throws IOException {
        try {
            return Files.createTempDirectory(pluginsDir, ".installing-", PosixFilePermissions.asFileAttribute(DIR_AND_EXECUTABLE_PERMS));
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
        terminal.println(VERBOSE, info.toString());

        // don't let luser install plugin as a module...
        // they might be unavoidably in maven central and are packaged up the same way)
        if (MODULES.contains(info.getName())) {
            throw new UserError(ExitCodes.USAGE, "plugin '" + info.getName() + "' cannot be installed like this, it is a system module");
        }

        // check for jar hell before any copying
        jarHellCheck(pluginRoot, env.pluginsFile());

        // read optional security policy (extra permissions)
        // if it exists, confirm or warn the user
        Path policy = pluginRoot.resolve(PluginInfo.ES_PLUGIN_POLICY);
        if (Files.exists(policy)) {
            PluginSecurity.readPolicy(policy, terminal, env, isBatch);
        }

        return info;
    }

    /** check a candidate plugin for jar hell before installing it */
    void jarHellCheck(Path candidate, Path pluginsDir) throws Exception {
        // create list of current jars in classpath
        final List<URL> jars = new ArrayList<>();
        jars.addAll(Arrays.asList(JarHell.parseClassPath()));

        // read existing bundles. this does some checks on the installation too.
        PluginsService.getPluginBundles(pluginsDir);

        // add plugin jars to the list
        Path pluginJars[] = FileSystemUtils.files(candidate, "*.jar");
        for (Path jar : pluginJars) {
            jars.add(jar.toUri().toURL());
        }
        // TODO: no jars should be an error
        // TODO: verify the classname exists in one of the jars!

        // check combined (current classpath + new jars to-be-added)
        JarHell.checkJarHell(jars.toArray(new URL[jars.size()]));
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
            if (Files.exists(destination)) {
                throw new UserError(
                    ExitCodes.USAGE,
                    "plugin directory " + destination.toAbsolutePath() +
                        " already exists. To update the plugin, uninstall it first using 'remove " + info.getName() + "' command");
            }

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
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(destination)) {
                for (Path pluginFile : stream) {
                    if (Files.isDirectory(pluginFile)) {
                        setFileAttributes(pluginFile, DIR_AND_EXECUTABLE_PERMS);
                    } else {
                        setFileAttributes(pluginFile, FILE_PERMS);
                    }
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
            throw new UserError(ExitCodes.IO_ERROR, "bin in plugin " + info.getName() + " is not a directory");
        }
        Files.createDirectory(destBinDir);
        setFileAttributes(destBinDir, DIR_AND_EXECUTABLE_PERMS);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tmpBinDir)) {
            for (Path srcFile : stream) {
                if (Files.isDirectory(srcFile)) {
                    throw new UserError(
                        ExitCodes.DATA_ERROR,
                        "Directories not allowed in bin dir for plugin " + info.getName() + ", found " + srcFile.getFileName());
                }

                Path destFile = destBinDir.resolve(tmpBinDir.relativize(srcFile));
                Files.copy(srcFile, destFile);
                setFileAttributes(destFile, DIR_AND_EXECUTABLE_PERMS);
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
            throw new UserError(ExitCodes.IO_ERROR, "config in plugin " + info.getName() + " is not a directory");
        }

        Files.createDirectories(destConfigDir);
        setFileAttributes(destConfigDir, DIR_AND_EXECUTABLE_PERMS);
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
                    throw new UserError(ExitCodes.DATA_ERROR, "Directories not allowed in config dir for plugin " + info.getName());
                }

                Path destFile = destConfigDir.resolve(tmpConfigDir.relativize(srcFile));
                if (Files.exists(destFile) == false) {
                    Files.copy(srcFile, destFile);
                    setFileAttributes(destFile, FILE_PERMS);
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
}
