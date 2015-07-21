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
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.http.client.HttpDownloadHelper;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.common.cli.Terminal.Verbosity.VERBOSE;
import static org.elasticsearch.common.io.FileSystemUtils.moveFilesWithoutOverwriting;

/**
 *
 */
public class PluginManager {

    public enum OutputMode {
        DEFAULT, SILENT, VERBOSE
    }

    private static final ImmutableSet<String> BLACKLIST = ImmutableSet.<String>builder()
            .add("elasticsearch",
                    "elasticsearch.bat",
                    "elasticsearch.in.sh",
                    "plugin",
                    "plugin.bat",
                    "service.bat").build();

    static final ImmutableSet<String> OFFICIAL_PLUGINS = ImmutableSet.<String>builder()
            .add(
                    "elasticsearch-analysis-icu",
                    "elasticsearch-analysis-kuromoji",
                    "elasticsearch-analysis-phonetic",
                    "elasticsearch-analysis-smartcn",
                    "elasticsearch-analysis-stempel",
                    "elasticsearch-cloud-aws",
                    "elasticsearch-cloud-azure",
                    "elasticsearch-cloud-gce",
                    "elasticsearch-delete-by-query",
                    "elasticsearch-lang-javascript",
                    "elasticsearch-lang-python"
            ).build();

    private final Environment environment;
    private String url;
    private OutputMode outputMode;
    private TimeValue timeout;

    public PluginManager(Environment environment, String url, OutputMode outputMode, TimeValue timeout) {
        this.environment = environment;
        this.url = url;
        this.outputMode = outputMode;
        this.timeout = timeout;
    }

    public void downloadAndExtract(String name, Terminal terminal) throws IOException {
        if (name == null) {
            throw new IllegalArgumentException("plugin name must be supplied with install [name].");
        }
        HttpDownloadHelper downloadHelper = new HttpDownloadHelper();
        boolean downloaded = false;
        HttpDownloadHelper.DownloadProgress progress;
        if (outputMode == OutputMode.SILENT) {
            progress = new HttpDownloadHelper.NullProgress();
        } else {
            progress = new HttpDownloadHelper.VerboseProgress(terminal.writer());
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
            throw new IOException("plugin directory " + extractLocation.toAbsolutePath() + " already exists. To update the plugin, uninstall it first using remove " + name + " command");
        }

        // first, try directly from the URL provided
        if (url != null) {
            URL pluginUrl = new URL(url);
            terminal.println("Trying %s ...", pluginUrl.toExternalForm());
            try {
                downloadHelper.download(pluginUrl, pluginFile, progress, this.timeout);
                downloaded = true;
            } catch (ElasticsearchTimeoutException e) {
                throw e;
            } catch (Exception e) {
                // ignore
                terminal.println("Failed: %s", ExceptionsHelper.detailedMessage(e));
            }
        } else {
            if (PluginHandle.isOfficialPlugin(pluginHandle.repo, pluginHandle.user, pluginHandle.version)) {
                checkForOfficialPlugins(pluginHandle.name);
            }
        }

        if (!downloaded) {
            // We try all possible locations
            for (URL url : pluginHandle.urls()) {
                terminal.println("Trying %s ...", url.toExternalForm());
                try {
                    downloadHelper.download(url, pluginFile, progress, this.timeout);
                    downloaded = true;
                    break;
                } catch (ElasticsearchTimeoutException e) {
                    throw e;
                } catch (Exception e) {
                    terminal.println(VERBOSE, "Failed: %s", ExceptionsHelper.detailedMessage(e));
                }
            }
        }

        if (!downloaded) {
            throw new IOException("failed to download out of all possible locations..., use --verbose to get detailed information");
        }

        // unzip plugin to a temp dir
        Path tmp = unzipToTemporary(pluginFile);

        // create list of current jars in classpath
        final List<URL> jars = new ArrayList<>();
        ClassLoader loader = PluginManager.class.getClassLoader();
        if (loader instanceof URLClassLoader) {
            Collections.addAll(jars, ((URLClassLoader) loader).getURLs());
        }

        // add any jars we find in the plugin to the list
        Files.walkFileTree(tmp, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(".jar")) {
                    jars.add(file.toUri().toURL());
                }
                return FileVisitResult.CONTINUE;
            }
        });

        // check combined (current classpath + new jars to-be-added)
        try {
            JarHell.checkJarHell(jars.toArray(new URL[jars.size()]));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        // cleanup
        IOUtils.rm(tmp);

        // TODO: we have a tmpdir made above, so avoid zipfilesystem
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
            terminal.println("Installed %s into %s", name, extractLocation.toAbsolutePath());
        } catch (Exception e) {
            terminal.printError("failed to extract plugin [%s]: %s", pluginFile, ExceptionsHelper.detailedMessage(e));
            return;
        } finally {
            try {
                Files.delete(pluginFile);
            } catch (Exception ex) {
                terminal.printError("Failed to delete plugin file %s %s", pluginFile, ex);
            }
        }

        if (FileSystemUtils.hasExtensions(extractLocation, ".java")) {
            terminal.printError("Plugin installation assumed to be site plugin, but contains source code, aborting installation...");
            try {
                IOUtils.rm(extractLocation);
            } catch(Exception ex) {
                terminal.printError("Failed to remove site plugin from path %s - %s", extractLocation, ex.getMessage());
            }
            throw new IllegalArgumentException("Plugin installation assumed to be site plugin, but contains source code, aborting installation.");
        }

        // It could potentially be a non explicit _site plugin
        boolean potentialSitePlugin = true;
        Path binFile = extractLocation.resolve("bin");
        if (Files.isDirectory(binFile)) {
            Path toLocation = pluginHandle.binDir(environment);
            terminal.println(VERBOSE, "Found bin, moving to %s", toLocation.toAbsolutePath());
            if (Files.exists(toLocation)) {
                IOUtils.rm(toLocation);
            }
            try {
                FileSystemUtils.move(binFile, toLocation);
            } catch (IOException e) {
                throw new IOException("Could not move [" + binFile + "] to [" + toLocation + "]", e);
            }
            if (Files.getFileStore(toLocation).supportsFileAttributeView(PosixFileAttributeView.class)) {
                // add read and execute permissions to existing perms, so execution will work.
                // read should generally be set already, but set it anyway: don't rely on umask...
                final Set<PosixFilePermission> executePerms = new HashSet<>();
                executePerms.add(PosixFilePermission.OWNER_READ);
                executePerms.add(PosixFilePermission.GROUP_READ);
                executePerms.add(PosixFilePermission.OTHERS_READ);
                executePerms.add(PosixFilePermission.OWNER_EXECUTE);
                executePerms.add(PosixFilePermission.GROUP_EXECUTE);
                executePerms.add(PosixFilePermission.OTHERS_EXECUTE);
                Files.walkFileTree(toLocation, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (attrs.isRegularFile()) {
                            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(file);
                            perms.addAll(executePerms);
                            Files.setPosixFilePermissions(file, perms);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } else {
                terminal.println(VERBOSE, "Skipping posix permissions - filestore doesn't support posix permission");
            }
            terminal.println(VERBOSE, "Installed %s into %s", name, toLocation.toAbsolutePath());
            potentialSitePlugin = false;
        }

        Path configFile = extractLocation.resolve("config");
        if (Files.isDirectory(configFile)) {
            Path configDestLocation = pluginHandle.configDir(environment);
            terminal.println(VERBOSE, "Found config, moving to %s", configDestLocation.toAbsolutePath());
            moveFilesWithoutOverwriting(configFile, configDestLocation, ".new");
            terminal.println(VERBOSE, "Installed %s into %s", name, configDestLocation.toAbsolutePath());
            potentialSitePlugin = false;
        }

        // try and identify the plugin type, see if it has no .class or .jar files in it
        // so its probably a _site, and it it does not have a _site in it, move everything to _site
        if (!Files.exists(extractLocation.resolve("_site"))) {
            if (potentialSitePlugin && !FileSystemUtils.hasExtensions(extractLocation, ".class", ".jar")) {
                terminal.println(VERBOSE, "Identified as a _site plugin, moving to _site structure ...");
                Path site = extractLocation.resolve("_site");
                Path tmpLocation = environment.pluginsFile().resolve(extractLocation.getFileName() + ".tmp");
                Files.move(extractLocation, tmpLocation);
                Files.createDirectories(extractLocation);
                Files.move(tmpLocation, site);
                terminal.println(VERBOSE, "Installed " + name + " into " + site.toAbsolutePath());
            }
        }
    }

    private Path unzipToTemporary(Path zip) throws IOException {
        Path tmp = Files.createTempDirectory(environment.tmpFile(), null);

        try (ZipInputStream zipInput = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            byte[] buffer = new byte[8192];
            while ((entry = zipInput.getNextEntry()) != null) {
                Path targetFile = tmp.resolve(entry.getName());

                // be on the safe side: do not rely on that directories are always extracted
                // before their children (although this makes sense, but is it guaranteed?)
                Files.createDirectories(targetFile.getParent());
                if (entry.isDirectory() == false) {
                    try (OutputStream out = Files.newOutputStream(targetFile)) {
                        int len;
                        while((len = zipInput.read(buffer)) >= 0) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
                zipInput.closeEntry();
            }
        }

        return tmp;
    }

    public void removePlugin(String name, Terminal terminal) throws IOException {
        if (name == null) {
            throw new IllegalArgumentException("plugin name must be supplied with remove [name].");
        }
        PluginHandle pluginHandle = PluginHandle.parse(name);
        boolean removed = false;

        checkForForbiddenName(pluginHandle.name);
        Path pluginToDelete = pluginHandle.extractedDir(environment);
        if (Files.exists(pluginToDelete)) {
            terminal.println(VERBOSE, "Removing: %s", pluginToDelete);
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
            terminal.println(VERBOSE, "Removing: %s", pluginToDelete);
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
            terminal.println(VERBOSE, "Removing: %s", binLocation);
            try {
                IOUtils.rm(binLocation);
            } catch (IOException ex){
                throw new IOException("Unable to remove " + pluginHandle.name + ". Check file permissions on " +
                        binLocation.toString(), ex);
            }
            removed = true;
        }

        if (removed) {
            terminal.println("Removed %s", name);
        } else {
            terminal.println("Plugin %s not found. Run plugin --list to get list of installed plugins.", name);
        }
    }

    private static void checkForForbiddenName(String name) {
        if (!hasLength(name) || BLACKLIST.contains(name.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Illegal plugin name: " + name);
        }
    }

    protected static void checkForOfficialPlugins(String name) {
        // We make sure that users can use only new short naming for official plugins only
        if (!OFFICIAL_PLUGINS.contains(name)) {
            throw new IllegalArgumentException(name +
                    " is not an official plugin so you should install it using elasticsearch/" +
                    name + "/latest naming form.");
        }
    }

    public Path[] getListInstalledPlugins() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(environment.pluginsFile())) {
            return Iterators.toArray(stream.iterator(), Path.class);
        }
    }

    public void listInstalledPlugins(Terminal terminal) throws IOException {
        Path[] plugins = getListInstalledPlugins();
        terminal.println("Installed plugins in %s:", environment.pluginsFile().toAbsolutePath());
        if (plugins == null || plugins.length == 0) {
            terminal.println("    - No plugin detected");
        } else {
            for (Path plugin : plugins) {
                terminal.println("    - " + plugin.getFileName());
            }
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
                // Elasticsearch new download service uses groupId org.elasticsearch.plugins from 2.0.0
                if (user == null) {
                    // TODO Update to https
                    addUrl(urls, String.format(Locale.ROOT, "http://download.elastic.co/org.elasticsearch.plugins/%1$s/%1$s-%2$s.zip", repo, version));
                } else {
                    // Elasticsearch old download service
                    // TODO Update to https
                    addUrl(urls, String.format(Locale.ROOT, "http://download.elastic.co/%1$s/%2$s/%2$s-%3$s.zip", user, repo, version));
                    // Maven central repository
                    addUrl(urls, String.format(Locale.ROOT, "http://search.maven.org/remotecontent?filepath=%1$s/%2$s/%3$s/%2$s-%3$s.zip", user.replace('.', '/'), repo, version));
                    // Sonatype repository
                    addUrl(urls, String.format(Locale.ROOT, "https://oss.sonatype.org/service/local/repositories/releases/content/%1$s/%2$s/%3$s/%2$s-%3$s.zip", user.replace('.', '/'), repo, version));
                    // Github repository
                    addUrl(urls, String.format(Locale.ROOT, "https://github.com/%1$s/%2$s/archive/%3$s.zip", user, repo, version));
                }
            }
            if (user != null) {
                // Github repository for master branch (assume site)
                addUrl(urls, String.format(Locale.ROOT, "https://github.com/%1$s/%2$s/archive/master.zip", user, repo));
            }
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

            String endname = repo;
            if (repo.startsWith("elasticsearch-")) {
                // remove elasticsearch- prefix
                endname = repo.substring("elasticsearch-".length());
            } else if (repo.startsWith("es-")) {
                // remove es- prefix
                endname = repo.substring("es-".length());
            }

            if (isOfficialPlugin(repo, user, version)) {
                return new PluginHandle(endname, Version.CURRENT.number(), null, repo);
            }

            return new PluginHandle(endname, version, user, repo);
        }

        static boolean isOfficialPlugin(String repo, String user, String version) {
            return version == null && user == null && !Strings.isNullOrEmpty(repo);
        }
    }

}
