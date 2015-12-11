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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.*;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.http.client.HttpDownloadHelper;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginsService.Bundle;

import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.stream.StreamSupport;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.common.cli.Terminal.Verbosity.VERBOSE;
import static org.elasticsearch.common.io.FileSystemUtils.moveFilesWithoutOverwriting;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

/**
 *
 */
public class PluginManager {

    public static final String PROPERTY_SUPPORT_STAGING_URLS = "es.plugins.staging";

    public enum OutputMode {
        DEFAULT, SILENT, VERBOSE
    }

    private static final Set<String> BLACKLIST = unmodifiableSet(newHashSet(
            "elasticsearch",
            "elasticsearch.bat",
            "elasticsearch.in.sh",
            "plugin",
            "plugin.bat",
            "service.bat"));
    
    static final Set<String> MODULES = unmodifiableSet(newHashSet(
            "lang-expression",
            "lang-groovy"));

    static final Set<String> OFFICIAL_PLUGINS = unmodifiableSet(newHashSet(
            "analysis-icu",
            "analysis-kuromoji",
            "analysis-phonetic",
            "analysis-smartcn",
            "analysis-stempel",
            "delete-by-query",
            "discovery-azure",
            "discovery-ec2",
            "discovery-gce",
            "discovery-multicast",
            "lang-javascript",
            "lang-plan-a",
            "lang-python",
            "mapper-attachments",
            "mapper-murmur3",
            "mapper-size",
            "repository-azure",
            "repository-s3",
            "store-smb"));

    private final Environment environment;
    private URL url;
    private OutputMode outputMode;
    private TimeValue timeout;

    public PluginManager(Environment environment, URL url, OutputMode outputMode, TimeValue timeout) {
        this.environment = environment;
        this.url = url;
        this.outputMode = outputMode;
        this.timeout = timeout;
    }

    public void downloadAndExtract(String name, Terminal terminal, boolean batch) throws IOException {
        if (name == null && url == null) {
            throw new IllegalArgumentException("plugin name or url must be supplied with install.");
        }

        if (!Files.exists(environment.pluginsFile())) {
            terminal.println("Plugins directory [%s] does not exist. Creating...", environment.pluginsFile());
            Files.createDirectory(environment.pluginsFile());
        }

        if (!Environment.isWritable(environment.pluginsFile())) {
            throw new IOException("plugin directory " + environment.pluginsFile() + " is read only");
        }

        PluginHandle pluginHandle;
        if (name != null) {
            pluginHandle = PluginHandle.parse(name);
            checkForForbiddenName(pluginHandle.name);
        } else {
            // if we have no name but url, use temporary name that will be overwritten later
            pluginHandle = new PluginHandle("temp_name" + new Random().nextInt(), null, null);
        }

        Path pluginFile = download(pluginHandle, terminal);
        extract(pluginHandle, terminal, pluginFile, batch);
    }

    private Path download(PluginHandle pluginHandle, Terminal terminal) throws IOException {
        Path pluginFile = pluginHandle.newDistroFile(environment);

        HttpDownloadHelper downloadHelper = new HttpDownloadHelper();
        boolean downloaded = false;
        boolean verified = false;
        HttpDownloadHelper.DownloadProgress progress;
        if (outputMode == OutputMode.SILENT) {
            progress = new HttpDownloadHelper.NullProgress();
        } else {
            progress = new HttpDownloadHelper.VerboseProgress(terminal.writer());
        }

        // first, try directly from the URL provided
        if (url != null) {
            URL pluginUrl = url;
            boolean isSecureProcotol = "https".equalsIgnoreCase(pluginUrl.getProtocol());
            boolean isAuthInfoSet = !Strings.isNullOrEmpty(pluginUrl.getUserInfo());
            if (isAuthInfoSet && !isSecureProcotol) {
                throw new IOException("Basic auth is only supported for HTTPS!");
            }

            terminal.println("Trying %s ...", pluginUrl.toExternalForm());
            try {
                downloadHelper.download(pluginUrl, pluginFile, progress, this.timeout);
                downloaded = true;
                terminal.println("Verifying %s checksums if available ...", pluginUrl.toExternalForm());
                Tuple<URL, Path> sha1Info = pluginHandle.newChecksumUrlAndFile(environment, pluginUrl, "sha1");
                verified = downloadHelper.downloadAndVerifyChecksum(sha1Info.v1(), pluginFile,
                        sha1Info.v2(), progress, this.timeout, HttpDownloadHelper.SHA1_CHECKSUM);
                Tuple<URL, Path> md5Info = pluginHandle.newChecksumUrlAndFile(environment, pluginUrl, "md5");
                verified = verified || downloadHelper.downloadAndVerifyChecksum(md5Info.v1(), pluginFile,
                        md5Info.v2(), progress, this.timeout, HttpDownloadHelper.MD5_CHECKSUM);
            } catch (ElasticsearchTimeoutException | ElasticsearchCorruptionException e) {
                throw e;
            } catch (Exception e) {
                // ignore
                terminal.println("Failed: %s", ExceptionsHelper.detailedMessage(e));
            }
        } else {
            if (PluginHandle.isOfficialPlugin(pluginHandle.name, pluginHandle.user, pluginHandle.version)) {
                checkForOfficialPlugins(pluginHandle.name);
            }
        }

        if (!downloaded && url == null) {
            // We try all possible locations
            for (URL url : pluginHandle.urls()) {
                terminal.println("Trying %s ...", url.toExternalForm());
                try {
                    downloadHelper.download(url, pluginFile, progress, this.timeout);
                    downloaded = true;
                    terminal.println("Verifying %s checksums if available ...", url.toExternalForm());
                    Tuple<URL, Path> sha1Info = pluginHandle.newChecksumUrlAndFile(environment, url, "sha1");
                    verified = downloadHelper.downloadAndVerifyChecksum(sha1Info.v1(), pluginFile,
                            sha1Info.v2(), progress, this.timeout, HttpDownloadHelper.SHA1_CHECKSUM);
                    Tuple<URL, Path> md5Info = pluginHandle.newChecksumUrlAndFile(environment, url, "md5");
                    verified = verified || downloadHelper.downloadAndVerifyChecksum(md5Info.v1(), pluginFile,
                            md5Info.v2(), progress, this.timeout, HttpDownloadHelper.MD5_CHECKSUM);
                    break;
                } catch (ElasticsearchTimeoutException | ElasticsearchCorruptionException e) {
                    throw e;
                } catch (Exception e) {
                    terminal.println(VERBOSE, "Failed: %s", ExceptionsHelper.detailedMessage(e));
                }
            }
        }

        if (!downloaded) {
            // try to cleanup what we downloaded
            IOUtils.deleteFilesIgnoringExceptions(pluginFile);
            throw new IOException("failed to download out of all possible locations..., use --verbose to get detailed information");
        }

        if (verified == false) {
            terminal.println("NOTE: Unable to verify checksum for downloaded plugin (unable to find .sha1 or .md5 file to verify)");
        }
        return pluginFile;
    }

    private void extract(PluginHandle pluginHandle, Terminal terminal, Path pluginFile, boolean batch) throws IOException {
        // unzip plugin to a staging temp dir, named for the plugin
        Path tmp = Files.createTempDirectory(environment.tmpFile(), null);
        Path root = tmp.resolve(pluginHandle.name);
        unzipPlugin(pluginFile, root);

        // find the actual root (in case its unzipped with extra directory wrapping)
        root = findPluginRoot(root);

        // read and validate the plugin descriptor
        PluginInfo info = PluginInfo.readFromProperties(root);
        terminal.println(VERBOSE, "%s", info);

        // don't let luser install plugin as a module... 
        // they might be unavoidably in maven central and are packaged up the same way)
        if (MODULES.contains(info.getName())) {
            throw new IOException("plugin '" + info.getName() + "' cannot be installed like this, it is a system module");
        }

        // update name in handle based on 'name' property found in descriptor file
        pluginHandle = new PluginHandle(info.getName(), pluginHandle.version, pluginHandle.user);
        final Path extractLocation = pluginHandle.extractedDir(environment);
        if (Files.exists(extractLocation)) {
            throw new IOException("plugin directory " + extractLocation.toAbsolutePath() + " already exists. To update the plugin, uninstall it first using 'remove " + pluginHandle.name + "' command");
        }

        // check for jar hell before any copying
        if (info.isJvm()) {
            jarHellCheck(root, info.isIsolated());
        }

        // read optional security policy (extra permissions)
        // if it exists, confirm or warn the user
        Path policy = root.resolve(PluginInfo.ES_PLUGIN_POLICY);
        if (Files.exists(policy)) {
            PluginSecurity.readPolicy(policy, terminal, environment, batch);
        }

        // install plugin
        FileSystemUtils.copyDirectoryRecursively(root, extractLocation);
        terminal.println("Installed %s into %s", pluginHandle.name, extractLocation.toAbsolutePath());

        // cleanup
        tryToDeletePath(terminal, tmp, pluginFile);

        // take care of bin/ by moving and applying permissions if needed
        Path sourcePluginBinDirectory = extractLocation.resolve("bin");
        Path destPluginBinDirectory = pluginHandle.binDir(environment);
        boolean needToCopyBinDirectory = Files.exists(sourcePluginBinDirectory);
        if (needToCopyBinDirectory) {
            if (Files.exists(destPluginBinDirectory) && !Files.isDirectory(destPluginBinDirectory)) {
                tryToDeletePath(terminal, extractLocation);
                throw new IOException("plugin bin directory " + destPluginBinDirectory + " is not a directory");
            }

            try {
                copyBinDirectory(sourcePluginBinDirectory, destPluginBinDirectory, pluginHandle.name, terminal);
            } catch (IOException e) {
                // rollback and remove potentially before installed leftovers
                terminal.printError("Error copying bin directory [%s] to [%s], cleaning up, reason: %s", sourcePluginBinDirectory, destPluginBinDirectory, ExceptionsHelper.detailedMessage(e));
                tryToDeletePath(terminal, extractLocation, pluginHandle.binDir(environment));
                throw e;
            }

        }

        Path sourceConfigDirectory = extractLocation.resolve("config");
        Path destConfigDirectory = pluginHandle.configDir(environment);
        boolean needToCopyConfigDirectory = Files.exists(sourceConfigDirectory);
        if (needToCopyConfigDirectory) {
            if (Files.exists(destConfigDirectory) && !Files.isDirectory(destConfigDirectory)) {
                tryToDeletePath(terminal, extractLocation, destPluginBinDirectory);
                throw new IOException("plugin config directory " + destConfigDirectory + " is not a directory");
            }

            try {
                terminal.println(VERBOSE, "Found config, moving to %s", destConfigDirectory.toAbsolutePath());
                moveFilesWithoutOverwriting(sourceConfigDirectory, destConfigDirectory, ".new");

                if (Environment.getFileStore(destConfigDirectory).supportsFileAttributeView(PosixFileAttributeView.class)) {
                    //We copy owner, group and permissions from the parent ES_CONFIG directory, assuming they were properly set depending
                    // on how es was installed in the first place: can be root:elasticsearch (750) if es was installed from rpm/deb packages
                    // or most likely elasticsearch:elasticsearch if installed from tar/zip. As for permissions we don't rely on umask.
                    PosixFileAttributes parentDirAttributes = Files.getFileAttributeView(destConfigDirectory.getParent(), PosixFileAttributeView.class).readAttributes();
                    //for files though, we make sure not to copy execute permissions from the parent dir and leave them untouched
                    Set<PosixFilePermission> baseFilePermissions = new HashSet<>();
                    for (PosixFilePermission posixFilePermission : parentDirAttributes.permissions()) {
                        switch (posixFilePermission) {
                            case OWNER_EXECUTE:
                            case GROUP_EXECUTE:
                            case OTHERS_EXECUTE:
                                break;
                            default:
                                baseFilePermissions.add(posixFilePermission);
                        }
                    }
                    Files.walkFileTree(destConfigDirectory, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            if (attrs.isRegularFile()) {
                                Set<PosixFilePermission> newFilePermissions = new HashSet<>(baseFilePermissions);
                                Set<PosixFilePermission> currentFilePermissions = Files.getPosixFilePermissions(file);
                                for (PosixFilePermission posixFilePermission : currentFilePermissions) {
                                    switch (posixFilePermission) {
                                        case OWNER_EXECUTE:
                                        case GROUP_EXECUTE:
                                        case OTHERS_EXECUTE:
                                            newFilePermissions.add(posixFilePermission);
                                    }
                                }
                                setPosixFileAttributes(file, parentDirAttributes.owner(), parentDirAttributes.group(), newFilePermissions);
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                            setPosixFileAttributes(dir, parentDirAttributes.owner(), parentDirAttributes.group(), parentDirAttributes.permissions());
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } else {
                    terminal.println(VERBOSE, "Skipping posix permissions - filestore doesn't support posix permission");
                }

                terminal.println(VERBOSE, "Installed %s into %s", pluginHandle.name, destConfigDirectory.toAbsolutePath());
            } catch (IOException e) {
                terminal.printError("Error copying config directory [%s] to [%s], cleaning up, reason: %s", sourceConfigDirectory, destConfigDirectory, ExceptionsHelper.detailedMessage(e));
                tryToDeletePath(terminal, extractLocation, destPluginBinDirectory, destConfigDirectory);
                throw e;
            }
        }
    }

    private static void setPosixFileAttributes(Path path, UserPrincipal owner, GroupPrincipal group, Set<PosixFilePermission> permissions) throws IOException {
        PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        fileAttributeView.setOwner(owner);
        fileAttributeView.setGroup(group);
        fileAttributeView.setPermissions(permissions);
    }

    static void tryToDeletePath(Terminal terminal, Path ... paths) {
        for (Path path : paths) {
            try {
                IOUtils.rm(path);
            } catch (IOException e) {
                terminal.printError(e);
            }
        }
    }

    private void copyBinDirectory(Path sourcePluginBinDirectory, Path destPluginBinDirectory, String pluginName, Terminal terminal) throws IOException {
        boolean canCopyFromSource = Files.exists(sourcePluginBinDirectory) && Files.isReadable(sourcePluginBinDirectory) && Files.isDirectory(sourcePluginBinDirectory);
        if (canCopyFromSource) {
            terminal.println(VERBOSE, "Found bin, moving to %s", destPluginBinDirectory.toAbsolutePath());
            if (Files.exists(destPluginBinDirectory)) {
                IOUtils.rm(destPluginBinDirectory);
            }
            try {
                Files.createDirectories(destPluginBinDirectory.getParent());
                FileSystemUtils.move(sourcePluginBinDirectory, destPluginBinDirectory);
            } catch (IOException e) {
                throw new IOException("Could not move [" + sourcePluginBinDirectory + "] to [" + destPluginBinDirectory + "]", e);
            }
            if (Environment.getFileStore(destPluginBinDirectory).supportsFileAttributeView(PosixFileAttributeView.class)) {
                PosixFileAttributes parentDirAttributes = Files.getFileAttributeView(destPluginBinDirectory.getParent(), PosixFileAttributeView.class).readAttributes();
                //copy permissions from parent bin directory
                Set<PosixFilePermission> filePermissions = new HashSet<>();
                for (PosixFilePermission posixFilePermission : parentDirAttributes.permissions()) {
                    switch (posixFilePermission) {
                        case OWNER_EXECUTE:
                        case GROUP_EXECUTE:
                        case OTHERS_EXECUTE:
                            break;
                        default:
                            filePermissions.add(posixFilePermission);
                    }
                }
                // add file execute permissions to existing perms, so execution will work.
                filePermissions.add(PosixFilePermission.OWNER_EXECUTE);
                filePermissions.add(PosixFilePermission.GROUP_EXECUTE);
                filePermissions.add(PosixFilePermission.OTHERS_EXECUTE);
                Files.walkFileTree(destPluginBinDirectory, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (attrs.isRegularFile()) {
                            setPosixFileAttributes(file, parentDirAttributes.owner(), parentDirAttributes.group(), filePermissions);
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        setPosixFileAttributes(dir, parentDirAttributes.owner(), parentDirAttributes.group(), parentDirAttributes.permissions());
                        return FileVisitResult.CONTINUE;
                    }
                });
            } else {
                terminal.println(VERBOSE, "Skipping posix permissions - filestore doesn't support posix permission");
            }
            terminal.println(VERBOSE, "Installed %s into %s", pluginName, destPluginBinDirectory.toAbsolutePath());
        }
    }

    /** we check whether we need to remove the top-level folder while extracting
     *  sometimes (e.g. github) the downloaded archive contains a top-level folder which needs to be removed
     */
    private Path findPluginRoot(Path dir) throws IOException {
        if (Files.exists(dir.resolve(PluginInfo.ES_PLUGIN_PROPERTIES))) {
            return dir;
        } else {
            final Path[] topLevelFiles = FileSystemUtils.files(dir);
            if (topLevelFiles.length == 1 && Files.isDirectory(topLevelFiles[0])) {
                Path subdir = topLevelFiles[0];
                if (Files.exists(subdir.resolve(PluginInfo.ES_PLUGIN_PROPERTIES))) {
                    return subdir;
                }
            }
        }
        throw new RuntimeException("Could not find plugin descriptor '" + PluginInfo.ES_PLUGIN_PROPERTIES + "' in plugin zip");
    }

    /** check a candidate plugin for jar hell before installing it */
    private void jarHellCheck(Path candidate, boolean isolated) throws IOException {
        // create list of current jars in classpath
        final List<URL> jars = new ArrayList<>();
        jars.addAll(Arrays.asList(JarHell.parseClassPath()));

        // read existing bundles. this does some checks on the installation too.
        List<Bundle> bundles = PluginsService.getPluginBundles(environment.pluginsFile());

        // if we aren't isolated, we need to jarhellcheck against any other non-isolated plugins
        // thats always the first bundle
        if (isolated == false) {
            jars.addAll(bundles.get(0).urls);
        }

        // add plugin jars to the list
        Path pluginJars[] = FileSystemUtils.files(candidate, "*.jar");
        for (Path jar : pluginJars) {
            jars.add(jar.toUri().toURL());
        }

        // check combined (current classpath + new jars to-be-added)
        try {
            JarHell.checkJarHell(jars.toArray(new URL[jars.size()]));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void unzipPlugin(Path zip, Path target) throws IOException {
        Files.createDirectories(target);

        try (ZipInputStream zipInput = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            byte[] buffer = new byte[8192];
            while ((entry = zipInput.getNextEntry()) != null) {
                Path targetFile = target.resolve(entry.getName());

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
            terminal.println("Plugin %s not found. Run \"plugin list\" to get list of installed plugins.", name);
        }
    }

    static void checkForForbiddenName(String name) {
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
        if (!Files.exists(environment.pluginsFile())) {
            return new Path[0];
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(environment.pluginsFile())) {
            return StreamSupport.stream(stream.spliterator(), false).toArray(length -> new Path[length]);
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

        final String version;
        final String user;
        final String name;

        PluginHandle(String name, String version, String user) {
            this.version = version;
            this.user = user;
            this.name = name;
        }

        List<URL> urls() {
            List<URL> urls = new ArrayList<>();
            if (version != null) {
                // Elasticsearch new download service uses groupId org.elasticsearch.plugin from 2.0.0
                if (user == null) {
                    if (!Strings.isNullOrEmpty(System.getProperty(PROPERTY_SUPPORT_STAGING_URLS))) {
                        addUrl(urls, String.format(Locale.ROOT, "https://download.elastic.co/elasticsearch/staging/%s-%s/org/elasticsearch/plugin/%s/%s/%s-%s.zip", version, Build.CURRENT.shortHash(), name, version, name, version));
                    }
                    addUrl(urls, String.format(Locale.ROOT, "https://download.elastic.co/elasticsearch/release/org/elasticsearch/plugin/%s/%s/%s-%s.zip", name, version, name, version));
                } else {
                    // Elasticsearch old download service
                    addUrl(urls, String.format(Locale.ROOT, "https://download.elastic.co/%1$s/%2$s/%2$s-%3$s.zip", user, name, version));
                    // Maven central repository
                    addUrl(urls, String.format(Locale.ROOT, "https://search.maven.org/remotecontent?filepath=%1$s/%2$s/%3$s/%2$s-%3$s.zip", user.replace('.', '/'), name, version));
                    // Sonatype repository
                    addUrl(urls, String.format(Locale.ROOT, "https://oss.sonatype.org/service/local/repositories/releases/content/%1$s/%2$s/%3$s/%2$s-%3$s.zip", user.replace('.', '/'), name, version));
                    // Github repository
                    addUrl(urls, String.format(Locale.ROOT, "https://github.com/%1$s/%2$s/archive/%3$s.zip", user, name, version));
                }
            }
            if (user != null) {
                // Github repository for master branch (assume site)
                addUrl(urls, String.format(Locale.ROOT, "https://github.com/%1$s/%2$s/archive/master.zip", user, name));
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

        Path newDistroFile(Environment env) throws IOException {
            return Files.createTempFile(env.tmpFile(), name, ".zip");
        }

        Tuple<URL, Path> newChecksumUrlAndFile(Environment env, URL originalUrl, String suffix) throws IOException {
            URL newUrl = new URL(originalUrl.toString() + "." + suffix);
            return new Tuple<>(newUrl, Files.createTempFile(env.tmpFile(), name, ".zip." + suffix));
        }

        Path extractedDir(Environment env) {
            return env.pluginsFile().resolve(name);
        }

        Path binDir(Environment env) {
            return env.binFile().resolve(name);
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

            if (isOfficialPlugin(repo, user, version)) {
                return new PluginHandle(repo, Version.CURRENT.number(), null);
            }

            return new PluginHandle(repo, version, user);
        }

        static boolean isOfficialPlugin(String repo, String user, String version) {
            return version == null && user == null && !Strings.isNullOrEmpty(repo);
        }
    }

}
