/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.env;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The environment of where things exists.
 */
@SuppressForbidden(reason = "configures paths for the system")
// TODO: move PathUtils to be package-private here instead of
// public+forbidden api!
public class Environment {

    private static final Path[] EMPTY_PATH_ARRAY = new Path[0];

    public static final Setting<String> PATH_HOME_SETTING = Setting.simpleString("path.home", Property.NodeScope);
    public static final Setting<List<String>> PATH_DATA_SETTING = Setting.listSetting(
        "path.data",
        Collections.emptyList(),
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<String> PATH_LOGS_SETTING = new Setting<>("path.logs", "", Function.identity(), Property.NodeScope);
    public static final Setting<List<String>> PATH_REPO_SETTING = Setting.listSetting(
        "path.repo",
        Collections.emptyList(),
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<String> PATH_SHARED_DATA_SETTING = Setting.simpleString("path.shared_data", Property.NodeScope);
    public static final Setting<String> NODE_PIDFILE_SETTING = Setting.simpleString("node.pidfile", Property.NodeScope);

    private final Settings settings;

    private final Path[] dataFiles;

    private final Path[] repoFiles;

    private final Path configFile;

    private final Path pluginsFile;

    private final Path modulesFile;

    private final Path sharedDataFile;

    /** location of bin/, used by plugin manager */
    private final Path binFile;

    /** location of lib/, */
    private final Path libFile;

    private final Path logsFile;

    /** Path to the PID file (can be null if no PID file is configured) **/
    private final Path pidFile;

    /** Path to the temporary file directory used by the JDK */
    private final Path tmpFile;

    public Environment(final Settings settings, final Path configPath) {
        this(settings, configPath, PathUtils.get(System.getProperty("java.io.tmpdir")));
    }

    // Should only be called directly by this class's unit tests
    Environment(final Settings settings, final Path configPath, final Path tmpPath) {
        final Path homeFile;
        if (PATH_HOME_SETTING.exists(settings)) {
            homeFile = PathUtils.get(PATH_HOME_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            throw new IllegalStateException(PATH_HOME_SETTING.getKey() + " is not configured");
        }

        if (configPath != null) {
            configFile = configPath.toAbsolutePath().normalize();
        } else {
            configFile = homeFile.resolve("config");
        }

        tmpFile = Objects.requireNonNull(tmpPath);

        pluginsFile = homeFile.resolve("plugins");

        List<String> dataPaths = PATH_DATA_SETTING.get(settings);
        if (dataPaths.isEmpty() == false) {
            dataFiles = new Path[dataPaths.size()];
            for (int i = 0; i < dataPaths.size(); i++) {
                dataFiles[i] = PathUtils.get(dataPaths.get(i)).toAbsolutePath().normalize();
            }
        } else {
            dataFiles = new Path[] { homeFile.resolve("data") };
        }
        if (PATH_SHARED_DATA_SETTING.exists(settings)) {
            sharedDataFile = PathUtils.get(PATH_SHARED_DATA_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            sharedDataFile = null;
        }
        List<String> repoPaths = PATH_REPO_SETTING.get(settings);
        if (repoPaths.isEmpty()) {
            repoFiles = EMPTY_PATH_ARRAY;
        } else {
            repoFiles = new Path[repoPaths.size()];
            for (int i = 0; i < repoPaths.size(); i++) {
                repoFiles[i] = PathUtils.get(repoPaths.get(i)).toAbsolutePath().normalize();
            }
        }

        // this is trappy, Setting#get(Settings) will get a fallback setting yet return false for Settings#exists(Settings)
        if (PATH_LOGS_SETTING.exists(settings)) {
            logsFile = PathUtils.get(PATH_LOGS_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            logsFile = homeFile.resolve("logs");
        }

        if (NODE_PIDFILE_SETTING.exists(settings)) {
            pidFile = PathUtils.get(NODE_PIDFILE_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            pidFile = null;
        }

        binFile = homeFile.resolve("bin");
        libFile = homeFile.resolve("lib");
        modulesFile = homeFile.resolve("modules");

        final Settings.Builder finalSettings = Settings.builder().put(settings);
        if (PATH_DATA_SETTING.exists(settings)) {
            if (dataPathUsesList(settings)) {
                finalSettings.putList(PATH_DATA_SETTING.getKey(), Arrays.stream(dataFiles).map(Path::toString).toList());
            } else {
                assert dataFiles.length == 1;
                finalSettings.put(PATH_DATA_SETTING.getKey(), dataFiles[0]);
            }
        }
        finalSettings.put(PATH_HOME_SETTING.getKey(), homeFile);
        finalSettings.put(PATH_LOGS_SETTING.getKey(), logsFile.toString());
        if (PATH_REPO_SETTING.exists(settings)) {
            finalSettings.putList(Environment.PATH_REPO_SETTING.getKey(), Arrays.stream(repoFiles).map(Path::toString).toList());
        }
        if (PATH_SHARED_DATA_SETTING.exists(settings)) {
            assert sharedDataFile != null;
            finalSettings.put(Environment.PATH_SHARED_DATA_SETTING.getKey(), sharedDataFile.toString());
        }
        if (NODE_PIDFILE_SETTING.exists(settings)) {
            assert pidFile != null;
            finalSettings.put(Environment.NODE_PIDFILE_SETTING.getKey(), pidFile.toString());
        }
        this.settings = finalSettings.build();
    }

    /**
     * The settings used to build this environment.
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * The data location.
     */
    public Path[] dataFiles() {
        return dataFiles;
    }

    /**
     * The shared data location
     */
    public Path sharedDataFile() {
        return sharedDataFile;
    }

    /**
     * The shared filesystem repo locations.
     */
    public Path[] repoFiles() {
        return repoFiles;
    }

    /**
     * Resolves the specified location against the list of configured repository roots
     *
     * If the specified location doesn't match any of the roots, returns null.
     */
    public Path resolveRepoFile(String location) {
        return PathUtils.get(repoFiles, location);
    }

    /**
     * Checks if the specified URL is pointing to the local file system and if it does, resolves the specified url
     * against the list of configured repository roots
     *
     * If the specified url doesn't match any of the roots, returns null.
     */
    public URL resolveRepoURL(URL url) {
        try {
            if ("file".equalsIgnoreCase(url.getProtocol())) {
                if (url.getHost() == null || "".equals(url.getHost())) {
                    // only local file urls are supported
                    Path path = PathUtils.get(repoFiles, url.toURI());
                    if (path == null) {
                        // Couldn't resolve against known repo locations
                        return null;
                    }
                    // Normalize URL
                    return path.toUri().toURL();
                }
                return null;
            } else if ("jar".equals(url.getProtocol())) {
                String file = url.getFile();
                int pos = file.indexOf("!/");
                if (pos < 0) {
                    return null;
                }
                String jarTail = file.substring(pos);
                String filePath = file.substring(0, pos);
                URL internalUrl = new URL(filePath);
                URL normalizedUrl = resolveRepoURL(internalUrl);
                if (normalizedUrl == null) {
                    return null;
                }
                return new URL("jar", "", normalizedUrl.toExternalForm() + jarTail);
            } else {
                // It's not file or jar url and it didn't match the white list - reject
                return null;
            }
        } catch (MalformedURLException ex) {
            // cannot make sense of this file url
            return null;
        } catch (URISyntaxException ex) {
            return null;
        }
    }

    // TODO: rename all these "file" methods to "dir"
    /**
     * The config directory.
     */
    public Path configFile() {
        return configFile;
    }

    public Path pluginsFile() {
        return pluginsFile;
    }

    public Path binFile() {
        return binFile;
    }

    public Path libFile() {
        return libFile;
    }

    public Path modulesFile() {
        return modulesFile;
    }

    public Path logsFile() {
        return logsFile;
    }

    /**
     * The PID file location (can be null if no PID file is configured)
     */
    public Path pidFile() {
        return pidFile;
    }

    /** Path to the default temp directory used by the JDK */
    public Path tmpFile() {
        return tmpFile;
    }

    /** Ensure the configured temp directory is a valid directory */
    public void validateTmpFile() throws IOException {
        validateTemporaryDirectory("Temporary directory", tmpFile);
    }

    /**
     * Ensure the temp directories needed for JNA are set up correctly.
     */
    public void validateNativesConfig() throws IOException {
        validateTmpFile();
        if (Constants.LINUX) {
            validateTemporaryDirectory(LIBFFI_TMPDIR_ENVIRONMENT_VARIABLE + " environment variable", getLibffiTemporaryDirectory());
        }
    }

    private static void validateTemporaryDirectory(String description, Path path) throws IOException {
        if (path == null) {
            throw new NullPointerException(description + " was not specified");
        }
        if (Files.exists(path) == false) {
            throw new FileNotFoundException(description + " [" + path + "] does not exist or is not accessible");
        }
        if (Files.isDirectory(path) == false) {
            throw new IOException(description + " [" + path + "] is not a directory");
        }
    }

    private static final String LIBFFI_TMPDIR_ENVIRONMENT_VARIABLE = "LIBFFI_TMPDIR";

    @SuppressForbidden(reason = "using PathUtils#get since libffi resolves paths without interference from the JVM")
    private static Path getLibffiTemporaryDirectory() {
        final String environmentVariable = System.getenv(LIBFFI_TMPDIR_ENVIRONMENT_VARIABLE);
        if (environmentVariable == null) {
            return null;
        }
        // Explicitly resolve into an absolute path since the working directory might be different from the one in which we were launched
        // and it would be confusing to report that the given relative path doesn't exist simply because it's being resolved relative to a
        // different location than the one the user expects.
        final String workingDirectory = System.getProperty("user.dir");
        if (workingDirectory == null) {
            assert false;
            return null;
        }
        return PathUtils.get(workingDirectory).resolve(environmentVariable);
    }

    /** Returns true if the data path is a list, false otherwise */
    public static boolean dataPathUsesList(Settings settings) {
        if (settings.hasValue(PATH_DATA_SETTING.getKey()) == false) {
            return false;
        }
        String rawDataPath = settings.get(PATH_DATA_SETTING.getKey());
        return rawDataPath.startsWith("[") || rawDataPath.contains(",");
    }

    public static FileStore getFileStore(final Path path) throws IOException {
        return new ESFileStore(Files.getFileStore(path));
    }

    public static long getUsableSpace(Path path) throws IOException {
        long freeSpaceInBytes = Environment.getFileStore(path).getUsableSpace();

        /* See: https://bugs.openjdk.java.net/browse/JDK-8162520 */
        if (freeSpaceInBytes < 0) {
            freeSpaceInBytes = Long.MAX_VALUE;
        }
        return freeSpaceInBytes;
    }

    /**
     * asserts that the two environments are equivalent for all things the environment cares about (i.e., all but the setting
     * object which may contain different setting)
     */
    public static void assertEquivalent(Environment actual, Environment expected) {
        assertEquals(actual.dataFiles(), expected.dataFiles(), "dataFiles");
        assertEquals(actual.repoFiles(), expected.repoFiles(), "repoFiles");
        assertEquals(actual.configFile(), expected.configFile(), "configFile");
        assertEquals(actual.pluginsFile(), expected.pluginsFile(), "pluginsFile");
        assertEquals(actual.binFile(), expected.binFile(), "binFile");
        assertEquals(actual.libFile(), expected.libFile(), "libFile");
        assertEquals(actual.modulesFile(), expected.modulesFile(), "modulesFile");
        assertEquals(actual.logsFile(), expected.logsFile(), "logsFile");
        assertEquals(actual.pidFile(), expected.pidFile(), "pidFile");
        assertEquals(actual.tmpFile(), expected.tmpFile(), "tmpFile");
    }

    private static void assertEquals(Object actual, Object expected, String name) {
        assert Objects.deepEquals(actual, expected) : "actual " + name + " [" + actual + "] is different than [ " + expected + "]";
    }
}
