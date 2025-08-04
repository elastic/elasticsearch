/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.List;
import java.util.Objects;

/**
 * The environment of where things exists.
 */
@SuppressForbidden(reason = "configures paths for the system")
// TODO: move PathUtils to be package-private here instead of
// public+forbidden api!
public class Environment {

    private static final Path[] EMPTY_PATH_ARRAY = new Path[0];

    public static final Setting<String> PATH_HOME_SETTING = Setting.simpleString("path.home", Property.NodeScope);
    public static final Setting<List<String>> PATH_DATA_SETTING = Setting.stringListSetting("path.data", Property.NodeScope);
    public static final Setting<String> PATH_LOGS_SETTING = Setting.simpleString("path.logs", Property.NodeScope);
    public static final Setting<List<String>> PATH_REPO_SETTING = Setting.stringListSetting("path.repo", Property.NodeScope);
    public static final Setting<String> PATH_SHARED_DATA_SETTING = Setting.simpleString("path.shared_data", Property.NodeScope);

    private final Settings settings;

    private final Path[] dataDirs;

    private final Path[] repoDirs;

    private final Path configDir;

    private final Path pluginsDir;

    private final Path modulesDir;

    private final Path sharedDataDir;

    /** location of bin/, used by plugin manager */
    private final Path binDir;

    /** location of lib/, */
    private final Path libDir;

    private final Path logsDir;

    /** Path to the temporary file directory used by the JDK */
    private final Path tmpDir;

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
            configDir = configPath.toAbsolutePath().normalize();
        } else {
            configDir = homeFile.resolve("config");
        }

        tmpDir = Objects.requireNonNull(tmpPath);

        pluginsDir = homeFile.resolve("plugins");

        List<String> dataPaths = PATH_DATA_SETTING.get(settings);
        if (dataPaths.isEmpty() == false) {
            dataDirs = new Path[dataPaths.size()];
            for (int i = 0; i < dataPaths.size(); i++) {
                dataDirs[i] = PathUtils.get(dataPaths.get(i)).toAbsolutePath().normalize();
            }
        } else {
            dataDirs = new Path[] { homeFile.resolve("data") };
        }
        if (PATH_SHARED_DATA_SETTING.exists(settings)) {
            sharedDataDir = PathUtils.get(PATH_SHARED_DATA_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            sharedDataDir = null;
        }
        List<String> repoPaths = PATH_REPO_SETTING.get(settings);
        if (repoPaths.isEmpty()) {
            repoDirs = EMPTY_PATH_ARRAY;
        } else {
            repoDirs = new Path[repoPaths.size()];
            for (int i = 0; i < repoPaths.size(); i++) {
                repoDirs[i] = PathUtils.get(repoPaths.get(i)).toAbsolutePath().normalize();
            }
        }

        // this is trappy, Setting#get(Settings) will get a fallback setting yet return false for Settings#exists(Settings)
        if (PATH_LOGS_SETTING.exists(settings)) {
            logsDir = PathUtils.get(PATH_LOGS_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            logsDir = homeFile.resolve("logs");
        }

        binDir = homeFile.resolve("bin");
        libDir = homeFile.resolve("lib");
        modulesDir = homeFile.resolve("modules");

        final Settings.Builder finalSettings = Settings.builder().put(settings);
        if (PATH_DATA_SETTING.exists(settings)) {
            if (dataPathUsesList(settings)) {
                finalSettings.putList(PATH_DATA_SETTING.getKey(), Arrays.stream(dataDirs).map(Path::toString).toList());
            } else {
                assert dataDirs.length == 1;
                finalSettings.put(PATH_DATA_SETTING.getKey(), dataDirs[0]);
            }
        }
        finalSettings.put(PATH_HOME_SETTING.getKey(), homeFile);
        finalSettings.put(PATH_LOGS_SETTING.getKey(), logsDir.toString());
        if (PATH_REPO_SETTING.exists(settings)) {
            finalSettings.putList(Environment.PATH_REPO_SETTING.getKey(), Arrays.stream(repoDirs).map(Path::toString).toList());
        }
        if (PATH_SHARED_DATA_SETTING.exists(settings)) {
            assert sharedDataDir != null;
            finalSettings.put(Environment.PATH_SHARED_DATA_SETTING.getKey(), sharedDataDir.toString());
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
    public Path[] dataDirs() {
        return dataDirs;
    }

    /**
     * The shared data location
     */
    public Path sharedDataDir() {
        return sharedDataDir;
    }

    /**
     * The shared filesystem repo locations.
     */
    public Path[] repoDirs() {
        return repoDirs;
    }

    /**
     * Resolves the specified location against the list of configured repository roots
     *
     * If the specified location doesn't match any of the roots, returns null.
     */
    public Path resolveRepoDir(String location) {
        return PathUtils.get(repoDirs, location);
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
                    Path path = PathUtils.get(repoDirs, url.toURI());
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

    /**
     * The config directory.
     */
    public Path configDir() {
        return configDir;
    }

    public Path pluginsDir() {
        return pluginsDir;
    }

    public Path binDir() {
        return binDir;
    }

    public Path libDir() {
        return libDir;
    }

    public Path modulesDir() {
        return modulesDir;
    }

    public Path logsDir() {
        return logsDir;
    }

    /** Path to the default temp directory used by the JDK */
    public Path tmpDir() {
        return tmpDir;
    }

    /** Ensure the configured temp directory is a valid directory */
    public void validateTmpDir() throws IOException {
        validateTemporaryDirectory("Temporary directory", tmpDir);
    }

    /**
     * Ensure the temp directories needed for JNA are set up correctly.
     */
    public void validateNativesConfig() throws IOException {
        validateTmpDir();
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
        assert freeSpaceInBytes >= 0;
        return freeSpaceInBytes;
    }

    /**
     * asserts that the two environments are equivalent for all things the environment cares about (i.e., all but the setting
     * object which may contain different setting)
     */
    public static void assertEquivalent(Environment actual, Environment expected) {
        assertEquals(actual.dataDirs(), expected.dataDirs(), "dataDirs");
        assertEquals(actual.repoDirs(), expected.repoDirs(), "sharedRepoDirs");
        assertEquals(actual.configDir(), expected.configDir(), "configDir");
        assertEquals(actual.pluginsDir(), expected.pluginsDir(), "pluginsDir");
        assertEquals(actual.binDir(), expected.binDir(), "binDir");
        assertEquals(actual.libDir(), expected.libDir(), "libDir");
        assertEquals(actual.modulesDir(), expected.modulesDir(), "modulesDir");
        assertEquals(actual.logsDir(), expected.logsDir(), "logsDir");
        assertEquals(actual.tmpDir(), expected.tmpDir(), "tmpDir");
    }

    private static void assertEquals(Object actual, Object expected, String name) {
        assert Objects.deepEquals(actual, expected) : "actual " + name + " [" + actual + "] is different than [ " + expected + "]";
    }
}
