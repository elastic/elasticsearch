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

package org.elasticsearch.env;

import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.elasticsearch.common.Strings.cleanPath;

/**
 * The environment of where things exists.
 */
@SuppressForbidden(reason = "configures paths for the system")
// TODO: move PathUtils to be package-private here instead of 
// public+forbidden api!
public class Environment {

    private final Settings settings;

    private final Path[] dataFiles;

    private final Path[] dataWithClusterFiles;

    private final Path[] repoFiles;

    private final Path configFile;

    private final Path scriptsFile;

    private final Path pluginsFile;

    private final Path sharedDataFile;

    /** location of bin/, used by plugin manager */
    private final Path binFile;

    /** location of lib/, */
    private final Path libFile;

    private final Path logsFile;

    /** Path to the PID file (can be null if no PID file is configured) **/
    private final Path pidFile;
    
    /** Path to the temporary file directory used by the JDK */
    private final Path tmpFile = PathUtils.get(System.getProperty("java.io.tmpdir"));

    /** List of filestores on the system */
    private static final FileStore[] fileStores;

    /**
     * We have to do this in clinit instead of init, because ES code is pretty messy,
     * and makes these environments, throws them away, makes them again, etc.
     */
    static {
        // gather information about filesystems
        ArrayList<FileStore> allStores = new ArrayList<>();
        for (FileStore store : PathUtils.getDefaultFileSystem().getFileStores()) {
            allStores.add(new ESFileStore(store));
        }
        fileStores = allStores.toArray(new ESFileStore[allStores.size()]);
    }

    public Environment(Settings settings) {
        this.settings = settings;
        final Path homeFile;
        if (settings.get("path.home") != null) {
            homeFile = PathUtils.get(cleanPath(settings.get("path.home")));
        } else {
            throw new IllegalStateException("path.home is not configured");
        }

        if (settings.get("path.conf") != null) {
            configFile = PathUtils.get(cleanPath(settings.get("path.conf")));
        } else {
            configFile = homeFile.resolve("config");
        }

        if (settings.get("path.scripts") != null) {
            scriptsFile = PathUtils.get(cleanPath(settings.get("path.scripts")));
        } else {
            scriptsFile = configFile.resolve("scripts");
        }

        if (settings.get("path.plugins") != null) {
            pluginsFile = PathUtils.get(cleanPath(settings.get("path.plugins")));
        } else {
            pluginsFile = homeFile.resolve("plugins");
        }

        String[] dataPaths = settings.getAsArray("path.data");
        if (dataPaths.length > 0) {
            dataFiles = new Path[dataPaths.length];
            dataWithClusterFiles = new Path[dataPaths.length];
            for (int i = 0; i < dataPaths.length; i++) {
                dataFiles[i] = PathUtils.get(dataPaths[i]);
                dataWithClusterFiles[i] = dataFiles[i].resolve(ClusterName.clusterNameFromSettings(settings).value());
            }
        } else {
            dataFiles = new Path[]{homeFile.resolve("data")};
            dataWithClusterFiles = new Path[]{homeFile.resolve("data").resolve(ClusterName.clusterNameFromSettings(settings).value())};
        }
        if (settings.get("path.shared_data") != null) {
            sharedDataFile = PathUtils.get(cleanPath(settings.get("path.shared_data")));
        } else {
            sharedDataFile = null;
        }
        String[] repoPaths = settings.getAsArray("path.repo");
        if (repoPaths.length > 0) {
            repoFiles = new Path[repoPaths.length];
            for (int i = 0; i < repoPaths.length; i++) {
                repoFiles[i] = PathUtils.get(repoPaths[i]);
            }
        } else {
            repoFiles = new Path[0];
        }
        if (settings.get("path.logs") != null) {
            logsFile = PathUtils.get(cleanPath(settings.get("path.logs")));
        } else {
            logsFile = homeFile.resolve("logs");
        }

        if (settings.get("pidfile") != null) {
            pidFile = PathUtils.get(cleanPath(settings.get("pidfile")));
        } else {
            pidFile = null;
        }

        binFile = homeFile.resolve("bin");
        libFile = homeFile.resolve("lib");
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
     * The data location with the cluster name as a sub directory.
     */
    public Path[] dataWithClusterFiles() {
        return dataWithClusterFiles;
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

    /**
     * The config location.
     */
    public Path configFile() {
        return configFile;
    }

    /**
     * Location of on-disk scripts
     */
    public Path scriptsFile() {
        return scriptsFile;
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

    /**
     * Looks up the filestore associated with a Path.
     * <p>
     * This is an enhanced version of {@link Files#getFileStore(Path)}:
     * <ul>
     *   <li>On *nix systems, the store returned for the root filesystem will contain
     *       the actual filesystem type (e.g. {@code ext4}) instead of {@code rootfs}.
     *   <li>On some systems, the custom attribute {@code lucene:spins} is supported
     *       via the {@link FileStore#getAttribute(String)} method.
     *   <li>Only requires the security permissions of {@link Files#getFileStore(Path)},
     *       no permissions to the actual mount point are required.
     *   <li>Exception handling has the same semantics as {@link Files#getFileStore(Path)}.
     *   <li>Works around https://bugs.openjdk.java.net/browse/JDK-8034057.
     * </ul>
     */
    public static FileStore getFileStore(Path path) throws IOException {
        return ESFileStore.getMatchingFileStore(path, fileStores);
    }
    
    /**
     * Returns true if the path is writable.
     * Acts just like {@link Files#isWritable(Path)}, except won't
     * falsely return false for paths on SUBST'd drive letters
     * See https://bugs.openjdk.java.net/browse/JDK-8034057
     * Note this will set the file modification time (to its already-set value)
     * to test access.
     */
    @SuppressForbidden(reason = "works around https://bugs.openjdk.java.net/browse/JDK-8034057")
    public static boolean isWritable(Path path) throws IOException {
        boolean v = Files.isWritable(path);
        if (v || Constants.WINDOWS == false) {
            return v;
        }

        // isWritable returned false on windows, the hack begins!!!!!!
        // resetting the modification time is the least destructive/simplest
        // way to check for both files and directories, and fails early just
        // in getting the current value if file doesn't exist, etc
        try {
            Files.setLastModifiedTime(path, Files.getLastModifiedTime(path));
            return true;
        } catch (Throwable e) {
            return false;
        }
    }
}
