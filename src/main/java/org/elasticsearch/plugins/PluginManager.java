/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.http.client.HttpDownloadHelper;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPerparer;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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

    private final Environment environment;

    private String url;

    public PluginManager(Environment environment, String url) {
        this.environment = environment;
        this.url = url;

        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }

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
            e.printStackTrace();
        }
    }

    public void downloadAndExtract(String name, boolean verbose) throws IOException {
        HttpDownloadHelper downloadHelper = new HttpDownloadHelper();

        if (!environment.pluginsFile().canWrite()) {
            System.out.println();
            throw new IOException("plugin directory " + environment.pluginsFile() + " is read only");
        }

        PluginHandle pluginHandle = PluginHandle.parse(name);
        File pluginFile = pluginHandle.distroFile(environment);
        // extract the plugin
        File extractLocation = pluginHandle.extractedDir(environment);
        if (extractLocation.exists()) {
            throw new IOException("plugin directory " + extractLocation.getAbsolutePath() + " already exists. To update the plugin, uninstall it first using -remove " + name + " command");
        }

        // first, try directly from the URL provided
        boolean downloaded = false;

        if (url != null) {
            URL pluginUrl = new URL(url);
            System.out.println("Trying " + pluginUrl.toExternalForm() + "...");
            try {
                downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                downloaded = true;
            } catch (IOException e) {
                // ignore
                if (verbose) {
                    System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e));
                }
            }
        }

        if (!downloaded) {
            // We try all possible locations
            for (URL url: pluginHandle.urls()) {
                System.out.println("Trying " + url.toExternalForm() + "...");
                try {
                    downloadHelper.download(url, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                    downloaded = true;
                    break;
                } catch (Exception e) {
                    if (verbose) {
                        System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e));
                    }
                }
            }
        }

        if (!downloaded) {
            throw new IOException("failed to download out of all possible locations..., use -verbose to get detailed information");
        }

        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(pluginFile);
            //we check whether we need to remove the top-level folder while extracting
            //sometimes (e.g. github) the downloaded archive contains a top-level folder which needs to be removed
            boolean removeTopLevelDir = topLevelDirInExcess(zipFile);
            Enumeration <? extends ZipEntry> zipEntries = zipFile.entries();
            while (zipEntries.hasMoreElements()) {
                ZipEntry zipEntry = zipEntries.nextElement();
                if (zipEntry.isDirectory()) {
                    continue;
                }
                String zipEntryName = zipEntry.getName().replace('\\', '/');
                if (removeTopLevelDir) {
                    zipEntryName = zipEntryName.substring(zipEntryName.indexOf('/'));
                }
                File target = new File(extractLocation, zipEntryName);
                FileSystemUtils.mkdirs(target.getParentFile());
                Streams.copy(zipFile.getInputStream(zipEntry), new FileOutputStream(target));
            }
            System.out.println("Installed " + name + " into " + extractLocation.getAbsolutePath());
        } catch (Exception e) {
            System.err.println("failed to extract plugin [" + pluginFile + "]: " + ExceptionsHelper.detailedMessage(e));
            return;
        } finally {
            if (zipFile != null) {
                try {
                    zipFile.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            pluginFile.delete();
        }

        if (FileSystemUtils.hasExtensions(extractLocation, ".java")) {
            System.out.println("Plugin installation assumed to be site plugin, but contains source code, aborting installation...");
            FileSystemUtils.deleteRecursively(extractLocation);
            return;
        }

        File binFile = new File(extractLocation, "bin");
        if (binFile.exists() && binFile.isDirectory()) {
            File toLocation = pluginHandle.binDir(environment);
            System.out.println("Found bin, moving to " + toLocation.getAbsolutePath());
            FileSystemUtils.deleteRecursively(toLocation);
            binFile.renameTo(toLocation);
            System.out.println("Installed " + name + " into " + toLocation.getAbsolutePath());
        }

        // try and identify the plugin type, see if it has no .class or .jar files in it
        // so its probably a _site, and it it does not have a _site in it, move everything to _site
        if (!new File(extractLocation, "_site").exists()) {
            if (!FileSystemUtils.hasExtensions(extractLocation, ".class", ".jar")) {
                System.out.println("Identified as a _site plugin, moving to _site structure ...");
                File site = new File(extractLocation, "_site");
                File tmpLocation = new File(environment.pluginsFile(), name + ".tmp");
                extractLocation.renameTo(tmpLocation);
                FileSystemUtils.mkdirs(extractLocation);
                tmpLocation.renameTo(site);
                System.out.println("Installed " + name + " into " + site.getAbsolutePath());
            }
        }
    }

    public void removePlugin(String name) throws IOException {
        PluginHandle pluginHandle = PluginHandle.parse(name);
        boolean removed = false;

        File pluginToDelete = pluginHandle.extractedDir(environment);
        if (pluginToDelete.exists()) {
            FileSystemUtils.deleteRecursively(pluginToDelete, true);
            removed = true;
        }
        pluginToDelete = pluginHandle.distroFile(environment);
        if (pluginToDelete.exists()) {
            pluginToDelete.delete();
            removed = true;
        }
        File binLocation = pluginHandle.binDir(environment);
        if (binLocation.exists()) {
            FileSystemUtils.deleteRecursively(binLocation);
            removed = true;
        }
        if (removed) {
            System.out.println("Removed " + name);
        } else {
            System.out.println("Plugin " + name + " not found. Run plugin --list to get list of installed plugins.");
    }
    }

    public File[] getListInstalledPlugins() {
        File[] plugins = environment.pluginsFile().listFiles();
        return plugins;
    }

    public void listInstalledPlugins() {
        File[] plugins = getListInstalledPlugins();
        System.out.println("Installed plugins:");
        if (plugins == null || plugins.length == 0) {
            System.out.println("    - No plugin detected in " + environment.pluginsFile().getAbsolutePath());
        } else {
            for (int i = 0; i < plugins.length; i++) {
                System.out.println("    - " + plugins[i].getName());
            }
        }
    }

    private boolean topLevelDirInExcess(ZipFile zipFile) {
        //We don't rely on ZipEntry#isDirectory because it might be that there is no explicit dir
        //but the files path do contain dirs, thus they are going to be extracted on sub-folders anyway
        Enumeration<? extends ZipEntry> zipEntries = zipFile.entries();
        Set<String> topLevelDirNames = new HashSet<String>();
        while (zipEntries.hasMoreElements()) {
            ZipEntry zipEntry = zipEntries.nextElement();
            String zipEntryName = zipEntry.getName().replace('\\', '/');

            int slash = zipEntryName.indexOf('/');
            //if there isn't a slash in the entry name it means that we have a file in the top-level
            if (slash == -1 ) {
                return false;
            }

            topLevelDirNames.add(zipEntryName.substring(0, slash));
            //if we have more than one top-level folder
            if (topLevelDirNames.size() > 1) {
                return false;
            }
        }
        return topLevelDirNames.size() == 1 && !"_site".equals(topLevelDirNames.iterator().next());
    }

    private static final int EXIT_CODE_OK = 0;
    private static final int EXIT_CODE_CMD_USAGE = 64;
    private static final int EXIT_CODE_IO_ERROR = 74;
    private static final int EXIT_CODE_ERROR = 70;

    public static void main(String[] args) {
        Tuple<Settings, Environment> initialSettings = InternalSettingsPerparer.prepareSettings(EMPTY_SETTINGS, true);

        if (!initialSettings.v2().pluginsFile().exists()) {
            FileSystemUtils.mkdirs(initialSettings.v2().pluginsFile());
        }

        String url = null;
        boolean verbose = false;
        String pluginName = null;
        int action = ACTION.NONE;

        if (args.length < 1) {
            displayHelp(null);
        }

        try {
            for (int c = 0; c < args.length; c++) {
                String command = args[c];
                if ("-u".equals(command) || "--url".equals(command)
                        // Deprecated commands
                        || "url".equals(command) || "-url".equals(command)) {
                    url = args[++c];
                } else if ("-v".equals(command) || "--v".equals(command)
                        || "verbose".equals(command) || "-verbose".equals(command)) {
                    verbose = true;
                } else if (command.equals("-i") || command.equals("--install")
                        // Deprecated commands
                        || command.equals("install") || command.equals("-install")) {
                    pluginName = args[++c];
                    action = ACTION.INSTALL;

                } else if (command.equals("-r") || command.equals("--remove")
                        // Deprecated commands
                        || command.equals("remove") || command.equals("-remove")) {
                    pluginName = args[++c];
                    action = ACTION.REMOVE;
                } else if (command.equals("-l") || command.equals("--list")) {
                    action = ACTION.LIST;
                } else if (command.equals("-h") || command.equals("--help")) {
                    displayHelp(null);
                } else {
                    displayHelp("Command [" + args[c] + "] unknown.");
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
            PluginManager pluginManager = new PluginManager(initialSettings.v2(), url);
            switch (action) {
                case ACTION.INSTALL:
                    try {
                        System.out.println("-> Installing " + pluginName + "...");
                        pluginManager.downloadAndExtract(pluginName, verbose);
                        exitCode = EXIT_CODE_OK;
                    } catch (IOException e) {
                        exitCode = EXIT_CODE_IO_ERROR;
                        System.out.println("Failed to install " + pluginName + ", reason: " + e.getMessage());
                    } catch (Throwable e) {
                        exitCode = EXIT_CODE_ERROR;
                        displayHelp("Error while installing plugin, reason: " + e.getClass().getSimpleName() +
                                ": " + e.getMessage());
                    }
                    break;
                case ACTION.REMOVE:
                    try {
                        System.out.println("-> Removing " + pluginName + " ");
                        pluginManager.removePlugin(pluginName);
                        exitCode = EXIT_CODE_OK;
                    } catch (IOException e) {
                        exitCode = EXIT_CODE_IO_ERROR;
                        System.out.println("Failed to remove " + pluginName + ", reason: " + e.getMessage());
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
                    System.out.println("Unknown Action [" + action + "]");
                    exitCode = EXIT_CODE_ERROR;

            }
            System.exit(exitCode); // exit here!
        }
    }

    private static void displayHelp(String message) {
        System.out.println("Usage:");
        System.out.println("    -u, --url     [plugin location]   : Set exact URL to download the plugin from");
        System.out.println("    -i, --install [plugin name]       : Downloads and installs listed plugins [*]");
        System.out.println("    -r, --remove  [plugin name]       : Removes listed plugins");
        System.out.println("    -l, --list                        : List installed plugins");
        System.out.println("    -v, --verbose                     : Prints verbose messages");
        System.out.println("    -h, --help                        : Prints this help message");
        System.out.println();
        System.out.println(" [*] Plugin name could be:");
        System.out.println("     elasticsearch/plugin/version for official elasticsearch plugins (download from download.elasticsearch.org)");
        System.out.println("     groupId/artifactId/version   for community plugins (download from maven central or oss sonatype)");
        System.out.println("     username/repository          for site plugins (download from github master)");

        if (message != null) {
            System.out.println();
            System.out.println("Message:");
            System.out.println("   " + message);
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
            List<URL> urls = new ArrayList<URL>();
            if (version != null) {
                // Elasticsearch download service
                addUrl(urls, "http://download.elasticsearch.org/" + user + "/" + repo + "/" + repo + "-" + version + ".zip");
                // Maven central repository
                addUrl(urls, "http://search.maven.org/remotecontent?filepath=" + user.replace('.', '/') + "/" + repo + "/" + version + "/" + repo + "-" + version + ".zip");
                // Sonatype repository
                addUrl(urls, "https://oss.sonatype.org/service/local/repositories/releases/content/" + user.replace('.', '/') + "/" + repo + "/" + version + "/" + repo + "-" + version + ".zip");
                // Github repository
                addUrl(urls, "https://github.com/" + user + "/" + repo + "/archive/v" + version + ".zip");
            }
            // Github repository for master branch (assume site)
            addUrl(urls, "https://github.com/" + user + "/" + repo + "/archive/master.zip");
            return urls;
        }

        private static void addUrl(List<URL> urls, String url) {
            try {
                URL _url = new URL(url);
                urls.add(new URL(url));
            } catch (MalformedURLException e) {
                // We simply ignore malformed URL
            }
        }

        File distroFile(Environment env) {
            return new File(env.pluginsFile(), name + ".zip");
        }

        File extractedDir(Environment env) {
            return new File(env.pluginsFile(), name);
        }

        File binDir(Environment env) {
            return new File(new File(env.homeFile(), "bin"), name);
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
