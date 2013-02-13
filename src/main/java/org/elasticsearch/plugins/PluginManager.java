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
import java.net.URL;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 *
 */
public class PluginManager {

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

        File pluginFile = new File(environment.pluginsFile(), name + ".zip");

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

        // now, try as a path name...
        String filterZipName = null;
        if (!downloaded) {
            if (name.indexOf('/') != -1) {
                // github repo
                String[] elements = name.split("/");
                String userName = elements[0];
                String repoName = elements[1];
                String version = null;
                if (elements.length > 2) {
                    version = elements[2];
                }
                filterZipName = userName + "-" + repoName;
                // the installation file should not include the userName, just the repoName
                name = repoName;
                if (name.startsWith("elasticsearch-")) {
                    // remove elasticsearch- prefix
                    name = name.substring("elasticsearch-".length());
                } else if (name.startsWith("es-")) {
                    // remove es- prefix
                    name = name.substring("es-".length());
                }

                // update the plugin file name to reflect the extracted name
                pluginFile = new File(environment.pluginsFile(), name + ".zip");

                if (version != null) {
                    URL pluginUrl = new URL("http://download.elasticsearch.org/" + userName + "/" + repoName + "/" + repoName + "-" + version + ".zip");
                    System.out.println("Trying " + pluginUrl.toExternalForm() + "...");
                    try {
                        downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                        downloaded = true;
                    } catch (Exception e) {
                        if (verbose) {
                            System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e));
                        }
                    }
                    if (!downloaded) {
                        // try maven, see if its there... (both central and sonatype)
                        pluginUrl = new URL("http://search.maven.org/remotecontent?filepath=" + userName.replace('.', '/') + "/" + repoName + "/" + version + "/" + repoName + "-" + version + ".zip");
                        System.out.println("Trying " + pluginUrl.toExternalForm() + "...");
                        try {
                            downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                            downloaded = true;
                        } catch (Exception e) {
                            if (verbose) {
                                System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e));
                            }
                        }
                        if (!downloaded) {
                            pluginUrl = new URL("https://oss.sonatype.org/service/local/repositories/releases/content/" + userName.replace('.', '/') + "/" + repoName + "/" + version + "/" + repoName + "-" + version + ".zip");
                            System.out.println("Trying " + pluginUrl.toExternalForm() + "...");
                            try {
                                downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                                downloaded = true;
                            } catch (Exception e) {
                                if (verbose) {
                                    System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e));
                                }
                            }
                        }
                    }
                    if (!downloaded) {
                        // try it as a site plugin tagged
                        pluginUrl = new URL("https://github.com/" + userName + "/" + repoName + "/zipball/v" + version);
                        System.out.println("Trying " + pluginUrl.toExternalForm() + "... (assuming site plugin)");
                        try {
                            downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                            downloaded = true;
                        } catch (Exception e1) {
                            // ignore
                            if (verbose) {
                                System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e1));
                            }
                        }
                    }
                } else {
                    // assume site plugin, download master....
                    URL pluginUrl = new URL("https://github.com/" + userName + "/" + repoName + "/zipball/master");
                    System.out.println("Trying " + pluginUrl.toExternalForm() + "... (assuming site plugin)");
                    try {
                        downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                        downloaded = true;
                    } catch (Exception e2) {
                        // ignore
                        if (verbose) {
                            System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e2));
                        }
                    }
                }
            }
        }

        if (!downloaded) {
            throw new IOException("failed to download out of all possible locations..., use -verbose to get detailed information");
        }

        // extract the plugin
        File extractLocation = new File(environment.pluginsFile(), name);
        if (extractLocation.exists()) {
            throw new IOException("plugin directory already exists. To update the plugin, uninstall it first using -remove " + name + " command");
        }
        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(pluginFile);
            Enumeration<? extends ZipEntry> zipEntries = zipFile.entries();
            while (zipEntries.hasMoreElements()) {
                ZipEntry zipEntry = zipEntries.nextElement();
                if (zipEntry.isDirectory()) {
                    continue;
                }
                String zipName = zipEntry.getName().replace('\\', '/');
                if (filterZipName != null) {
                    if (zipName.startsWith(filterZipName)) {
                        zipName = zipName.substring(zipName.indexOf('/'));
                    }
                }
                File target = new File(extractLocation, zipName);
                FileSystemUtils.mkdirs(target.getParentFile());
                Streams.copy(zipFile.getInputStream(zipEntry), new FileOutputStream(target));
            }
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
            File toLocation = new File(new File(environment.homeFile(), "bin"), name);
            System.out.println("Found bin, moving to " + toLocation.getAbsolutePath());
            FileSystemUtils.deleteRecursively(toLocation);
            binFile.renameTo(toLocation);
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
            }
        }

        System.out.println("Installed " + name);
    }

    public void removePlugin(String name) throws IOException {
        File pluginToDelete = new File(environment.pluginsFile(), name);
        if (pluginToDelete.exists()) {
            FileSystemUtils.deleteRecursively(pluginToDelete, true);
        }
        pluginToDelete = new File(environment.pluginsFile(), name + ".zip");
        if (pluginToDelete.exists()) {
            pluginToDelete.delete();
        }
        File binLocation = new File(new File(environment.homeFile(), "bin"), name);
        if (binLocation.exists()) {
            FileSystemUtils.deleteRecursively(binLocation);
        }
    }

    public static void main(String[] args) {
        Tuple<Settings, Environment> initialSettings = InternalSettingsPerparer.prepareSettings(EMPTY_SETTINGS, true);

        if (!initialSettings.v2().pluginsFile().exists()) {
            FileSystemUtils.mkdirs(initialSettings.v2().pluginsFile());
        }

        String url = null;
        boolean verbose = false;
        for (int i = 0; i < args.length; i++) {
            if ("url".equals(args[i]) || "-url".equals(args[i])) {
                url = args[i + 1];
            } else if ("verbose".equals(args[i]) || "-verbose".equals(args[i])) {
                verbose = true;
            }
        }


        PluginManager pluginManager = new PluginManager(initialSettings.v2(), url);

        if (args.length < 1) {
            System.out.println("Usage:");
            System.out.println("    -url     [plugin location]   : Set exact URL to download the plugin from");
            System.out.println("    -install [plugin name]       : Downloads and installs listed plugins");
            System.out.println("    -remove  [plugin name]       : Removes listed plugins");
            System.out.println("    -verbose                     : Prints verbose messages");
        }
        for (int c = 0; c < args.length; c++) {
            String command = args[c];
            if (command.equals("install") || command.equals("-install")) {
                String pluginName = args[++c];
                System.out.println("-> Installing " + pluginName + "...");
                try {
                    pluginManager.downloadAndExtract(pluginName, verbose);
                } catch (IOException e) {
                    System.out.println("Failed to install " + pluginName + ", reason: " + e.getMessage());
                }
            } else if (command.equals("remove") || command.equals("-remove")) {
                String pluginName = args[++c];
                System.out.println("-> Removing " + pluginName + " ");
                try {
                    pluginManager.removePlugin(pluginName);
                } catch (IOException e) {
                    System.out.println("Failed to remove " + pluginName + ", reason: " + e.getMessage());
                }
            } else {
                // not install or remove, continue
                c++;
            }
        }
    }
}
