package org.elasticsearch.plugins;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
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
        File pluginFile = new File(url + "/" + name + "/elasticsearch-" + name + "-" + Version.CURRENT.number() + ".zip");
        boolean downloaded = false;
        String filterZipName = null;
        if (!pluginFile.exists()) {
            pluginFile = new File(url + "/elasticsearch-" + name + "-" + Version.CURRENT.number() + ".zip");
            if (!pluginFile.exists()) {
                pluginFile = new File(environment.pluginsFile(), name + ".zip");
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
                } else {
                    url = "http://elasticsearch.googlecode.com/svn/plugins";
                }
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
                        pluginFile = new File(environment.pluginsFile(), name + ".zip");
                        if (version == null) {
                            // try with ES version from downloads
                            URL pluginUrl = new URL("https://github.com/downloads/" + userName + "/" + repoName + "/" + repoName + "-" + Version.CURRENT.number() + ".zip");
                            System.out.println("Trying " + pluginUrl.toExternalForm() + "...");
                            try {
                                downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                                downloaded = true;
                            } catch (IOException e) {
                                // try a tag with ES version
                                if (verbose) {
                                    System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e));
                                }
                                pluginUrl = new URL("https://github.com/" + userName + "/" + repoName + "/zipball/v" + Version.CURRENT.number());
                                System.out.println("Trying " + pluginUrl.toExternalForm() + "...");
                                try {
                                    downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                                    downloaded = true;
                                } catch (IOException e1) {
                                    // download master
                                    if (verbose) {
                                        System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e1));
                                    }
                                    pluginUrl = new URL("https://github.com/" + userName + "/" + repoName + "/zipball/master");
                                    System.out.println("Trying " + pluginUrl.toExternalForm() + "...");
                                    try {
                                        downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                                        downloaded = true;
                                    } catch (IOException e2) {
                                        // ignore
                                        if (verbose) {
                                            System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e2));
                                        }
                                    }
                                }
                            }
                        } else {
                            // download explicit version
                            URL pluginUrl = new URL("https://github.com/downloads/" + userName + "/" + repoName + "/" + repoName + "-" + version + ".zip");
                            System.out.println("Trying " + pluginUrl.toExternalForm() + "...");
                            try {
                                downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                                downloaded = true;
                            } catch (IOException e) {
                                // try a tag with ES version
                                if (verbose) {
                                    System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e));
                                }
                                pluginUrl = new URL("https://github.com/" + userName + "/" + repoName + "/zipball/v" + version);
                                System.out.println("Trying " + pluginUrl.toExternalForm() + "...");
                                try {
                                    downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                                    downloaded = true;
                                } catch (IOException e1) {
                                    // ignore
                                    if (verbose) {
                                        System.out.println("Failed: " + ExceptionsHelper.detailedMessage(e1));
                                    }
                                }
                            }
                        }
                    } else {
                        URL pluginUrl = new URL(url + "/" + name + "/elasticsearch-" + name + "-" + Version.CURRENT.number() + ".zip");
                        System.out.println("Trying " + pluginUrl.toExternalForm() + "...");
                        try {
                            downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
                            downloaded = true;
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }
            } else {
                System.out.println("Using plugin from local fs: " + pluginFile.getAbsolutePath());
                downloaded = true;
            }
        } else {
            System.out.println("Using plugin from local fs: " + pluginFile.getAbsolutePath());
            downloaded = true;
        }

        if (!downloaded) {
            throw new IOException("failed to download");
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
            System.out.println("    -url     [plugins location]  : Set URL to download plugins from");
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
