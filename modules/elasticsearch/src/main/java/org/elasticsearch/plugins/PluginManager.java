package org.elasticsearch.plugins;

import org.elasticsearch.Version;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.http.client.HttpDownloadHelper;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPerparer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;

/**
 * @author kimchy (shay.banon)
 */
public class PluginManager {

    private final Environment environment;

    private final String url;

    public PluginManager(Environment environment, String url) {
        this.environment = environment;
        this.url = url;
    }

    public void downloadPlugin(String name) throws IOException {
        HttpDownloadHelper downloadHelper = new HttpDownloadHelper();

        File pluginFile = new File(url + "/" + name + "/elasticsearch-" + name + "-" + Version.number() + ".zip");
        if (!pluginFile.exists()) {
            pluginFile = new File(url + "/elasticsearch-" + name + "-" + Version.number() + ".zip");
            if (!pluginFile.exists()) {
                URL pluginUrl = new URL(url + "/" + name + "/elasticsearch-" + name + "-" + Version.number() + ".zip");
                System.out.println("Downloading plugin from " + pluginUrl.toExternalForm());
                pluginFile = new File(environment.pluginsFile(), name + ".zip");
                downloadHelper.download(pluginUrl, pluginFile, new HttpDownloadHelper.VerboseProgress(System.out));
            } else {
                System.out.println("Using plugin from local fs: " + pluginFile.getAbsolutePath());
            }
        } else {
            System.out.println("Using plugin from local fs: " + pluginFile.getAbsolutePath());
        }

        // extract the plugin
        File extractLocation = new File(environment.pluginsFile(), name);
        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(pluginFile);
            Enumeration<? extends ZipEntry> zipEntries = zipFile.entries();
            while (zipEntries.hasMoreElements()) {
                ZipEntry zipEntry = zipEntries.nextElement();
                if (!(zipEntry.getName().endsWith(".jar") || zipEntry.getName().endsWith(".zip"))) {
                    continue;
                }
                String zipName = zipEntry.getName().replace('\\', '/');
                File target = new File(extractLocation, zipName);
                target.getParentFile().mkdirs();
                Streams.copy(zipFile.getInputStream(zipEntry), new FileOutputStream(target));
            }
        } catch (Exception e) {
            System.err.println("failed to extract plugin [" + pluginFile + "]");
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
    }

    public static void main(String[] args) {
        Tuple<Settings, Environment> initialSettings = InternalSettingsPerparer.prepareSettings(EMPTY_SETTINGS, true);

        if (!initialSettings.v2().pluginsFile().exists()) {
            initialSettings.v2().pluginsFile().mkdirs();
        }

        String url = "http://elasticsearch.googlecode.com/svn/plugins";
        for (int i = 0; i < args.length; i++) {
            if ("url".equals(args[i]) || "-url".equals(args[i])) {
                url = args[i + 1];
                break;
            }
        }

        PluginManager pluginManager = new PluginManager(initialSettings.v2(), url);

        if (args.length < 1) {
            System.out.println("Usage:");
            System.out.println("    -url     [plugins location]  : Set URL to download plugins from");
            System.out.println("    -install [plugin name]       : Downloads and installs listed plugins");
            System.out.println("    -remove  [plugin name]       : Removes listed plugins");
        }
        for (int c = 0; c < args.length; c++) {
            String command = args[c];
            if (command.equals("install") || command.equals("-install")) {
                String pluginName = args[++c];
                System.out.print("-> Installing " + pluginName + " ");
                try {
                    pluginManager.downloadPlugin(pluginName);
                    System.out.println(" DONE");
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
