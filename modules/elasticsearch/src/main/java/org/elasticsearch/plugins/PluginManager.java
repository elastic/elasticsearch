package org.elasticsearch.plugins;

import org.elasticsearch.Version;
import org.elasticsearch.common.http.client.HttpDownloadHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPerparer;
import org.elasticsearch.util.Tuple;
import org.elasticsearch.util.io.FileSystemUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

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

        URL pluginUrl = new URL(url + "/" + name + "/elasticsearch-" + name + "-" + Version.number() + ".zip");
        downloadHelper.download(pluginUrl, new File(environment.pluginsFile(), name + ".zip"), new HttpDownloadHelper.VerboseProgress(System.out));
    }

    public void removePlugin(String name) throws IOException {
        File pluginToDelete = new File(environment.pluginsFile(), name + ".zip");
        if (!pluginToDelete.exists()) {
            throw new FileNotFoundException("Plugin [" + name + "] does not exists");
        }
        pluginToDelete.delete();
        FileSystemUtils.deleteRecursively(new File(new File(environment.workFile(), "plugins"), name), true);
    }

    public static void main(String[] args) {
        Tuple<Settings, Environment> initialSettings = InternalSettingsPerparer.prepareSettings(EMPTY_SETTINGS, true);

        if (!initialSettings.v2().pluginsFile().exists()) {
            initialSettings.v2().pluginsFile().mkdirs();
        }

        PluginManager pluginManager = new PluginManager(initialSettings.v2(), "http://elasticsearch.googlecode.com/svn/plugins");

        if (args.length < 1) {
            System.out.println("Usage:");
            System.out.println("    - install [list of plugin names]: Downloads and installs listed plugins");
            System.out.println("    - remove [list of plugin names]: Removes listed plugins");
        }
        String command = args[0];
        if (command.equals("install") || command.equals("-install")) {
            if (args.length < 2) {
                System.out.println("'install' requires an additional parameter with the plugin name");
            }
            for (int i = 1; i < args.length; i++) {
                String pluginName = args[i];
                System.out.print("-> Installing " + pluginName + " ");
                try {
                    pluginManager.downloadPlugin(pluginName);
                    System.out.println(" DONE");
                } catch (IOException e) {
                    System.out.println("Failed to install " + pluginName + ", reason: " + e.getMessage());
                }
            }
        } else if (command.equals("remove") || command.equals("-remove")) {
            if (args.length < 2) {
                System.out.println("'remove' requires an additional parameter with the plugin name");
            }
            for (int i = 1; i < args.length; i++) {
                String pluginName = args[i];
                System.out.println("-> Removing " + pluginName + " ");
                try {
                    pluginManager.removePlugin(pluginName);
                } catch (IOException e) {
                    System.out.println("Failed to remove " + pluginName + ", reason: " + e.getMessage());
                }
            }
        } else {
            System.out.println("No command matching '" + command + "' found");
        }
    }
}
