package org.elasticsearch.plugins;

import org.elasticsearch.Version;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPerparer;
import org.elasticsearch.util.Tuple;
import org.elasticsearch.util.http.HttpDownloadHelper;
import org.elasticsearch.util.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;

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

    public static void main(String[] args) {
        Tuple<Settings, Environment> initialSettings = InternalSettingsPerparer.prepareSettings(EMPTY_SETTINGS, true);

        if (!initialSettings.v2().pluginsFile().exists()) {
            initialSettings.v2().pluginsFile().mkdirs();
        }

        PluginManager pluginManager = new PluginManager(initialSettings.v2(), "http://elasticsearch.googlecode.com/svn/plugins");

        if (args.length < 1) {
            System.out.println("Usage:");
            System.out.println("    - get [list of plugin names]: Downloads all the listed plugins");
        }
        String command = args[0];
        if (command.equals("get") || command.equals("-get") || command.equals("-g") || command.equals("--get")) {
            if (args.length < 2) {
                System.out.println("'get' requires an additional parameter with the plugin name");
            }
            for (int i = 1; i < args.length; i++) {
                String pluginName = args[i];
                System.out.print("-> Downloading " + pluginName + " ");
                try {
                    pluginManager.downloadPlugin(pluginName);
                    System.out.println(" DONE");
                } catch (IOException e) {
                    System.out.println("Failed to download " + pluginName + ", reason: " + e.getMessage());
                }
            }
        } else {
            System.out.println("No command matching '" + command + "' found");
        }
    }
}
