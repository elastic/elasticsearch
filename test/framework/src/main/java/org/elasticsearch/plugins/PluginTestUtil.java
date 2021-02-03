/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** Utility methods for testing plugins */
public class PluginTestUtil {

    public static void writePluginProperties(Path pluginDir, String... stringProps) throws IOException {
        writeProperties(pluginDir.resolve(PluginInfo.ES_PLUGIN_PROPERTIES), stringProps);
    }

    /** convenience method to write a plugin properties file */
    private static void writeProperties(Path propertiesFile, String... stringProps) throws IOException {
        assert stringProps.length % 2 == 0;
        Files.createDirectories(propertiesFile.getParent());
        Properties properties =  new Properties();
        for (int i = 0; i < stringProps.length; i += 2) {
            properties.put(stringProps[i], stringProps[i + 1]);
        }
        try (OutputStream out = Files.newOutputStream(propertiesFile)) {
            properties.store(out, "");
        }
    }
}
