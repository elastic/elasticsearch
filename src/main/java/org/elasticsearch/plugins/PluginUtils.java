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

package org.elasticsearch.plugins;

import com.google.common.collect.Lists;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public final class PluginUtils {

    private PluginUtils() {}

    @SuppressWarnings("unchecked")
    static Plugin loadPlugin(String className, Settings settings, ClassLoader classLoader) {
        try {
            Class<? extends Plugin> pluginClass = (Class<? extends Plugin>) classLoader.loadClass(className);
            try {
                return pluginClass.getConstructor(Settings.class).newInstance(settings);
            } catch (NoSuchMethodException e) {
                try {
                    return pluginClass.newInstance();
                } catch (Exception e1) {
                    throw new ElasticsearchException("No constructor for [" + pluginClass + "]. A plugin class must " +
                            "have either an empty default constructor or a single argument constructor accepting a " +
                            "Settings instance");
                }
            }

        } catch (Exception e) {
            throw new ElasticsearchException("Failed to load plugin class [" + className + "]", e);
        }
    }

    static List<File> pluginClassPathAsFiles(File pluginFolder) throws IOException {
        List<File> cpFiles = Lists.newArrayList();
        cpFiles.add(pluginFolder);

        List<File> libFiles = Lists.newArrayList();
        File[] files = pluginFolder.listFiles();
        if (files != null) {
            Collections.addAll(libFiles, files);
        }
        File libLocation = new File(pluginFolder, "lib");
        if (libLocation.exists() && libLocation.isDirectory()) {
            files = libLocation.listFiles();
            if (files != null) {
                Collections.addAll(libFiles, files);
            }
        }

        // if there are jars in it, add it as well
        for (File libFile : libFiles) {
            if (libFile.getName().endsWith(".jar") || libFile.getName().endsWith(".zip")) {
                cpFiles.add(libFile);
            }
        }

        return cpFiles;
    }

    static boolean lookupIsolation(List<URL> pluginProperties, boolean defaultIsolation) throws IOException {
        Properties props = new Properties();
        InputStream is = null;
        for (URL prop : pluginProperties) {
            try {
                props.load(prop.openStream());
                return Booleans.parseBoolean(props.getProperty("isolation"), defaultIsolation);
            } finally {
                IOUtils.closeWhileHandlingException(is);
            }
        }
        return defaultIsolation;
    }

    static List<URL> lookupPluginProperties(List<File> pluginClassPath) throws Exception {
        if (pluginClassPath.isEmpty()) {
            return Collections.emptyList();
        }

        List<URL> found = Lists.newArrayList();

        for (File file : pluginClassPath) {
            String toString = file.getName();
            if (toString.endsWith(".jar") || toString.endsWith(".zip")) {
                JarFile jar = new JarFile(file);
                try {
                    JarEntry jarEntry = jar.getJarEntry("es-plugin.properties");
                    if (jarEntry != null) {
                        found.add(new URL("jar:" + file.toURI().toString() + "!/es.plugin.properties"));
                    }
                } finally {
                    IOUtils.closeWhileHandlingException(jar);
                }
            }
            else {
                File props = new File(file, "es-plugin.properties");
                if (props.exists() && props.canRead()) {
                    found.add(props.toURI().toURL());
                }
            }
        }

        return found;
    }

    static URL[] convertFileToUrl(List<File> pluginClassPath) throws IOException {
        URL[] urls = new URL[pluginClassPath.size()];
        for (int i = 0; i < urls.length; i++) {
            urls[i] = pluginClassPath.get(i).toURI().toURL();
        }
        return urls;
    }
}