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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

public class PluginClassLoader extends URLClassLoader {

    private final ClassLoader system;
    private final URL url;

    PluginClassLoader(URL[] urls, ClassLoader parent) throws IOException {
        super(urls, parent);
        url = (urls != null && urls.length > 0 ? urls[0] : null);

        ClassLoader sys = getSystemClassLoader();
        while (sys.getParent() != null) {
            sys = sys.getParent();
        }

        system = sys;
    }

    // load first from system class loader then fall back to this one
    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> c = findLoadedClass(name);
        if (c == null) {
            // check system class loader (jvm & bootclasspath)
            if (system != null) {
                try {
                    c = system.loadClass(name);
                } catch (ClassNotFoundException ignored) {
                }
            }
            if (c == null) {
                try {
                    // check plugin classloader
                    c = findClass(name);
                } catch (ClassNotFoundException e) {
                    // fall back to parent
                    c = super.loadClass(name, resolve);
                }
            }
        }
        if (resolve) {
            resolveClass(c);
        }
        return c;
    }

    @Override
    public URL getResource(String name) {
        // apply same rules frmo loadClass
        URL url = null;
        if (system != null) {
            url = system.getResource(name);
        }
        if (url == null) {
            url = findResource(name);
            if (url == null) {
                url = super.getResource(name);
            }
        }
        return url;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        List<URL> urls = Lists.newArrayList();
        if (system != null) {
            urls.addAll(Collections.list(system.getResources(name)));
        }
        urls.addAll(Collections.list(findResources(name)));

        ClassLoader parent = getParent();
        if (parent != null) {
            urls.addAll(Collections.list(parent.getResources(name)));
        }

        return Collections.enumeration(urls);
    }

    @Override
    public String toString() {
        return String.format(Locale.US, "PluginClassLoader for url [%s], classpath [%s], systemCL [%s]", url, Arrays.toString(getURLs()), system);
    }
}