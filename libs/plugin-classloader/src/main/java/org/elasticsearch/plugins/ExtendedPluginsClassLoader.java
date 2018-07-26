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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;

/**
 * A classloader that is a union over the parent core classloader and classloaders of extended plugins.
 */
public class ExtendedPluginsClassLoader extends ClassLoader {

    /** Loaders of plugins extended by a plugin. */
    private final List<ClassLoader> extendedLoaders;

    private ExtendedPluginsClassLoader(ClassLoader parent, List<ClassLoader> extendedLoaders) {
        super(parent);
        this.extendedLoaders = Collections.unmodifiableList(extendedLoaders);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        for (ClassLoader loader : extendedLoaders) {
            try {
                return loader.loadClass(name);
            } catch (ClassNotFoundException e) {
                // continue
            }
        }
        throw new ClassNotFoundException(name);
    }

    /**
     * Return a new classloader across the parent and extended loaders.
     */
    public static ExtendedPluginsClassLoader create(ClassLoader parent, List<ClassLoader> extendedLoaders) {
        return AccessController.doPrivileged((PrivilegedAction<ExtendedPluginsClassLoader>)
            () -> new ExtendedPluginsClassLoader(parent, extendedLoaders));
    }
}
