/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.loader;

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
        return AccessController.doPrivileged(
            (PrivilegedAction<ExtendedPluginsClassLoader>) () -> new ExtendedPluginsClassLoader(parent, extendedLoaders)
        );
    }
}
