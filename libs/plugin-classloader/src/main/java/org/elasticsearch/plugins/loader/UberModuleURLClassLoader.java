/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.loader;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

/**
 * This class loader loads classes and resources from a search path of URLs
 * referring to by JAR files, defining all classes into a single named module.
 *
 * To be meaningful, this loader needs to use this loaded in combination
 * with ModuleLayer::defineModules.
 */
// Inspired by j.n.URLCLassLoader.FactoryURLClassLoader. We need our own subclass
// so that we can override findResource(moduleName, name)
public final class UberModuleURLClassLoader extends URLClassLoader {

    private final String moduleName;

    static {
        ClassLoader.registerAsParallelCapable();
    }

    UberModuleURLClassLoader(String moduleName, URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.moduleName = moduleName;
    }

    /**
     * Creates a new instance of URLClassLoader for the specified, module name,
     * URLs and parent class loader.
     *
     * @param moduleName the synthetic module name
     * @param urls the URLs to search for classes and resources
     * @param parent the parent class loader for delegation
     * @return the class loader
     */
    public static URLClassLoader newInstance(final String moduleName, final URL[] urls, final ClassLoader parent) {
        Objects.requireNonNull(moduleName);
        Objects.requireNonNull(urls);
        Objects.requireNonNull(parent);

        // Need a privileged block to create the class loader
        @SuppressWarnings("removal")
        URLClassLoader ucl = AccessController.doPrivileged(new PrivilegedAction<>() {
            public URLClassLoader run() {
                return new UberModuleURLClassLoader(moduleName, urls, parent);
            }
        });
        return ucl;
    }

    // @SuppressWarnings("removal")
    // @Override
    // public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    // SecurityManager sm = System.getSecurityManager();
    // if (sm != null) {
    // int i = name.lastIndexOf('.');
    // if (i != -1) {
    // sm.checkPackageAccess(name.substring(0, i));
    // }
    // }
    // return super.loadClass(name, resolve);
    // }

    @Override
    protected URL findResource(String moduleName, String name) {
        if (moduleName == null || this.moduleName.equals(moduleName)) {
            return findResource(name);
        } else {
            return null;
        }
    }
}
