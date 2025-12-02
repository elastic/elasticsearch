/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.cli;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.function.Predicate;

/**
 * A URLClassLoader that attempts to load classes from its URLs before delegating
 * to its parent classloader, for classes that match a provided filter.
 */
class ParentLastUrlClassLoader extends URLClassLoader {

    private final Predicate<String> filter;

    ParentLastUrlClassLoader(URL[] urls, ClassLoader parent, Predicate<String> filter) {
        super(urls, parent);
        this.filter = filter;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // The following implementation mirrors that of ClassLoader.loadClass, except that
        // we attempt to load the class from this classloader first, and only delegate
        // to the parent (1) if the filter does not match or (2) if the class is not found locally
        synchronized (getClassLoadingLock(name)) {
            Class<?> c = findLoadedClass(name);
            if (c == null) {
                if (filter.test(name)) {
                    try {
                        c = findClass(name);
                    } catch (ClassNotFoundException e) {
                        // ignore and try parent
                    }
                }
                if (c == null) {
                    c = super.loadClass(name, resolve);
                }
            }
            if (resolve) {
                resolveClass(c);
            }
            return c;
        }
    }

}
