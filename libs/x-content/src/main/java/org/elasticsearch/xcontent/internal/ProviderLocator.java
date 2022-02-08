/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import org.elasticsearch.xcontent.spi.XContentProvider;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

public final class ProviderLocator {

    public static final XContentProvider INSTANCE = provider();

    private static XContentProvider provider() {
        String classpath = System.getProperty("java.class.path");
        for (String s : classpath.split(":")) {
            System.out.println(s);
        }

        try (InputStream is = ProviderLocator.class.getResourceAsStream("provider-jars.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            return loadAsNonModule(reader.lines().map(ProviderLocator.class::getResource).toArray(URL[]::new));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static XContentProvider loadAsNonModule(URL[] urls) {
            URLClassLoader loader = URLClassLoader.newInstance(urls, XContentProvider.class.getClassLoader());
            ServiceLoader<XContentProvider> sl = ServiceLoader.load(XContentProvider.class, loader);
            return sl.findFirst().orElseThrow(() -> new RuntimeException("cannot locate x-content provider"));
    }
}
