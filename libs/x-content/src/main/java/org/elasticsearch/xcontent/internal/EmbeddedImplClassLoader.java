/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.SecureClassLoader;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A class loader that is responsible for loading implementation classes and resources embedded within an archive.
 *
 * TODO: describe ...
 */
public final class EmbeddedImplClassLoader extends SecureClassLoader {

    private static final String IMPL_PREFIX = "IMPL-JARS/";
    private static final String MANIFEST_FILE = "/MANIFEST.TXT";

    private final List<String> prefixes;
    private final ClassLoader parent;

    private static List<String> getProviderPrefixes(ClassLoader parent, String providerName) {
        final String providerPrefix = IMPL_PREFIX + providerName;
        final InputStream is = parent.getResourceAsStream(providerPrefix + MANIFEST_FILE);
        if (is == null) {
            throw new IllegalStateException("missing x-content provider jars list");
        }
        try (
            InputStream in = is;
            InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(isr)
        ) {
            return reader.lines().map(s -> providerPrefix + "/" + s).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static EmbeddedImplClassLoader getInstance(ClassLoader parent, String providerName) {
        return new EmbeddedImplClassLoader(parent, getProviderPrefixes(parent, providerName));
    }

    private EmbeddedImplClassLoader(ClassLoader parent, List<String> prefixes) {
        super(parent);
        this.prefixes = prefixes;
        this.parent = parent;
    }

    /** Searches for the named resource. Iterates over all prefixes. */
    private InputStream privilegedGetResourceAsStreamOrNull(String name) {
        return AccessController.doPrivileged(new PrivilegedAction<InputStream>() {
            @Override
            public InputStream run() {
                return prefixes.stream()
                    .map(p -> p + "/" + name)
                    .map(parent::getResourceAsStream)
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
            }
        });
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
        String filepath = name.replace('.', '/').concat(".class");
        InputStream is = privilegedGetResourceAsStreamOrNull(filepath);
        if (is != null) {
            try (InputStream in = is) {
                byte[] bytes = in.readAllBytes();
                return defineClass(name, bytes, 0, bytes.length);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return super.findClass(name);
    }

    @Override
    protected URL findResource(String name) {
        Objects.requireNonNull(name);
        URL url = prefixes.stream().map(p -> p + "/" + name).map(parent::getResource).filter(Objects::nonNull).findFirst().orElse(null);
        if (url != null) {
            return url;
        }
        return super.findResource(name);
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
        final int size = prefixes.size();
        @SuppressWarnings("unchecked")
        Enumeration<URL>[] tmp = (Enumeration<URL>[]) new Enumeration<?>[size + 1];
        for (int i = 0; i < size; i++) {
            tmp[i] = parent.getResources(prefixes.get(i) + "/" + name);
        }
        tmp[size] = super.findResources(name);
        return new CompoundEnumeration<>(tmp);
    }

    static final class CompoundEnumeration<E> implements Enumeration<E> {
        private final Enumeration<E>[] enumerations;
        private int index;

        CompoundEnumeration(Enumeration<E>[] enumerations) {
            this.enumerations = enumerations;
        }

        private boolean next() {
            while (index < enumerations.length) {
                if (enumerations[index] != null && enumerations[index].hasMoreElements()) {
                    return true;
                }
                index++;
            }
            return false;
        }

        public boolean hasMoreElements() {
            return next();
        }

        public E nextElement() {
            if (next() == false) {
                throw new NoSuchElementException();
            }
            return enumerations[index].nextElement();
        }
    }
}
