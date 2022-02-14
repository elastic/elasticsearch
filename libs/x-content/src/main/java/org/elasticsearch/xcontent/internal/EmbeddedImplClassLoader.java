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
import java.util.stream.Collectors;

public class EmbeddedImplClassLoader extends SecureClassLoader {

    private static final String IMPL_PREFIX = "IMPL-JARS";
    private final List<String> prefixes;

    EmbeddedImplClassLoader(ClassLoader parent) {
        super(parent);

        final InputStream is = getParent().getResourceAsStream(IMPL_PREFIX + "/MANIFEST.TXT");
        if (is == null) {
            throw new IllegalStateException("missing x-content provider jars list");
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            prefixes = reader.lines().map(s -> IMPL_PREFIX + "/" + s).collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Searches for the named resource. Iterates over all prefixes. */
    private InputStream privilegedGetResourceAsStreamOrNull(String name) {
        return AccessController.doPrivileged(new PrivilegedAction<InputStream>() {
            @Override
            public InputStream run() {
                final ClassLoader outer = getParent();
                return prefixes.stream()
                    .map(p -> p + "/" + name)
                    .map(outer::getResourceAsStream)
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
        for (String prefix : prefixes) {
            URL url = getParent().getResource(prefix + "/" + name);
            if (url != null) {
                return url;
            }
        }
        return super.findResource(name);
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
        final ClassLoader outer = getParent();
        final int size = prefixes.size();
        @SuppressWarnings("unchecked")
        Enumeration<URL>[] tmp = (Enumeration<URL>[]) new Enumeration<?>[size + 1];
        for (int i = 0; i < size; i++) {
            tmp[i] = outer.getResources(prefixes.get(i) + "/" + name);
        }
        tmp[size + 1] = super.findResources(name);
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
