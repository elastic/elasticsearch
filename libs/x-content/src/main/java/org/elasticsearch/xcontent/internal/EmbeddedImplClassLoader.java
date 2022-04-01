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
import java.security.CodeSigner;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.SecureClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A class loader that is responsible for loading implementation classes and resources embedded within an archive.
 *
 * <p> This loader facilitates a scenario whereby an API can embed its implementation and dependencies all within the same archive as the
 * API itself. The archive can be put directly on the class path, where it's API classes are loadable by the application class loader, but
 * the embedded implementation and dependencies are not. When locating a concrete provider, the API can create an instance of an
 * EmbeddedImplClassLoader to locate and load the implementation.
 *
 * <p> The archive typically consists of two disjoint logically groups:
 *  1. the top-level classes and resources,
 *  2. the embedded classes and resources
 *
 * <p> The top-level classes and resources are typically loaded and located, respectively, by the parent of an EmbeddedImplClassLoader
 * loader. The embedded classes and resources, are located by the parent loader as pure resources with a provider specific name prefix, and
 * classes are defined by the EmbeddedImplClassLoader. The list of prefixes is determined by reading the entries in the MANIFEST.TXT.
 *
 * <p> For example, the structure of the archive named x-content:
 * <pre>
 *  /org/elasticsearch/xcontent/XContent.class
 *  /IMPL-JARS/x-content/LISTING.TXT - contains list of jar file names, newline separated
 *  /IMPL-JARS/x-content/x-content-impl.jar/xxx
 *  /IMPL-JARS/x-content/dep-1.jar/abc
 *  /IMPL-JARS/x-content/dep-2.jar/xyz
 * </pre>
 */
public final class EmbeddedImplClassLoader extends SecureClassLoader {

    private static final String IMPL_PREFIX = "IMPL-JARS/";
    private static final String MANIFEST_FILE = "/LISTING.TXT";

    private final List<String> prefixes;
    private final ClassLoader parent;
    private final Map<String, CodeSource> prefixToCodeBase;

    private static Map<String, CodeSource> getProviderPrefixes(ClassLoader parent, String providerName) {
        String providerPrefix = IMPL_PREFIX + providerName;
        URL manifest = parent.getResource(providerPrefix + MANIFEST_FILE);
        if (manifest == null) {
            throw new IllegalStateException("missing x-content provider jars list");
        }
        try (
            InputStream in = manifest.openStream();
            InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(isr)
        ) {
            List<String> jars = reader.lines().toList();
            Map<String, CodeSource> map = new HashMap<>();
            for (String jar : jars) {
                map.put(providerPrefix + "/" + jar, new CodeSource(new URL(manifest, jar), (CodeSigner[]) null /*signers*/));
            }
            return Collections.unmodifiableMap(map);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static EmbeddedImplClassLoader getInstance(ClassLoader parent, String providerName) {
        return new EmbeddedImplClassLoader(parent, getProviderPrefixes(parent, providerName));
    }

    private EmbeddedImplClassLoader(ClassLoader parent, Map<String, CodeSource> prefixToCodeBase) {
        super(null);
        this.prefixes = prefixToCodeBase.keySet().stream().toList();
        this.prefixToCodeBase = prefixToCodeBase;
        this.parent = parent;
    }

    record Resource(InputStream inputStream, CodeSource codeSource) {}

    /** Searches for the named resource. Iterates over all prefixes. */
    private Resource privilegedGetResourceOrNull(String name) {
        return AccessController.doPrivileged(new PrivilegedAction<Resource>() {
            @Override
            public Resource run() {
                for (String prefix : prefixes) {
                    URL url = parent.getResource(prefix + "/" + name);
                    if (url != null) {
                        try {
                            InputStream is = url.openStream();
                            return new Resource(is, prefixToCodeBase.get(prefix));
                        } catch (IOException e) {
                            // silently ignore, same as ClassLoader
                        }
                    }
                }
                return null;
            }
        });
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
        String filepath = name.replace('.', '/').concat(".class");
        Resource res = privilegedGetResourceOrNull(filepath);
        if (res != null) {
            try (InputStream in = res.inputStream()) {
                byte[] bytes = in.readAllBytes();
                return defineClass(name, bytes, 0, bytes.length, res.codeSource());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return parent.loadClass(name);
    }

    @Override
    protected URL findResource(String name) {
        Objects.requireNonNull(name);
        URL url = prefixes.stream().map(p -> p + "/" + name).map(parent::getResource).filter(Objects::nonNull).findFirst().orElse(null);
        if (url != null) {
            return url;
        }
        return parent.getResource(name);
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
        final int size = prefixes.size();
        @SuppressWarnings("unchecked")
        Enumeration<URL>[] tmp = (Enumeration<URL>[]) new Enumeration<?>[size + 1];
        for (int i = 0; i < size; i++) {
            tmp[i] = parent.getResources(prefixes.get(i) + "/" + name);
        }
        tmp[size] = parent.getResources(name);
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
