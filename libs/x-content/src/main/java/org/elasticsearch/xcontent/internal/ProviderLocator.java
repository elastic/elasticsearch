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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureClassLoader;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * A provider locator for finding the {@link XContentProvider}.
 *
 * <p> Provider implementations are located as follows:
 * <ol>
 *   <li> First the <i>provider specific</i> path is found, </li>
 *   <li> then the provider is loaded from the artifacts located at that path. </li>
 * </ol>
 *
 * <p> The provider specific path is found by searching for the <i>root providers</i> path, then resolving the specific provider
 * type against that path, <i>x-content</i> in this case. The root providers' path is an entry on the class path referring to a directory
 * in ending with "providers". Within the distribution this will be <i>lib/providers</i>, but may vary when running tests.
 *
 * <p> The artifacts that constitute the provider implementation (the impl jar and its dependencies) are explicitly named in the
 * provider-jars.txt resource file. Each name is resolved against the provider specific path to locate the actual on disk jar artifact. The
 * collection of these artifacts constitute the provider implementation.
 */
public final class ProviderLocator {

    /**
     * Returns the provider instance.
     */
    public static final XContentProvider INSTANCE = provider();

    private static class EmbeddedImplClassLoader extends SecureClassLoader {

        private static final String IMPL_PREFIX = "IMPL-JARS";
        private final List<String> prefixes;

        EmbeddedImplClassLoader() {
            super(ProviderLocator.class.getClassLoader());

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
    }

    private static XContentProvider provider() {
        try {
            PrivilegedExceptionAction<XContentProvider> pa = ProviderLocator::loadAsNonModule;
            return AccessController.doPrivileged(pa);
        } catch (PrivilegedActionException e) {
            throw new UncheckedIOException((IOException) e.getCause());
        }
    }

    private static XContentProvider loadAsNonModule() {
        ClassLoader loader = new EmbeddedImplClassLoader();
        ServiceLoader<XContentProvider> sl = ServiceLoader.load(XContentProvider.class, loader);
        return sl.findFirst().orElseThrow(() -> new RuntimeException("cannot locate x-content provider"));
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
