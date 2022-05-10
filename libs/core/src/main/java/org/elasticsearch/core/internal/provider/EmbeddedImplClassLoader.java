/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.CodeSigner;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.SecureClassLoader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.jar.Manifest;

import static java.util.jar.Attributes.Name.MULTI_RELEASE;
import static java.util.stream.Collectors.toUnmodifiableMap;

/**
 * A class loader that is responsible for loading implementation classes and resources embedded
 * within an archive.
 *
 * <p> This loader facilitates a scenario whereby an API can embed its implementation and
 * dependencies all within the same archive as the API itself. The archive can be put directly on
 * the class path, where it's API classes are loadable by the application class loader, but the
 * embedded implementation and dependencies are not. When locating a concrete provider, the API can
 * create an instance of an EmbeddedImplClassLoader to locate and load the implementation.
 *
 * <p> The archive typically consists of two disjoint logically groups:
 *  1. the top-level classes and resources,
 *  2. the embedded classes and resources
 *
 * <p> The top-level classes and resources are typically loaded and located, respectively, by the
 * parent of an EmbeddedImplClassLoader loader. The embedded classes and resources, are located by
 * the parent loader as pure resources with a provider specific name prefix, and classes are defined
 * by the EmbeddedImplClassLoader. The list of prefixes is determined by reading the entries in the
 * LISTING.TXT.
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
    private static final String JAR_LISTING_FILE = "/LISTING.TXT";

    record JarMeta(String prefix, boolean isMultiRelease) {}

    /** Ordered list of jar metadata (prefixes and code sources) to use when loading classes and resources. */
    private final List<JarMeta> jarMetas;

    /** A map of prefix to codebase, used to determine the code source when defining classes. */
    private final Map<String, CodeSource> prefixToCodeBase;

    /** The loader used to find the class and resource bytes. */
    private final ClassLoader parent;

    static EmbeddedImplClassLoader getInstance(ClassLoader parent, String providerName) {
        PrivilegedAction<EmbeddedImplClassLoader> pa = () -> new EmbeddedImplClassLoader(parent, getProviderPrefixes(parent, providerName));
        return AccessController.doPrivileged(pa);
    }

    private EmbeddedImplClassLoader(ClassLoader parent, Map<JarMeta, CodeSource> prefixToCodeBase) {
        super(null);
        this.jarMetas = prefixToCodeBase.keySet().stream().toList();
        this.parent = parent;
        this.prefixToCodeBase = prefixToCodeBase.entrySet()
            .stream()
            .collect(toUnmodifiableMap(k -> k.getKey().prefix(), Map.Entry::getValue));
    }

    record Resource(InputStream inputStream, CodeSource codeSource) {}

    /** Searches for the named resource. Iterates over all prefixes. */
    private Resource privilegedGetResourceOrNull(String name) {
        return AccessController.doPrivileged((PrivilegedAction<Resource>) () -> {
            for (JarMeta jarMeta : jarMetas) {
                InputStream is = findResourceForPrefixOrNull(name, jarMeta, parent::getResourceAsStream);
                if (is != null) {
                    return new Resource(is, prefixToCodeBase.get(jarMeta.prefix()));
                }
            }
            return null;
        });
    }

    @Override
    public Class<?> findClass(String moduleName, String name) {
        try {
            Class<?> c = findClass(name);
            if (moduleName != null && moduleName.equals(c.getModule().getName()) == false) {
                throw new AssertionError("expected module:" + moduleName + ", got: " + c.getModule().getName());
            }
            return c;
        } catch (ClassNotFoundException ignore) {}
        return null;
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
        for (JarMeta jarMeta : jarMetas) {
            URL url = findResourceForPrefixOrNull(name, jarMeta, parent::getResource);
            if (url != null) {
                return url;
            }
        }
        return parent.getResource(name);
    }

    /**
     * Searches for the named resource, returning its url or null if not found.
     * Iterates over all multi-release versions, then the root, for the given jar prefix.
     */
    <T> T findResourceForPrefixOrNull(String name, JarMeta jarMeta, Function<String, T> finder) {
        T resource;
        if (jarMeta.isMultiRelease) {
            resource = findVersionedResourceForPrefixOrNull(jarMeta.prefix(), name, finder);
            if (resource != null) {
                return resource;
            }
        }
        return finder.apply(jarMeta.prefix() + "/" + name);
    }

    <T> T findVersionedResourceForPrefixOrNull(String prefix, String name, Function<String, T> finder) {
        for (int v = RUNTIME_VERSION_FEATURE; v >= BASE_VERSION_FEATURE; v--) {
            T resource = finder.apply(prefix + "/" + MRJAR_VERSION_PREFIX + v + "/" + name);
            if (resource != null) {
                return resource;
            }
        }
        return null;
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
        Enumeration<URL> enum1 = new Enumeration<>() {
            private int jarMetaIndex = 0;

            private URL url = null;

            private boolean next() {
                if (url != null) {
                    return true;
                } else {
                    while (jarMetaIndex < jarMetas.size()) {
                        URL u = findResourceForPrefixOrNull(name, jarMetas.get(jarMetaIndex), parent::getResource);
                        jarMetaIndex++;
                        if (u != null) {
                            url = u;
                            return true;
                        }
                    }
                    return false;
                }
            }

            @Override
            public boolean hasMoreElements() {
                return next();
            }

            @Override
            public URL nextElement() {
                if (next() == false) {
                    throw new NoSuchElementException();
                }
                URL u = url;
                url = null;
                return u;
            }
        };

        @SuppressWarnings("unchecked")
        Enumeration<URL>[] tmp = (Enumeration<URL>[]) new Enumeration<?>[2];
        tmp[0] = enum1;
        tmp[1] = parent.getResources(name);
        return new CompoundEnumeration<>(tmp);
    }

    // -- infra

    private static Map<JarMeta, CodeSource> getProviderPrefixes(ClassLoader parent, String providerName) {
        String providerPrefix = IMPL_PREFIX + providerName;
        InputStream in = parent.getResourceAsStream(providerPrefix + JAR_LISTING_FILE);
        if (in == null) {
            throw new IllegalStateException("missing %s provider jars list".formatted(providerName));
        }
        try (
            in;
            InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(isr)
        ) {
            List<String> jars = reader.lines().toList();
            Map<JarMeta, CodeSource> map = new HashMap<>();
            for (String jar : jars) {
                final String jarPrefix = providerPrefix + "/" + jar;
                JarMeta jam;
                if (isMultiRelease(parent, jarPrefix)) {
                    jam = new JarMeta(jarPrefix, true);
                } else {
                    jam = new JarMeta(jarPrefix, false);
                }
                map.put(jam, codeSource(parent.getResource(providerPrefix + JAR_LISTING_FILE), jar));
            }
            return Map.copyOf(map);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static CodeSource codeSource(URL baseURL, String jarName) throws MalformedURLException {
        return new CodeSource(new URL(baseURL, jarName), (CodeSigner[]) null /*signers*/);
    }

    private static boolean isMultiRelease(ClassLoader parent, String jarPrefix) throws IOException {
        try (InputStream is = parent.getResourceAsStream(jarPrefix + "/META-INF/MANIFEST.MF")) {
            if (is != null) {
                Manifest manifest = new Manifest(is);
                return Boolean.parseBoolean(manifest.getMainAttributes().getValue(MULTI_RELEASE));
            }
        }
        return false;
    }

    private static final int BASE_VERSION_FEATURE = 8; // lowest supported release version
    private static final int RUNTIME_VERSION_FEATURE = Runtime.version().feature();

    static {
        assert RUNTIME_VERSION_FEATURE >= BASE_VERSION_FEATURE;
    }

    private static final String MRJAR_VERSION_PREFIX = "META-INF/versions/";

    private static String getParent(String uriString) {
        int index = uriString.lastIndexOf('/');
        if (index > 0) {
            return uriString.substring(0, index);
        }
        return "/";
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
