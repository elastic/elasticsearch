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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.CodeSigner;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.jar.Manifest;

import static java.util.jar.Attributes.Name.MULTI_RELEASE;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.elasticsearch.core.internal.provider.EmbeddedModulePath.isPackageName;

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

    record JarMeta(String prefix, boolean isMultiRelease, Set<String> packages, Map<String, List<Integer>> pkgToVersions) {}

    /** Ordered list of jar metadata (prefixes and code sources) to use when loading classes and resources. */
    private final List<JarMeta> jarMetas;

    /** Map of package to Jar metadata. */
    private final Map<String, JarMeta> packageToJarMeta;

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
        Map<String, JarMeta> map = new HashMap<>();
        for (var jarMeta : prefixToCodeBase.keySet()) {
            jarMeta.packages().stream().forEach(pkg -> {
                var prev = map.put(pkg, jarMeta);
                assert prev == null;
            });
        }
        this.packageToJarMeta = Collections.unmodifiableMap(map);
    }

    record Resource(InputStream inputStream, CodeSource codeSource) {}

    /** Searches for the named resource. Iterates over all prefixes. */
    private Resource privilegedGetResourceOrNull(JarMeta jarMeta, String pkg, String filepath) {
        return AccessController.doPrivileged((PrivilegedAction<Resource>) () -> {
            InputStream is = findResourceInLoaderPkgOrNull(jarMeta, pkg, filepath, parent::getResourceAsStream);
            if (is != null) {
                return new Resource(is, prefixToCodeBase.get(jarMeta.prefix()));
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
        String pkg = toPackageName(filepath);
        JarMeta jarMeta = packageToJarMeta.get(pkg);
        if (jarMeta != null) {
            Resource res = privilegedGetResourceOrNull(jarMeta, pkg, filepath);
            if (res != null) {
                try (InputStream in = res.inputStream()) {
                    byte[] bytes = in.readAllBytes();
                    return defineClass(name, bytes, 0, bytes.length, res.codeSource());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            throw new ClassNotFoundException(name);
        } else {
            return parent.loadClass(name);
        }
    }

    @Override
    protected URL findResource(String name) {
        Objects.requireNonNull(name);
        String pkg = toPackageName(name);
        JarMeta jarMeta = packageToJarMeta.get(pkg);
        URL url;
        if (jarMeta != null) {
            url = findResourceInLoaderPkgOrNull(jarMeta, pkg, name, parent::getResource);
        } else {
            // not in a package defined to this loader, slow search
            url = findMiscResourceOrNull(name);
        }
        if (url != null) {
            return url;
        }
        return parent.getResource(name);
    }

    /**
     * Searches for the named resource located in a package defined to this loader, returning its
     * url or null if not found. Iterates over all known package specific multi-release versions,
     * then the root, for the given jar prefix.
     */
    <T> T findResourceInLoaderPkgOrNull(JarMeta jarMeta, String pkg, String name, Function<String, T> finder) {
        List<Integer> releaseVersions = jarMeta.pkgToVersions().getOrDefault(pkg, List.of());
        for (int releaseVersion : releaseVersions) {
            String fullName = jarMeta.prefix() + "/" + MRJAR_VERSION_PREFIX + releaseVersion + "/" + name;
            T t = finder.apply(fullName);
            if (t != null) {
                return t;
            }
        }
        return finder.apply(jarMeta.prefix() + "/" + name);
    }

    /**
     * Searches for the named resource, returning its url or null if not found.
     * Iterates over all multi-release versions, then the root, for all jars.
     * Slow path for resources not necessarily in packages defined to this loader.
     */
    URL findMiscResourceOrNull(String name) {
        for (JarMeta jarMeta : jarMetas) {
            URL url = findResourceForPrefixOrNull(name, jarMeta);
            if (url != null) {
                return url;
            }
        }
        return null;
    }

    /**
     * Searches for the named resource, returning its url or null if not found.
     * Iterates over all multi-release versions, then the root, for the given jar prefix.
     */
    URL findResourceForPrefixOrNull(String name, JarMeta jarMeta) {
        if (jarMeta.isMultiRelease) {
            URL url = findVersionedResourceForPrefixOrNull(jarMeta.prefix(), name);
            if (url != null) {
                return url;
            }
        }
        return parent.getResource(jarMeta.prefix() + "/" + name);
    }

    URL findVersionedResourceForPrefixOrNull(String prefix, String name) {
        for (int v = RUNTIME_VERSION_FEATURE; v >= BASE_VERSION_FEATURE; v--) {
            URL url = parent.getResource(prefix + "/" + MRJAR_VERSION_PREFIX + v + "/" + name);
            if (url != null) {
                return url;
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
                        URL u = findResourceForPrefixOrNull(name, jarMetas.get(jarMetaIndex));
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

    // -- modules

    /**
     * Returns a module finder capable of finding the modules that are loadable by this embedded
     * impl class loader.
     *
     * <p> The module finder returned by this method can be used during resolution in order to
     * create a configuration. This configuration can subsequently be materialized as a module layer
     * in which classes and resources are loaded by this embedded impl class loader.
     *
     * @param missingModules a set of module names to ignore if not present
     */
    InMemoryModuleFinder moduleFinder(Set<String> missingModules) throws IOException {
        Path[] modulePath = modulePath();
        assert modulePath.length >= 1;
        InMemoryModuleFinder moduleFinder = InMemoryModuleFinder.of(missingModules, modulePath);
        if (modulePath[0].getFileSystem().provider().getScheme().equals("jar")) {
            modulePath[0].getFileSystem().close();
        }
        return moduleFinder;
    }

    /**
     * Returns the base prefix for a versioned prefix. Otherwise, the given prefix if not versioned.
     * For example, given "IMPL-JARS/x-content/jackson-core-2.13.2.jar/META-INF/versions/9", returns
     * "IMPL-JARS/x-content/jackson-core-2.13.2.jar".
     */
    static String basePrefix(String prefix) {
        int idx = prefix.indexOf(MRJAR_VERSION_PREFIX);
        if (idx == -1) {
            return prefix;
        }
        return prefix.substring(0, idx - 1);
    }

    private Path[] modulePath() throws IOException {
        URI rootURI = rootURI(prefixToCodeBase.values().stream().findFirst().map(CodeSource::getLocation).orElseThrow());
        return embeddedJarPath(prefixToCodeBase.keySet(), rootURI);
    }

    private static Path[] embeddedJarPath(Set<String> prefixes, URI rootURI) throws IOException {
        Function<Path, Path[]> entries = path -> prefixes.stream()
            .map(EmbeddedImplClassLoader::basePrefix)
            .distinct()
            .map(pfx -> path.resolve(pfx))
            .toArray(Path[]::new);
        if (rootURI.getScheme().equals("file")) {
            return entries.apply(Path.of(rootURI));
        } else if (rootURI.getScheme().equals("jar")) {
            FileSystem fileSystem = FileSystems.newFileSystem(rootURI, Map.of(), ClassLoader.getSystemClassLoader());
            Path rootPath = fileSystem.getPath("/");
            return entries.apply(rootPath);
        } else {
            throw new IOException("unknown scheme:" + rootURI.getScheme());
        }
    }

    // -- infra

    /**
     * Returns the root URI for a given url. The root URI is the base URI where all classes and
     * resources can be searched for by appending a prefixes.
     *
     * Depending on whether running from a jar (distribution), or an exploded archive (testing),
     * the given url will have one of two schemes, "file", or "jar:file". For example:
     *  distro- jar:file:/xxx/distro/lib/elasticsearch-x-content-8.2.0-SNAPSHOT.jar!/IMPL-JARS/x-content/xlib-2.10.4.jar
     *  rootURI jar:file:/xxx/distro/lib/elasticsearch-x-content-8.2.0-SNAPSHOT.jar
     *
     *  test  - file:/x/git/es_modules/libs/x-content/build/generated-resources/impl/IMPL-JARS/x-content/xlib-2.10.4.jar
     *  rootURI file:/x/git/es_modules/libs/x-content/build/generated-resources/impl
     */
    static URI rootURI(URL url) {
        try {
            URI uri = url.toURI();
            if (uri.getScheme().equals("jar")) {
                String s = uri.toString();
                return URI.create(s.substring(0, s.lastIndexOf("!/")));
            } else {
                return URI.create(getParent(getParent(getParent(uri.toString()))));
            }
        } catch (URISyntaxException unexpected) {
            throw new AssertionError(unexpected); // should never happen
        }
    }

    static String removeMRJARPrefix(Path path) {
        assert path.startsWith(MRJAR_VERSION_PREFIX) : path;
        int c = path.getNameCount();
        assert c >= 3 : path.getNameCount();
        return c == 3 ? "" : path.subpath(3, path.getNameCount()).toString();
    }

    static int getMRVersionFromPrefix(Path path) {
        assert path.startsWith(MRJAR_VERSION_PREFIX) : path;
        int c = path.getNameCount();
        assert c >= 3 : path.getNameCount();
        return Integer.valueOf(path.subpath(2, 3).toString());
    }

    static String toPackageName(String name) {
        assert name.endsWith("/") == false;
        int index = name.lastIndexOf('/');
        if (index != -1) {
            return name.substring(0, index).replace('/', '.');
        }
        return name;
    }

    /** Tuple of package and package-to-release-version map. */
    record ScanResult(Set<String> packages, Map<String, List<Integer>> pkgVersions) {}

    static ScanResult scanPackages(Path dir, boolean isMultiRelease) {
        String separator = dir.getFileSystem().getSeparator();
        Set<String> pkgs = new HashSet<>();
        Map<String, List<Integer>> pkgVersions = new HashMap<>();
        try (var paths = Files.find(dir, Integer.MAX_VALUE, ((path, attrs) -> attrs.isRegularFile()))) {
            paths.map(path -> dir.relativize(path)).forEach(path -> {
                Path parent = path.getParent();
                if (parent != null) {
                    if (parent.startsWith(MRJAR_VERSION_PREFIX)) {
                        int version = getMRVersionFromPrefix(path);
                        if (isMultiRelease && version <= RUNTIME_VERSION_FEATURE) {
                            String pkg = removeMRJARPrefix(parent).replace(separator, ".");
                            if (isPackageName(pkg)) {
                                pkgVersions.computeIfAbsent(pkg, k -> new ArrayList<>()).add(version);
                                pkgs.add(pkg);
                            }
                        }
                    } else {
                        String pkg = parent.toString().replace(separator, ".");
                        if (isPackageName(pkg)) {
                            pkgs.add(pkg);
                        }
                    }
                }
            });
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }

        return new ScanResult(
            Set.copyOf(pkgs),
            pkgVersions.entrySet()
                .stream() // sort MR JAR prefixes by release number descending
                .collect(toUnmodifiableMap(Map.Entry::getKey, v -> v.getValue().stream().sorted(Comparator.reverseOrder()).toList()))
        );
    }

    private static Map<JarMeta, CodeSource> getProviderPrefixes(ClassLoader parent, String providerName) {
        String providerPrefix = IMPL_PREFIX + providerName;
        InputStream in = parent.getResourceAsStream(providerPrefix + JAR_LISTING_FILE);
        if (in == null) {
            throw new IllegalStateException(String.format(Locale.ROOT, "missing %s provider jars list", providerName));
        }
        try (
            in;
            InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(isr)
        ) {
            List<String> jars = reader.lines().toList();
            Map<JarMeta, CodeSource> map = new HashMap<>();
            for (String jar : jars) {
                final CodeSource codeSource = codeSource(parent.getResource(providerPrefix + JAR_LISTING_FILE), jar);
                final String jarPrefix = providerPrefix + "/" + jar;
                final boolean isMultiRelease = isMultiRelease(parent, jarPrefix);
                URI rootURI = rootURI(codeSource.getLocation());
                Path p = embeddedJarPath(Set.of(jarPrefix), rootURI)[0];
                ScanResult scan;
                try {
                    scan = scanPackages(p, isMultiRelease);
                } finally {
                    if (p.getFileSystem().provider().getScheme().equals("jar")) {
                        p.getFileSystem().close();
                    }
                }
                map.put(new JarMeta(jarPrefix, isMultiRelease(parent, jarPrefix), scan.packages(), scan.pkgVersions()), codeSource);
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
