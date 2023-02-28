/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.module.Configuration;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.CodeSigner;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.SecureClassLoader;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This classloader will load classes from non-modularized sets of jars.
 * A synthetic module will be created for all jars in the bundle. We want
 * to be able to construct the read relationships in the module graph for this
 * synthetic module, which will make it different from the unnamed (classpath)
 * module.
 * <p>
 * Internally, we can delegate to a URLClassLoader, which is battle-tested when
 * it comes to reading classes out of jars.
 * <p>
 * This classloader needs to avoid parent-first search: we'll check classes
 * against a list of packages in this synthetic module, and load a class
 * directly if it's part of this synthetic module. This will keep libraries from
 * clashing.
 */
public class UberModuleClassLoader extends SecureClassLoader implements AutoCloseable {

    private final Module module;
    private final URLClassLoader internalLoader;
    private final CodeSource codeSource;
    private final ModuleLayer.Controller moduleController;
    private final Set<String> packageNames;

    private static Map<String, Set<String>> getModuleToServiceMap(ModuleLayer moduleLayer) {
        Set<String> unqualifiedExports = moduleLayer.modules()
            .stream()
            .flatMap(module -> module.getDescriptor().exports().stream())
            .filter(Predicate.not(ModuleDescriptor.Exports::isQualified))
            .map(ModuleDescriptor.Exports::source)
            .collect(Collectors.toSet());
        return moduleLayer.modules()
            .stream()
            .map(Module::getDescriptor)
            .filter(ModuleSupport::hasAtLeastOneUnqualifiedExport)
            .collect(
                Collectors.toMap(
                    ModuleDescriptor::name,
                    md -> md.provides()
                        .stream()
                        .map(ModuleDescriptor.Provides::service)
                        .filter(name -> unqualifiedExports.contains(packageName(name)))
                        .collect(Collectors.toSet())
                )
            );
    }

    static UberModuleClassLoader getInstance(ClassLoader parent, String moduleName, Set<URL> jarUrls) {
        return getInstance(parent, ModuleLayer.boot(), moduleName, jarUrls, Set.of());
    }

    @SuppressWarnings("removal")
    static UberModuleClassLoader getInstance(
        ClassLoader parent,
        ModuleLayer parentLayer,
        String moduleName,
        Set<URL> jarUrls,
        Set<String> moduleDenyList
    ) {
        Path[] jarPaths = jarUrls.stream().map(UberModuleClassLoader::urlToPathUnchecked).toArray(Path[]::new);
        var parentLayerModuleToServiceMap = getModuleToServiceMap(parentLayer);
        Set<String> requires = parentLayerModuleToServiceMap.keySet()
            .stream()
            .filter(Predicate.not(moduleDenyList::contains))
            .collect(Collectors.toSet());
        Set<String> uses = parentLayerModuleToServiceMap.entrySet()
            .stream()
            .filter(Predicate.not(entry -> moduleDenyList.contains(entry.getKey())))
            .flatMap(entry -> entry.getValue().stream())
            .collect(Collectors.toSet());

        ModuleFinder finder = ModuleSupport.ofSyntheticPluginModule(
            moduleName,
            jarPaths,
            requires,
            uses,
            s -> isPackageInLayers(s, parentLayer)
        );
        // TODO: check that denied modules are not brought as transitive dependencies (or switch to allow-list?)
        Configuration cf = parentLayer.configuration().resolve(finder, ModuleFinder.of(), Set.of(moduleName));

        Set<String> packageNames = finder.find(moduleName).map(ModuleReference::descriptor).map(ModuleDescriptor::packages).orElseThrow();

        PrivilegedAction<UberModuleClassLoader> pa = () -> new UberModuleClassLoader(
            parent,
            moduleName,
            jarUrls.toArray(new URL[0]),
            cf,
            parentLayer,
            packageNames
        );
        return AccessController.doPrivileged(pa);
    }

    private static boolean isPackageInLayers(String packageName, ModuleLayer moduleLayer) {
        if (moduleLayer.modules().stream().map(Module::getPackages).anyMatch(p -> p.contains(packageName))) {
            return true;
        }
        if (moduleLayer.parents().equals(List.of(ModuleLayer.empty()))) {
            return false;
        }
        return moduleLayer.parents().stream().anyMatch(ml -> isPackageInLayers(packageName, ml));
    }

    /**
     * Constructor
     */
    private UberModuleClassLoader(
        ClassLoader parent,
        String moduleName,
        URL[] jarURLs,
        Configuration cf,
        ModuleLayer mparent,
        Set<String> packageNames
    ) {
        super(parent);

        this.internalLoader = new URLClassLoader(jarURLs);
        // code source is always the first jar on the list
        this.codeSource = new CodeSource(jarURLs[0], (CodeSigner[]) null);
        // Defining a module layer tells the Java virtual machine about the
        // classes that may be loaded from the module, and is what makes the
        // Class::getModule call return the name of our ubermodule.
        this.moduleController = ModuleLayer.defineModules(cf, List.of(mparent), s -> this);
        this.module = this.moduleController.layer().findModule(moduleName).orElseThrow();

        this.packageNames = packageNames;
    }

    public ModuleLayer getLayer() {
        return moduleController.layer();
    }

    /**
     * @param moduleName
     *         The module name; or {@code null} to find the class in the
     *         {@linkplain #getUnnamedModule() unnamed module} for this
     *         class loader
     * @param name
     *         The <a href="#binary-name">binary name</a> of the class
     *
     * @return
     */
    @Override
    protected Class<?> findClass(String moduleName, String name) {
        if (Objects.isNull(moduleName) || this.module.getName().equals(moduleName) == false) {
            return null;
        }
        return findClass(name);
    }

    /**
     * @param name
     *          The <a href="#binary-name">binary name</a> of the class
     *
     * @return
     */
    @Override
    protected Class<?> findClass(String name) {
        String rn = name.replace('.', '/').concat(".class");

        try (InputStream in = internalLoader.getResourceAsStream(rn)) {
            if (in == null) {
                return null;
            }
            byte[] bytes = in.readAllBytes();
            return defineClass(name, bytes, 0, bytes.length, codeSource);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * This classloader does not restrict access to resources in its jars. Users should
     * expect the same behavior as that provided by {@link URLClassLoader}.
     *
     * @param moduleName Name of this classloader's synthetic module
     * @param name The resource name
     * @return a URL for the resource, or null if the resource could not be found,
     *   if the module name does not match, or if the loader is closed.
     */
    @Override
    protected URL findResource(String moduleName, String name) {
        if (Objects.isNull(moduleName) || this.module.getName().equals(moduleName) == false) {
            return null;
        }
        return findResource(name);
    }

    /**
     * This classloader does not restrict access to resources in its jars. Users should
     * expect the same behavior as that provided by {@link URLClassLoader}.
     *
     * @param name The resource name
     * @return a URL for the resource, or null if the resource could not be found,
     *   or if the loader is closed.
     */
    @Override
    protected URL findResource(String name) {
        return internalLoader.findResource(name);
    }

    /**
     * This classloader does not restrict access to resources in its jars. Users should
     * expect the same behavior as that provided by {@link URLClassLoader}.
     *
     * @param name The resource name
     * @return an Enumeration of URLs. If the loader is closed, the Enumeration contains no elements.
     */
    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
        return internalLoader.findResources(name);
    }

    @Override
    protected Class<?> loadClass(String cn, boolean resolve) throws ClassNotFoundException {
        // if cn's package is in this ubermodule, look here only (or just first?)
        String packageName = packageName(cn);

        // TODO: do we need to check the security manager here?

        synchronized (getClassLoadingLock(cn)) {

            // check if already loaded
            Class<?> c = findLoadedClass(cn);

            if (c == null) {
                if (this.packageNames.contains(packageName)) {
                    // find in module or null
                    c = findClass(cn);
                } else {
                    c = getParent().loadClass(cn);
                }
            }

            if (c == null) {
                throw new ClassNotFoundException(cn);
            }

            if (resolve) {
                resolveClass(c);
            }

            return c;
        }
    }

    // For testing in cases where code must be given access to an unnamed module
    void addReadsSystemClassLoaderUnnamedModule() {
        moduleController.layer()
            .modules()
            .forEach(module -> moduleController.addReads(module, ClassLoader.getSystemClassLoader().getUnnamedModule()));
    }

    /**
     * Returns the package name for the given class name
     */
    private static String packageName(String cn) {
        int pos = cn.lastIndexOf('.');
        return (pos < 0) ? "" : cn.substring(0, pos);
    }

    @SuppressForbidden(reason = "plugin infrastructure provides URLs but module layer uses Paths")
    static Path urlToPathUnchecked(URL url) {
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    @SuppressWarnings("removal")
    public void close() throws Exception {
        PrivilegedAction<Void> pa = () -> {
            try {
                internalLoader.close();
            } catch (IOException e) {
                throw new IllegalStateException("Could not close internal URLClassLoader");
            }
            return null;
        };
        AccessController.doPrivileged(pa);
    }

    // visible for testing
    public URLClassLoader getInternalLoader() {
        return internalLoader;
    }
}
