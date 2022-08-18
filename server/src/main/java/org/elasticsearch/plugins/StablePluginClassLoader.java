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
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.CodeSigner;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * This classloader will load classes from non-modularized stable plugins. Fully
 * modularized plugins can be loaded using
 * {@link java.lang.module.ModuleFinder}.
 * <p>
 * If the stable plugin is not modularized, a synthetic module will be created
 * for all jars in the bundle. We want to be able to construct the
 * "requires" relationships for this synthetic module, which will make it
 * different from the unnamed (classpath) module.
 * <p>
 * We can delegate to a URLClassLoader, which is battle-tested when it comes
 * to reading classes out of jars.
 * <p>
 * This classloader needs to avoid parent-first search: we'll check classes
 * against a list of packages in this synthetic module, and load a class
 * directly if it's part of this synthetic module. This will keep libraries from
 * clashing.
 *
 * Resources:
 *   * {@link java.lang.ClassLoader}
 *   * {/@link jdk.internal.loader.BuiltinClassLoader} - boot, platform, and app loaders inherit from this
 *   * {/@link jdk.internal.loader.ClassLoaders.BootClassLoader} - see also {/@link jdk.internal.loader.BootLoader}
 *   * {/@link jdk.internal.loader.ClassLoaders.PlatformClassLoader}
 *   * {/@link jdk.internal.loader.ClassLoaders.AppClassLoader}
 *   * {@link java.security.SecureClassLoader} - user-facing classloaders inherit from this
 *   * {@link java.net.URLClassLoader} - loads classes from classpath jars
 *   * {/@link jdk.internal.loader.Loader} - loads classes from modules
 *   * {/@link org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader} - one of our custom classloaders
 *   * {@link java.lang.module.ModuleDescriptor} - how you build a module (for an ubermodule)
 *   * <a href="https://github.com/elastic/elasticsearch/pull/88216/files">WIP PR for ubermodule classloader</a>
 *
 * Alternate name ideas
 *   * CrateClassLoader, because we may use the crate metaphor with our plugins
 *   * UberModuleClassLoader, but this only describes one of the code paths
 *   * UberModuleURLClassLoader
 *   * ModularizingClassLoader
 *   * NamedModuleClassLoader, because it will always load into a named module (via module info, or via synthetic module)
 */
public class StablePluginClassLoader extends SecureClassLoader {

    private final Module module;
    private final URLClassLoader internalLoader;
    private final CodeSource codeSource;
    private final ModuleLayer.Controller moduleController;
    private final Set<String> packageNames;

    static StablePluginClassLoader getInstance(ClassLoader parent, List<Path> jarPaths) {
        return getInstance(parent, jarPaths, Set.of());
    }

    @SuppressWarnings("removal")
    static StablePluginClassLoader getInstance(ClassLoader parent, List<Path> jarPaths, Set<String> moduleDenyList) {
        PrivilegedAction<StablePluginClassLoader> pa = () -> new StablePluginClassLoader(parent, jarPaths, moduleDenyList);
        return AccessController.doPrivileged(pa);
    }

    /**
     * Constructor
     *
     * The constructors for URLClassLoader and jdk.internal.loader.Loader take or construct an object that does the
     * heavy lifting. jdk.internal.loader.Loader takes ResolvedModules for its argument, while URLClassLoader constructs
     * a URLClassPath object. The AppClassLoader, on the other hand, loads modules as it goes.
     *
     * First cut: single jar only
     * Second cut: main jar plus dependencies (can we have modularized main jar with unmodularized dependencies?)
     * Third cut: link it up with the "crate/bundle" descriptor
     *
     * TODO: consider a factory pattern for more specific exception handling for scan, etc.
     */
    @SuppressForbidden(reason = "need access to the jar file")
    public StablePluginClassLoader(ClassLoader parent, List<Path> jarPaths, Set<String> moduleDenyList) {
        super(parent);

        // TODO: module name should be derived from plugin descriptor (plugin names should be distinct, might
        // need to munge the characters a little)
        ModuleFinder finder = ModuleSupport.ofSyntheticPluginModule("synthetic", jarPaths.toArray(new Path[0]), Set.of());
        try {
            List<URL> jarURLs = new ArrayList<>();
            for (Path jarPath : jarPaths) {
                URI toUri = jarPath.toUri();
                URL toURL = toUri.toURL();
                jarURLs.add(toURL);
            }
            this.internalLoader = new URLClassLoader(jarURLs.toArray(new URL[0]));
            // we'll need to make up a code source for multiple jar files
            this.codeSource = new CodeSource(jarURLs.get(0), (CodeSigner[]) null);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        // we need a module layer to bind our module to this classloader
        ModuleLayer mparent = ModuleLayer.boot();
        Configuration cf = mparent.configuration().resolve(finder, ModuleFinder.of(), Set.of("synthetic"));
        this.moduleController = ModuleLayer.defineModules(cf, List.of(mparent), s -> this);

        this.module = this.moduleController.layer().findModule("synthetic").orElseThrow();

        // Every module reads java.base by default, but we can add all other modules
        // that are not in the deny list so that plugins can use, for example, java.management
        mparent.modules()
            .stream()
            .filter(Module::isNamed)
            .filter(m -> "java.base".equals(m.getName()) == false)
            .filter(m -> moduleDenyList.contains(m.getName()) == false)
            .forEach(m -> moduleController.addReads(module, m));

        // open this as versioned jar - most recent version of class file
        // TODO: verify behavior w/ versioned jar, c.f. EmbeddedImplClassLoader
        this.packageNames = new HashSet<>();
        for (Path jar : jarPaths) {
            try (JarFile jarFile = new JarFile(jar.toFile())) {
                Set<String> jarPackages = ModuleSupport.scan(jarFile)
                    .classFiles()
                    .stream()
                    .map(e -> ModuleSupport.toPackageName(e, "/"))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toSet());

                // TODO: We could enforce "split packages" on our jars here, but
                // that seems too restrictive
                this.packageNames.addAll(jarPackages);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
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
        // built-in classloader:
        // 1. if we have a module name, we get the package name and look up the module,
        // then load from the module by calling define class with the name and module
        // 2. if there's no moduleName, search on the classpath
        // The system has a map of packages to modules that it gets by loading the module
        // layer -- do we have this for our plugin loading?
        //
        // URLClassLoader does not implement this
        //
        // jdk.internal.loader.Loader can pull a class out of a loaded module pretty easily;
        // ModuleReference does a lot of the work
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
        // built-in classloaders:
        // try to find a module for the class name by looking up package, then do what
        // findClass(String, String) does
        //
        // URLClassLoader is constructed with a list of classpaths, and searches those classpaths
        // for resources. If it finds a .class file, it calls defineClass
        //
        // jdk.internal.loader.Loader can pull a class out of a loaded module pretty easily;
        // ModuleReference does a lot of the work

        String rn = name.replace('.', '/').concat(".class");

        try (InputStream in = internalLoader.getResourceAsStream(rn)) {
            if (in == null) {
                // TODO: we can check package name to know whether or not the resource
                // should definitely be in this module's packages or not
                // i.e. we can throw class not found if the resource's package is owned by this
                // classloader's module
                return null;
            }
            byte[] bytes = in.readAllBytes();
            return defineClass(name, bytes, 0, bytes.length, codeSource);
        } catch (IOException e) {
            // TODO
            throw new IllegalStateException(e);
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

            if (c == null) throw new ClassNotFoundException(cn);

            if (resolve) {
                resolveClass(c);
            }

            return c;
        }
    }

    /**
     * Returns the package name for the given class name
     * (from jdk.internal.loader.Loader)
     */
    private String packageName(String cn) {
        int pos = cn.lastIndexOf('.');
        return (pos < 0) ? "" : cn.substring(0, pos);
    }

}
