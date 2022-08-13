/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.io.IOException;
import java.io.InputStream;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
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
import java.util.Set;

/**
 * This classloader will load classes from non-modularized stable plugins. Fully
 * modularized plugins can be loaded using
 * {@link java.lang.module.ModuleFinder}.
 * <p>
 * If the stable plugin is not modularized, a synthetic module will be created
 * for all of the jars in the bundle. We want to be able to construct the
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
 * TODO:
 *   * We need a loadClass method
 *   * We need a findResources method
 *   * We need some tests
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

    private ModuleReference module;
    private URLClassLoader internalLoader;
    private CodeSource codeSource;
    private ModuleLayer.Controller moduleController;

    // TODO: we should take multiple jars
    static StablePluginClassLoader getInstance(ClassLoader parent, Path jar) {
        // TODO: we should take jars, not a module reference
        // TODO: delegate reading from jars to a URL class loader
        PrivilegedAction<StablePluginClassLoader> pa = () -> new StablePluginClassLoader(parent, jar);
        return AccessController.doPrivileged(pa);
    }
    /**
     * Constructor
     *
     * The constructors for URLClassLoader and jdk.internal.loader.Loader take or construct an object that does the
     * heavy lifting. jdk.internal.loader.Loader takes ResolvedModules for its argument, while URLClassLoader constructs
     * a URLClassPath object. The AppClassLoader, on the other hand, loads modules as it goes.
     *
     * First cut: single jar only, modularized or not
     * Second cut: main jar plus dependencies (can we have modularized main jar with unmodularized dependencies?)
     * Third cut: link it up with the "crate/bundle" descriptor
     */
    public StablePluginClassLoader(ClassLoader parent, Path jar) {
        super(parent);
        ModuleFinder finder = ModuleSupport.ofSyntheticPluginModule("synthetic", new Path[]{jar}, Set.of());
        // TODO: here, we need to figure out if our jar/jars are modules are not
        try {
            this.internalLoader = new URLClassLoader(new URL[]{jar.toUri().toURL()});
            this.module = finder.find("synthetic").orElseThrow();
            this.codeSource = new CodeSource(jar.toUri().toURL(), (CodeSigner[]) null);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        // load it with a module
        ModuleLayer mparent = ModuleLayer.boot();
        Configuration cf = mparent.configuration().resolve(finder, ModuleFinder.of(), Set.of("synthetic"));
        PrivilegedAction<ModuleLayer.Controller> pa =
            () -> ModuleLayer.defineModules(cf, List.of(mparent), s -> this);
        // this will bind the module to the classloader, I hope...
        this.moduleController = AccessController.doPrivileged(pa);
    }

    private static ModuleReference buildSyntheticModule(Path jar) {
        return ModuleSupport.ofSyntheticPluginModule("synthetic", new Path[]{jar}, Set.of()).find("synthetic").orElseThrow();
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
    public Class<?> findClass(String moduleName, String name) {
        // built-in classloader:
        // 1. if we have a module name, we get the package name and look up the module,
        //      then load from the module by calling define class with the name and module
        // 2. if there's no moduleName, search on the classpath
        // The system has a map of packages to modules that it gets by loading the module
        // layer -- do we have this for our plugin loading?
        //
        // URLClassLoader does not implement this
        //
        // jdk.internal.loader.Loader can pull a class out of a loaded module pretty easily;
        // ModuleReference does a lot of the work
        // TODO: implement
        return null;
    }

    /**
     * @param name
     *          The <a href="#binary-name">binary name</a> of the class
     *
     * @return
     */
    @Override
    public Class<?> findClass(String name) {
        // built-in classloaders:
        // try to find a module for the class name by looking up package, then do what
        //   findClass(String, String) does
        //
        // URLClassLoader is constructed with a list of classpaths, and searches those classpaths
        // for resources. If it finds a .class file, it calls defineClass
        //
        // jdk.internal.loader.Loader can pull a class out of a loaded module pretty easily;
        // ModuleReference does a lot of the work

        String rn = name.replace('.', '/').concat(".class");

        try (InputStream in = internalLoader.getResourceAsStream(rn)) {
            // TODO: null handling
            byte[] bytes = in.readAllBytes();
            return defineClass(name, bytes, 0, bytes.length, codeSource);
        } catch (IOException e){
            // TODO
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param name The resource name
     * @return
     */
    @Override
    protected URL findResource(String moduleName, String name) {
        // built-in classloaders:
        // look up module with package name, then find resource on module reference
        // if no module defined, then find on classpath. Finding on classpath involves
        // using URLClassPath, which we can't use because it's in jdk.internal.
        //
        // URLClassLoader does not implement this
        //
        // jdk.internal.loader.Loader uses a ModuleReader for the ModuleReference class,
        // searching the current module, then other modules, and checking access levels

        // TODO: find resource, but check module name?
        return null;
    }

    /**
     * @param name The resource name
     * @return
     */
    @Override
    protected URL findResource(String name) {
        // URLClassLoader basically delegates to URLClassPath, which handles access to jars and files
        //
        // jdk.internal.loader.Loader uses a ModuleReader for the ModuleReference class,
        // searching the current module, then other modules, and checking access levels

        return internalLoader.findResource(name);
    }

    @Override
    protected Enumeration<URL> findResources(String name) {
        // URLClassLoader also delegates here
        //
        // jdk.internal.loader.Loader uses a ModuleReader for the ModuleReference class,
        // searching the current module, then other modules, and checking access levels

        // TODO - how do we do this?
        return null;
    }
}
