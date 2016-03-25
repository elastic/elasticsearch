/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.AuthenticationModule;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Arrays;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

/**
 *
 */
public class XPackExtensionsService {
    private final Settings settings;

    /**
     * We keep around a list of extensions
     */
    private final List<Tuple<XPackExtensionInfo, XPackExtension> > extensions;

    /**
     * Constructs a new XPackExtensionsService
     * @param settings The settings of the system
     * @param extsDirectory The directory extensions exist in, or null if extensions should not be loaded from the filesystem
     * @param classpathExtensions Extensions that exist in the classpath which should be loaded
     */
    public XPackExtensionsService(Settings settings, Path extsDirectory, Collection<Class<? extends XPackExtension>> classpathExtensions) {
        this.settings = settings;
        List<Tuple<XPackExtensionInfo, XPackExtension>> extensionsLoaded = new ArrayList<>();

        // first we load extensions that are on the classpath. this is for tests
        for (Class<? extends XPackExtension> extClass : classpathExtensions) {
            XPackExtension ext = loadExtension(extClass, settings);
            XPackExtensionInfo extInfo = new XPackExtensionInfo(ext.name(), ext.description(), "NA", extClass.getName());
            extensionsLoaded.add(new Tuple<>(extInfo, ext));
        }

        // now, find all the ones that are in plugins/xpack/extensions
        if (extsDirectory != null) {
            try {
                List<Bundle> bundles = getExtensionBundles(extsDirectory);
                List<Tuple<XPackExtensionInfo, XPackExtension>> loaded = loadBundles(bundles);
                extensionsLoaded.addAll(loaded);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize extensions", ex);
            }
        }
        extensions = Collections.unmodifiableList(extensionsLoaded);
    }

    public void onModule(AuthenticationModule module) {
        for (Tuple<XPackExtensionInfo, XPackExtension> tuple : extensions) {
            tuple.v2().onModule(module);
        }
    }

    // a "bundle" is a an extension in a single classloader.
    static class Bundle {
        XPackExtensionInfo info;
        List<URL> urls = new ArrayList<>();
    }

    static List<Bundle> getExtensionBundles(Path extsDirectory) throws IOException {
        ESLogger logger = Loggers.getLogger(XPackExtensionsService.class);

        // TODO: remove this leniency, but tests bogusly rely on it
        if (!isAccessibleDirectory(extsDirectory, logger)) {
            return Collections.emptyList();
        }

        List<Bundle> bundles = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(extsDirectory)) {
            for (Path extension : stream) {
                if (FileSystemUtils.isHidden(extension)) {
                    logger.trace("--- skip hidden extension file[{}]", extension.toAbsolutePath());
                    continue;
                }
                logger.trace("--- adding extension [{}]", extension.toAbsolutePath());
                final XPackExtensionInfo info;
                try {
                    info = XPackExtensionInfo.readFromProperties(extension);
                } catch (IOException e) {
                    throw new IllegalStateException("Could not load extension descriptor for existing extension ["
                            + extension.getFileName() + "]. Was the extension built before 2.0?", e);
                }

                List<URL> urls = new ArrayList<>();
                try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(extension, "*.jar")) {
                    for (Path jar : jarStream) {
                        // normalize with toRealPath to get symlinks out of our hair
                        urls.add(jar.toRealPath().toUri().toURL());
                    }
                }
                final Bundle bundle = new Bundle();
                bundles.add(bundle);
                bundle.info = info;
                bundle.urls.addAll(urls);
            }
        }

        return bundles;
    }

    private List<Tuple<XPackExtensionInfo, XPackExtension> > loadBundles(List<Bundle> bundles) {
        List<Tuple<XPackExtensionInfo, XPackExtension>> exts = new ArrayList<>();

        for (Bundle bundle : bundles) {
            // jar-hell check the bundle against the parent classloader and the x-pack classloader
            // pluginmanager does it, but we do it again, in case lusers mess with jar files manually
            try {
                final List<URL> jars = new ArrayList<>();
                // add the parent jars to the list
                jars.addAll(Arrays.asList(JarHell.parseClassPath()));

                // add the x-pack jars to the list
                ClassLoader xpackLoader = getClass().getClassLoader();
                // this class is loaded from the isolated x-pack plugin's classloader
                if (xpackLoader instanceof URLClassLoader) {
                    jars.addAll(Arrays.asList(((URLClassLoader) xpackLoader).getURLs()));
                }

                jars.addAll(bundle.urls);

                JarHell.checkJarHell(jars.toArray(new URL[0]));
            } catch (Exception e) {
                throw new IllegalStateException("failed to load bundle " + bundle.urls + " due to jar hell", e);
            }

            // create a child to load the extension in this bundle
            ClassLoader loader = URLClassLoader.newInstance(bundle.urls.toArray(new URL[0]), getClass().getClassLoader());
            final Class<? extends XPackExtension> extClass = loadExtensionClass(bundle.info.getClassname(), loader);
            final XPackExtension ext = loadExtension(extClass, settings);
            exts.add(new Tuple<>(bundle.info, ext));
        }

        return Collections.unmodifiableList(exts);
    }

    private Class<? extends XPackExtension> loadExtensionClass(String className, ClassLoader loader) {
        try {
            return loader.loadClass(className).asSubclass(XPackExtension.class);
        } catch (ClassNotFoundException e) {
            throw new ElasticsearchException("Could not find extension class [" + className + "]", e);
        }
    }

    private XPackExtension loadExtension(Class<? extends XPackExtension> extClass, Settings settings) {
        try {
            try {
                return extClass.getConstructor(Settings.class).newInstance(settings);
            } catch (NoSuchMethodException e) {
                try {
                    return extClass.getConstructor().newInstance();
                } catch (NoSuchMethodException e1) {
                    throw new ElasticsearchException("No constructor for [" + extClass + "]. An extension class must " +
                            "have either an empty default constructor or a single argument constructor accepting a " +
                            "Settings instance");
                }
            }
        } catch (Throwable e) {
            throw new ElasticsearchException("Failed to load extension class [" + extClass.getName() + "]", e);
        }
    }
}
