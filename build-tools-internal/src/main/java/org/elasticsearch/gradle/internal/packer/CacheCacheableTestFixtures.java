/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.packer;

import org.gradle.api.DefaultTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.TaskAction;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.inject.Inject;

public abstract class CacheCacheableTestFixtures extends DefaultTask {

    @CompileClasspath
    public abstract ConfigurableFileCollection getClasspath();

    @Inject
    public abstract WorkerExecutor getWorkerExecutor();

    /**
     * Executes the forbidden apis task.
     */
    @TaskAction
    public void checkForbidden() {
        WorkQueue workQueue = getWorkerExecutor().classLoaderIsolation(spec -> spec.getClasspath().from(getClasspath()));
        workQueue.submit(CacheTestFixtureWorkAction.class, params -> params.getClasspath().setFrom(getClasspath()));
    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getClasspath();
    }

    abstract static class CacheTestFixtureWorkAction implements WorkAction<Parameters> {

        @Inject
        @SuppressWarnings("checkstyle:RedundantModifier")
        public CacheTestFixtureWorkAction() {}

        @Override
        public void execute() {
            final URLClassLoader urlLoader = createClassLoader(getParameters().getClasspath());
            try {
                Reflections reflections = new Reflections(
                    new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("org.elasticsearch.test.fixtures"))
                        .setScanners(new SubTypesScanner())
                );

                Class<?> ifClass = Class.forName("org.elasticsearch.test.fixtures.CacheableTestFixture");
                Set<Class<?>> classes = (Set<Class<?>>) reflections.getSubTypesOf(ifClass);

                for (Class<?> cacheableTestFixtureClazz : classes) {
                    if (Modifier.isAbstract(cacheableTestFixtureClazz.getModifiers()) == false) {
                        Constructor<?> declaredConstructor = cacheableTestFixtureClazz.getDeclaredConstructor();
                        declaredConstructor.setAccessible(true);
                        Object o = declaredConstructor.newInstance();
                        Method cacheMethod = cacheableTestFixtureClazz.getMethod("cache");
                        System.out.println("Caching resources from " + cacheableTestFixtureClazz.getName());
                        cacheMethod.invoke(o);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                // Close the classloader to free resources:
                try {
                    if (urlLoader != null) urlLoader.close();
                } catch (IOException ioe) {
                    // getLogger().warn("Cannot close classloader: ".concat(ioe.toString()));
                }
            }
        }

        private URLClassLoader createClassLoader(FileCollection classpath) {
            if (classpath == null) {
                throw new InvalidUserDataException("Missing 'classesDirs' or 'classpath' property.");
            }

            final Set<File> cpElements = new LinkedHashSet<>();
            cpElements.addAll(classpath.getFiles());
            final URL[] urls = new URL[cpElements.size()];
            try {
                int i = 0;
                for (final File cpElement : cpElements) {
                    urls[i++] = cpElement.toURI().toURL();
                }
                assert i == urls.length;
            } catch (MalformedURLException mfue) {
                throw new InvalidUserDataException("Failed to build classpath URLs.", mfue);
            }

            return URLClassLoader.newInstance(urls, ClassLoader.getSystemClassLoader());
        }

    }
}
