/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

/**
 * Runs LoggerUsageCheck on a set of directories.
 */
@CacheableTask
public abstract class TransportTestExistTask extends PrecommitTask {

    private FileCollection classpath;

    private final ListProperty<FileCollection> classesDirs;

    private ObjectFactory objectFactory;
    private FileCollection mainSources;
    private FileCollection testSources;
    private Configuration compileClasspath;
    private Configuration testClasspath;
    Set<String> skipClasses = new HashSet<>();

    @Inject
    public TransportTestExistTask(ObjectFactory objectFactory) {
        this.classesDirs = objectFactory.listProperty(FileCollection.class);
        this.objectFactory = objectFactory;
        setDescription("Runs TransportTestExistTask on output directories of all source sets");
    }

    @Inject
    abstract public WorkerExecutor getWorkerExecutor();

    @TaskAction
    public void runLoggerUsageTask() {
        WorkQueue workQueue = getWorkerExecutor().noIsolation();
        workQueue.submit(TransportTestExistWorkAction.class, parameters -> {
            parameters.getMainSources().setFrom(mainSources);
            parameters.getTestSources().setFrom(testSources);
            parameters.getCompileClasspath().setFrom(compileClasspath);
            parameters.getTestClasspath().setFrom(testClasspath);
            parameters.getSkipClasses().set(skipClasses);
        });
    }

    public void setMainSources(FileCollection mainSources) {
        this.mainSources = mainSources;
    }

    public void setTestSources(FileCollection testSources) {
        this.testSources = testSources;
    }

    public void setCompileClasspath(Configuration compileClasspath) {
        this.compileClasspath = compileClasspath;
    }

    public void setTestClasspath(Configuration testClasspath) {
        this.testClasspath = testClasspath;
    }

    public void skipMissingTransportTest(String className, String reason) {
        skipClasses.add(classNameToPath(className));
    }

    public void skipTest(String className, String reason) {
        skipClasses.add(classNameToPath(className));
    }

    private String classNameToPath(String className) {
        return className.replace('.', File.separatorChar);
    }

    abstract static class TransportTestExistWorkAction implements WorkAction<Parameters> {

        private final Logger logger = Logging.getLogger(TransportTestExistTask.class);
        private static final String MODULE_INFO = "module-info.class";

        private final ExecOperations execOperations;

        @Inject
        public TransportTestExistWorkAction(ExecOperations execOperations) {
            this.execOperations = execOperations;
        }

        @Override
        public void execute() {
            URL[] allUrls = allClasses();
            ClassLoader cl = new URLClassLoader(allUrls);

            String writeableClassName = "org.elasticsearch.common.io.stream.Writeable";
            Class<?> writeableClass = loadClass(cl, writeableClassName);

            Set<String> transportClasses = traverseClassesInRoots(getParameters().getMainSources()).stream()
                .filter(n -> isSubclassOf(n, cl, Set.of(writeableClass)))
                .collect(Collectors.toSet());

            Set<Class<?>> transportTestRootClasses = Set.of(
                loadClass(cl, "org.elasticsearch.test.AbstractWireTestCase"),
                loadClass(cl, "org.elasticsearch.test.AbstractQueryTestCase"),
                loadClass(cl, "org.elasticsearch.search.aggregations.BaseAggregationTestCase"),
                loadClass(cl, "org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase"),
                loadClass(cl, "org.elasticsearch.test.AbstractQueryVectorBuilderTestCase")
            );
            Set<String> transportTests = traverseClassesInRoots(getParameters().getTestSources()).stream()
                .filter(n -> isSubclassOf(n, cl, transportTestRootClasses))
                .collect(Collectors.toSet());

            Set<String> missingTestClasses = findMissingTestClasses(transportClasses, transportTests);

            if (missingTestClasses.size() > 0) {
                throw new GradleException(
                    "There are "
                        + missingTestClasses.size()
                        + " missing tests for classes\n"
                        + "tasks.named(\"transportTestExistCheck\").configure { task ->\n"
                        + missingTestClasses.stream()
                            .map(s -> "task.skipMissingTransportTest(\"" + s + "\",\"missing test\")")
                            .collect(Collectors.joining("\n"))
                        + "\n}"
                );
            }
        }

        private Set<String> findMissingTestClasses(Set<String> transportClasses, Set<String> transportTests) {
            Set<String> missingTestClasses = new HashSet<>();
            for (String c : transportClasses) {
                var name = getClassName(c);
                var nameToLook = name.contains("$") ? name.substring(0, name.indexOf('$')) : name;
                Optional<String> found = transportTests.stream().filter(tt -> tt.contains(nameToLook)).findAny();
                if (found.isEmpty()) {
                    missingTestClasses.add(c);
                } else {
                    System.out.println("Found test " + found.get() + " for class " + c);
                }
            }
            missingTestClasses.stream().forEach(s -> System.out.println(s));
            return missingTestClasses;
        }

        private URL[] allClasses() {
            Set<URL> mainClasses = getURLs(getParameters().getMainSources());
            Set<URL> testClasses = getURLs(getParameters().getTestSources());

            Set<URL> compileClassPath = getURLs(getParameters().getCompileClasspath());
            Set<URL> testClassPath = getURLs(getParameters().getTestClasspath());

            URL[] allUrls = Stream.of(mainClasses, testClasses, compileClassPath, testClassPath).flatMap(Set::stream).toArray(URL[]::new);
            return allUrls;
        }

        private boolean isSubclassOf(String name, ClassLoader cl, Set<Class<?>> rootClasses) {
            try {
                Class<?> clazz = Class.forName(name, false, cl);
                return rootClasses.stream().filter(c -> c.isAssignableFrom(clazz)).findAny().isPresent();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        private Set<String> traverseClassesInRoots(ConfigurableFileCollection roots) {
            return roots.getFiles().stream().map(f -> {
                try (var stream = Files.walk(Path.of(f.toString()))) {
                    return stream.filter(Files::isRegularFile)
                        .filter(p -> p.toString().endsWith(".class"))
                        .filter(p -> p.toString().endsWith(MODULE_INFO) == false)
                        .filter(p -> p.toString().startsWith("/META-INF") == false)// skip multi-release files
                        .map(p -> getNameFromFilePath(p.toString()))
                        .collect(Collectors.toList());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).flatMap(List::stream).collect(Collectors.toSet());
        }

        private Set<URL> getURLs(ConfigurableFileCollection mainSources1) {
            return mainSources1.getFiles().stream().map(f -> {
                try {
                    return f.toURI().toURL();
                } catch (MalformedURLException e) {
                    return null;
                }
            }).filter(f -> f != null).collect(Collectors.toSet());
        }

        private static String getNameFromFilePath(String file1) {
            String file = file1.split("java\\/(main|test)")[1].substring(1);
            String withoutDotClass = file.substring(0, file.lastIndexOf("."));
            return withoutDotClass.replace('/', '.');
        }

        private String getClassName(String transportClassWithPackage) {
            return transportClassWithPackage.substring(transportClassWithPackage.lastIndexOf('.') + 1);
        }

        private static Class<?> loadClass(ClassLoader cl, String writeableClassName) {
            Class<?> writeableClass = null;
            try {
                writeableClass = Class.forName(writeableClassName, false, cl);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return writeableClass;
        }
    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getMainSources();

        ConfigurableFileCollection getTestSources();

        ConfigurableFileCollection getCompileClasspath();

        ConfigurableFileCollection getTestClasspath();

        SetProperty<String> getSkipClasses();
    }

}
