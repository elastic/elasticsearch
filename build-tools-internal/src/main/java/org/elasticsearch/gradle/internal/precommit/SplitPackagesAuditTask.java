/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkerExecutor;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Checks for split packages with dependencies. These are not allowed in a future modularized world.
 */
public class SplitPackagesAuditTask extends DefaultTask {

    private static final Logger LOGGER = Logging.getLogger(SplitPackagesAuditTask.class);

    private final WorkerExecutor workerExecutor;
    private FileCollection classpath;
    private final SetProperty<File> srcDirs;
    private final SetProperty<String> ignoreClasses;
    private final RegularFileProperty markerFile;

    @Inject
    public SplitPackagesAuditTask(WorkerExecutor workerExecutor, ObjectFactory objectFactory) {
        this.workerExecutor = workerExecutor;
        this.srcDirs = objectFactory.setProperty(File.class);
        this.ignoreClasses = objectFactory.setProperty(String.class);
        this.markerFile = objectFactory.fileProperty();

        this.markerFile.set(getProject().getLayout().getBuildDirectory().file("markers/" + this.getName() + ".marker"));
    }

    @TaskAction
    public void auditSplitPackages() {
        workerExecutor.noIsolation().submit(SplitPackagesAuditAction.class, params -> {
            params.getProjectPath().set(getProject().getPath());
            params.getProjectBuildDirs().set(getProjectBuildDirs());
            params.getClasspath().from(classpath);
            params.getSrcDirs().set(srcDirs);
            params.getIgnoreClasses().set(ignoreClasses);
            params.getMarkerFile().set(markerFile);
        });
    }

    private Map<File, String> getProjectBuildDirs() {
        // while this is done in every project, it should be cheap to calculate
        Map<File, String> buildDirs = new HashMap<>();
        for (Project project : getProject().getRootProject().getAllprojects()) {
            buildDirs.put(project.getBuildDir(), project.getPath());
        }
        return buildDirs;
    }

    @CompileClasspath
    public FileCollection getClasspath() {
        return classpath.filter(File::exists);
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    @InputFiles
    @SkipWhenEmpty
    @PathSensitive(PathSensitivity.RELATIVE)
    public SetProperty<File> getSrcDirs() {
        return srcDirs;
    }

    @Input
    public SetProperty<String> getIgnoreClasses() {
        return ignoreClasses;
    }

    /**
     * Add classes that exist in split packages but should be ignored.
     */
    public void ignoreClasses(String... classes) {
        for (String classname : classes) {
            ignoreClasses.add(classname);
        }
    }

    @OutputFile
    public RegularFileProperty getMarkerFile() {
        return markerFile;
    }

    public abstract static class SplitPackagesAuditAction implements WorkAction<Parameters> {
        @Override
        public void execute() {
            final Parameters parameters = getParameters();
            final String projectPath = parameters.getProjectPath().get();

            // First determine all the packages that exist in the dependencies. There might be
            // split packages across the dependencies, which is "ok", in that we don't care
            // about it for the purpose of this project, that split will be detected in
            // the other project
            Map<String, List<File>> dependencyPackages = getDependencyPackages();

            // Next read each of the source directories and find if we define any package directories
            // that match those in our dependencies.
            Map<String, Set<String>> splitPackages = findSplitPackages(dependencyPackages.keySet());

            // Then filter out any known split packages/classes that we want to ignore.
            filterSplitPackages(splitPackages);

            // Finally, print out (and fail) if we have any split packages
            for (var entry : splitPackages.entrySet()) {
                String packageName = entry.getKey();
                List<File> deps = dependencyPackages.get(packageName);
                List<String> msg = new ArrayList<>();
                msg.add("Project " + projectPath + " defines classes in package " + packageName + " exposed by dependencies");
                msg.add("  Dependencies:");
                deps.forEach(f -> msg.add("    " + formatDependency(f)));
                msg.add("  Classes:");
                entry.getValue().forEach(c -> msg.add("    '" + c + "',"));
                LOGGER.error(String.join(System.lineSeparator(), msg));
            }
            if (splitPackages.isEmpty() == false) {
                throw new GradleException("Verification failed: Split packages found! See errors above for details.\n" +
                    "DO NOT ADD THESE SPLIT PACKAGES TO THE IGNORE LIST! Choose a new package name for the classes added.");
            }

            try {
                Files.write(parameters.getMarkerFile().getAsFile().get().toPath(), new byte[] {}, StandardOpenOption.CREATE);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create marker file", e);
            }
        }

        private Map<String, List<File>> getDependencyPackages() {
            Map<String, List<File>> packages = new HashMap<>();
            for (File classpathElement : getParameters().getClasspath().getFiles()) {
                for (String packageName : readPackages(classpathElement)) {
                    packages.computeIfAbsent(packageName, k -> new ArrayList<>()).add(classpathElement);
                }
            }
            if (LOGGER.isInfoEnabled()) {
                List<String> msg = new ArrayList<>();
                msg.add("Packages from dependencies:");
                packages.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> msg.add("  -" + e.getKey() + " -> " + e.getValue()));
                LOGGER.info(String.join(System.lineSeparator(), msg));
            }
            return packages;
        }

        private Map<String, Set<String>> findSplitPackages(Set<String> dependencyPackages) {
            Map<String, Set<String>> splitPackages = new HashMap<>();
            for (File srcDir : getParameters().getSrcDirs().get()) {
                try {
                    walkJavaFiles(srcDir.toPath(), ".java", path -> {
                        String packageName = getPackageName(path);
                        String className = path.subpath(path.getNameCount() - 1, path.getNameCount()).toString();
                        className = className.substring(0, className.length() - ".java".length());
                        LOGGER.info("Inspecting " + path + System.lineSeparator()
                            + "  package: " + packageName + System.lineSeparator()
                            + "  class: " + className);
                        if (dependencyPackages.contains(packageName)) {
                            splitPackages.computeIfAbsent(packageName, k -> new TreeSet<>()).add(packageName + "." + className);
                        }
                    });
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            if (LOGGER.isInfoEnabled()) {
                List<String> msg = new ArrayList<>();
                msg.add("Split packages:");
                splitPackages.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> msg.add("  -" + e.getKey() + " -> " + e.getValue()));
                LOGGER.info(String.join(System.lineSeparator(), msg));
            }
            return splitPackages;
        }

        private void filterSplitPackages(Map<String, Set<String>> splitPackages) {
            String lastPackageName = null;
            Set<String> currentClasses = null;
            boolean filterErrorsFound = false;
            for (String fqcn : getParameters().getIgnoreClasses().get().stream().sorted().collect(Collectors.toList())) {
                int lastDot = fqcn.lastIndexOf('.');
                if (lastDot == -1) {
                    LOGGER.error("Missing package in classname in split package ignores: " + fqcn);
                    filterErrorsFound = true;
                    continue;
                }
                String packageName = fqcn.substring(0, lastDot);
                String className = fqcn.substring(lastDot + 1);
                LOGGER.info("IGNORING package: " + packageName + ", class: " + className);
                if (packageName.equals(lastPackageName) == false) {
                    currentClasses = splitPackages.get(packageName);
                    lastPackageName = packageName;
                }

                if (currentClasses == null) {
                    LOGGER.error("Package is not split: " + fqcn);
                    filterErrorsFound = true;
                } else {
                    if (className.equals("*")) {
                        currentClasses.clear();
                    } else if (currentClasses.remove(fqcn) == false) {
                        LOGGER.error("Class does not exist: " + fqcn);
                        filterErrorsFound = true;
                    }
                    // cleanup if we have ignored the last class in a package
                    if (currentClasses.isEmpty()) {
                        splitPackages.remove(packageName);
                    }
                }
            }
            if (filterErrorsFound) {
                throw new GradleException("Unnecessary split package ignores found");
            }
        }

        // TODO: want to read packages the same for src dirs and jars, but src dirs we also want the files in the src package dir
        private static Set<String> readPackages(File classpathElement) {
            Set<String> packages = new HashSet<>();
            Consumer<Path> addClassPackage = p -> packages.add(getPackageName(p));

            try {
                if (classpathElement.isDirectory()) {
                    walkJavaFiles(classpathElement.toPath(), ".class", addClassPackage);
                } else if (classpathElement.getName().endsWith(".jar")) {
                    try (FileSystem jar = FileSystems.newFileSystem(classpathElement.toPath(), Map.of())) {
                        for (Path root : jar.getRootDirectories()) {
                            walkJavaFiles(root, ".class", addClassPackage);
                        }
                    }
                } else {
                    throw new GradleException("Unsupported classpath element: " + classpathElement);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            return packages;
        }

        private static void walkJavaFiles(Path root, String suffix, Consumer<Path> classConsumer) throws IOException {
            if (Files.exists(root) == false) {
                return;
            }
            Files.walk(root)
                .filter(p -> p.toString().endsWith(suffix))
                .map(root::relativize)
                .filter(p -> p.getNameCount() > 1) // module-info or other things without a package can be skipped
                .filter(p -> p.toString().startsWith("META-INF") == false)
                .forEach(classConsumer);
        }

        private static String getPackageName(Path path) {
            List<String> subpackages = new ArrayList<>();
            for (int i = 0; i < path.getNameCount() - 1; ++i) {
                subpackages.add(path.getName(i).toString());
            }
            return String.join(".", subpackages);
        }

        private String formatDependency(File dependencyFile) {
            if (dependencyFile.isDirectory()) {
                while (dependencyFile.getName().equals("build") == false) {
                    dependencyFile = dependencyFile.getParentFile();
                }
                String projectName = getParameters().getProjectBuildDirs().get().get(dependencyFile);
                if (projectName == null) {
                    throw new IllegalStateException("Build directory unknown to gradle: " + dependencyFile);
                }
                return "project " + projectName;
            }
            return dependencyFile.getName(); // just the jar filename
        }
    }

    interface Parameters extends WorkParameters {
        Property<String> getProjectPath();
        MapProperty<File, String> getProjectBuildDirs();
        ConfigurableFileCollection getClasspath();
        SetProperty<File> getSrcDirs();
        SetProperty<String> getIgnoreClasses();
        RegularFileProperty getMarkerFile();
    }
}
