/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.elasticsearch.gradle.internal.conventions.problems.ElasticsearchBuildProblems;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.problems.Problem;
import org.gradle.api.problems.ProblemId;
import org.gradle.api.problems.ProblemReporter;
import org.gradle.api.problems.Problems;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.module.Configuration;
import java.lang.module.FindException;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.lang.module.ResolutionException;
import java.lang.module.ResolvedModule;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import static java.util.stream.Collectors.toSet;

public class JavaModulePrecommitTask extends PrecommitTask {

    private static final String expectedVersion = VersionProperties.getElasticsearch();

    private final SetProperty<File> srcDirs;
    private final SetProperty<String> allowedUnreachable;
    private final ProblemReporter problemReporter;

    private FileCollection classesDirs;

    /** Bundled plugin jars: runtimeClasspath minus resolveableCompileOnly. Null for non-plugin projects. */
    private FileCollection bundledClasspath;

    /** Provided (compile-only) jars: resolveableCompileOnly. Null for non-plugin projects. */
    private FileCollection providedClasspath;

    private File resourcesDir;

    @Inject
    public JavaModulePrecommitTask(ObjectFactory objectFactory, Problems problems) {
        srcDirs = objectFactory.setProperty(File.class);
        allowedUnreachable = objectFactory.setProperty(String.class);
        this.problemReporter = problems.getReporter();
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileCollection getClassesDirs() {
        return this.classesDirs;
    }

    public void setClassesDirs(FileCollection classesDirs) {
        Objects.requireNonNull(classesDirs, "classesDirs");
        this.classesDirs = classesDirs;
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public File getResourcesDir() {
        return this.resourcesDir;
    }

    public void setResourcesDirs(File resourcesDirs) {
        Objects.requireNonNull(resourcesDirs, "resourcesDirs");
        this.resourcesDir = resourcesDirs;
    }

    @InputFiles
    @SkipWhenEmpty
    @PathSensitive(PathSensitivity.RELATIVE)
    public SetProperty<File> getSrcDirs() {
        return srcDirs;
    }

    @org.gradle.api.tasks.Optional
    @Classpath
    @InputFiles
    public FileCollection getBundledClasspath() {
        return bundledClasspath;
    }

    public void setBundledClasspath(FileCollection bundledClasspath) {
        this.bundledClasspath = bundledClasspath;
    }

    @org.gradle.api.tasks.Optional
    @Classpath
    @InputFiles
    public FileCollection getProvidedClasspath() {
        return providedClasspath;
    }

    public void setProvidedClasspath(FileCollection providedClasspath) {
        this.providedClasspath = providedClasspath;
    }

    @Input
    public SetProperty<String> getAllowedUnreachable() {
        return allowedUnreachable;
    }

    @TaskAction
    public void checkModule() {
        if (hasModuleInfoDotJava(getSrcDirs()) == false) {
            return; // non-modular project, nothing to do
        }
        ModuleReference mod = esModuleFor(getClassesDirs().getSingleFile());
        getLogger().info("{} checking module {}", getPath(), mod);
        List<Problem> problems = new ArrayList<>();
        checkModuleVersion(mod, problems);
        checkModuleNamePrefix(mod, problems);
        checkModuleServices(mod, problems);
        if (bundledClasspath != null && bundledClasspath.isEmpty() == false) {
            checkModuleResolution(mod, problems);
        }
        if (problems.isEmpty() == false) {
            throw problemReporter.throwing(
                new GradleException(
                    "Module validation failed for "
                        + mod.descriptor().name()
                        + " with "
                        + problems.size()
                        + " problem"
                        + (problems.size() == 1 ? "" : "s")
                ),
                problems
            );
        }
    }

    private void checkModuleVersion(ModuleReference mref, List<Problem> problems) {
        getLogger().info("{} checking module version for {}", this, mref.descriptor().name());
        Optional<String> rawVersion = mref.descriptor().rawVersion();
        if (rawVersion.isEmpty()) {
            problems.add(
                problemReporter.create(
                    ProblemId.create("missing-module-version", "Missing module version", ElasticsearchBuildProblems.JAVA_MODULE),
                    spec -> spec.contextualLabel("No version found in module " + mref.descriptor().name())
                        .solution("Add a version to the module descriptor")
                )
            );
            return;
        }
        if (rawVersion.get().equals(expectedVersion) == false) {
            problems.add(
                problemReporter.create(
                    ProblemId.create("module-version-mismatch", "Module version mismatch", ElasticsearchBuildProblems.JAVA_MODULE),
                    spec -> spec.contextualLabel(
                        "Expected version [" + expectedVersion + "] but found [" + rawVersion.get() + "] in " + mref.descriptor().name()
                    ).solution("Update the module version to " + expectedVersion)
                )
            );
        }
    }

    private void checkModuleNamePrefix(ModuleReference mref, List<Problem> problems) {
        getLogger().info("{} checking module name prefix for {}", this, mref.descriptor().name());
        if (mref.descriptor().name().startsWith("org.elasticsearch.") == false
            && mref.descriptor().name().startsWith("co.elastic.") == false) {
            problems.add(
                problemReporter.create(
                    ProblemId.create("invalid-module-name-prefix", "Invalid module name prefix", ElasticsearchBuildProblems.JAVA_MODULE),
                    spec -> spec.contextualLabel(
                        "Expected name starting with 'org.elasticsearch.' or 'co.elastic.' in " + mref.descriptor().name()
                    ).solution("Rename the module to start with 'org.elasticsearch.' or 'co.elastic.'")
                )
            );
        }
    }

    private void checkModuleServices(ModuleReference mref, List<Problem> problems) {
        getLogger().info("{} checking module services for {}", getPath(), mref.descriptor().name());
        Set<String> modServices = mref.descriptor().provides().stream().map(ModuleDescriptor.Provides::service).collect(toSet());
        Path servicesRoot = getResourcesDir().toPath().resolve("META-INF").resolve("services");
        getLogger().info("{} servicesRoot {}", getPath(), servicesRoot);
        if (Files.exists(servicesRoot)) {
            try (var paths = Files.walk(servicesRoot)) {
                paths.filter(Files::isRegularFile)
                    .map(p -> servicesRoot.relativize(p))
                    .map(Path::toString)
                    .peek(s -> getLogger().info("%s checking service %s", this, s))
                    .forEach(service -> {
                        if (modServices.contains(service) == false) {
                            problems.add(
                                problemReporter.create(
                                    ProblemId.create(
                                        "missing-module-service",
                                        "Missing module service declaration",
                                        ElasticsearchBuildProblems.JAVA_MODULE
                                    ),
                                    spec -> spec.contextualLabel(
                                        String.format(Locale.ROOT, "Expected provides %s in module %s", service, mref.descriptor().name())
                                    )
                                        .details("Module " + mref.descriptor().name() + " has provides " + mref.descriptor().provides())
                                        .solution("Add 'provides " + service + "' to the module-info.java")
                                )
                            );
                        }
                    });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void checkModuleResolution(ModuleReference mref, List<Problem> problems) {
        getLogger().info("{} checking module resolution for {}", getPath(), mref.descriptor().name());
        String rootName = mref.descriptor().name();

        // The observable universe: bundled jars + provided (compile-only) jars + the plugin's own classes.
        // This mirrors what PluginsLoader puts in its after-finder, plus the parent-layer modules
        // (server, core) that would be in the runtime parent layer but must be findable for
        // resolution of the plugin's own requires to succeed.
        List<Path> observablePaths = new ArrayList<>();
        observablePaths.add(getClassesDirs().getSingleFile().toPath());
        List<Path> bundledJarPaths = new ArrayList<>();
        for (File f : bundledClasspath) {
            if (f.isFile() && f.getName().endsWith(".jar")) {
                Path p = f.toPath();
                observablePaths.add(p);
                bundledJarPaths.add(p);
            }
        }
        if (providedClasspath != null) {
            for (File f : providedClasspath) {
                if (f.isFile() && f.getName().endsWith(".jar")) {
                    observablePaths.add(f.toPath());
                }
            }
        }

        Set<String> allowed = getAllowedUnreachable().get();
        try {
            List<String> unreachable = findUnreachableBundledModules(
                rootName,
                observablePaths.toArray(Path[]::new),
                bundledJarPaths,
                allowed
            );
            for (String moduleName : unreachable) {
                problems.add(
                    problemReporter.create(
                        ProblemId.create(
                            "unreachable-bundled-module",
                            "Unreachable bundled module",
                            ElasticsearchBuildProblems.JAVA_MODULE
                        ),
                        spec -> spec.contextualLabel(
                            "Bundled module '" + moduleName + "' is an explicit JPMS module but is not reachable from '" + rootName + "'"
                        ).solution("Add 'requires " + moduleName + ";' to module-info.java, or remove the unused dependency")
                    )
                );
            }
        } catch (FindException | ResolutionException e) {
            problems.add(
                problemReporter.create(
                    ProblemId.create("module-resolution-failed", "Module resolution failed", ElasticsearchBuildProblems.JAVA_MODULE),
                    spec -> spec.contextualLabel("Module resolution failed for '" + rootName + "': " + e.getMessage())
                        .solution("Check that all required modules are present in the classpath")
                )
            );
        }
    }

    /**
     * Returns the names of bundled explicit JPMS modules that are not reachable from {@code rootName}
     * in the module graph resolved over {@code observablePaths}.
     *
     * <p>Automatic modules are excluded from the result: when any automatic module is resolved the
     * JPMS resolver pulls in all observable automatic modules automatically, so they are never the
     * source of a missing-requires bug. Only explicit modules (those with a real {@code module-info})
     * can be silently dropped from the graph when nothing declares {@code requires} for them.
     *
     * <p>Extracted as a package-private static to enable unit testing without Gradle infrastructure.
     */
    static List<String> findUnreachableBundledModules(
        String rootName,
        Path[] observablePaths,
        Iterable<Path> bundledJarPaths,
        Set<String> allowed
    ) {
        Configuration cfg = Configuration.resolveAndBind(
            ModuleFinder.of(),
            List.of(ModuleLayer.boot().configuration()),
            ModuleFinder.of(observablePaths),
            Set.of(rootName)
        );
        Set<String> resolved = cfg.modules().stream().map(ResolvedModule::name).collect(toSet());

        List<String> unreachable = new ArrayList<>();
        for (Path jar : bundledJarPaths) {
            ModuleFinder.of(jar)
                .findAll()
                .stream()
                .map(ModuleReference::descriptor)
                .filter(d -> d.isAutomatic() == false)
                .map(ModuleDescriptor::name)
                .filter(n -> resolved.contains(n) == false)
                .filter(n -> allowed.contains(n) == false)
                .forEach(unreachable::add);
        }
        return unreachable;
    }

    private static boolean hasModuleInfoDotJava(SetProperty<File> srcDirs) {
        return srcDirs.get().stream().map(dir -> dir.toPath().resolve("module-info.java")).anyMatch(Files::exists);
    }

    private static ModuleReference esModuleFor(File filePath) {
        return ModuleFinder.of(filePath.toPath())
            .findAll()
            .stream()
            .min(Comparator.comparing(ModuleReference::descriptor))
            .orElseThrow(() -> new GradleException("module not found in " + filePath));
    }
}
