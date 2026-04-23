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
import org.gradle.api.problems.Severity;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
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
    private final ProblemReporter problemReporter;

    private FileCollection classesDirs;

    private FileCollection classpath;

    private File resourcesDir;

    @Inject
    public JavaModulePrecommitTask(ObjectFactory objectFactory, Problems problems) {
        srcDirs = objectFactory.setProperty(File.class);
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

    @Classpath
    @InputFiles
    public FileCollection getClasspath() {
        return classpath;
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
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
        if (problems.isEmpty() == false) {
            problemReporter.report(problems);
            throw new GradleException(
                "Module validation failed for "
                    + mod.descriptor().name()
                    + " with "
                    + problems.size()
                    + " problem"
                    + (problems.size() == 1 ? "" : "s")
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
                        .severity(Severity.ERROR)
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
                    ).severity(Severity.ERROR).solution("Update the module version to " + expectedVersion)
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
                    ).severity(Severity.ERROR).solution("Rename the module to start with 'org.elasticsearch.' or 'co.elastic.'")
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
                                        .severity(Severity.ERROR)
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
