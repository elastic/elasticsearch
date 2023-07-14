/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.model.ObjectFactory;
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
import java.util.Comparator;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import javax.inject.Inject;

import static java.util.stream.Collectors.toSet;

public class JavaModulePrecommitTask extends PrecommitTask {

    private static final String expectedVersion = VersionProperties.getElasticsearch();

    private final SetProperty<File> srcDirs;

    private FileCollection classesDirs;

    private FileCollection classpath;

    private File resourcesDir;

    @Inject
    public JavaModulePrecommitTask(ObjectFactory objectFactory) {
        srcDirs = objectFactory.setProperty(File.class);
    }

    @InputFiles
    public FileCollection getClassesDirs() {
        return this.classesDirs;
    }

    public void setClassesDirs(FileCollection classesDirs) {
        Objects.requireNonNull(classesDirs, "classesDirs");
        this.classesDirs = classesDirs;
    }

    @InputFiles
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
        checkModuleVersion(mod);
        checkModuleNamePrefix(mod);
        checkModuleServices(mod);
    }

    private void checkModuleVersion(ModuleReference mref) {
        getLogger().info("{} checking module version for {}", this, mref.descriptor().name());
        String mVersion = mref.descriptor()
            .rawVersion()
            .orElseThrow(() -> new GradleException("no version found in module " + mref.descriptor().name()));
        if (mVersion.equals(expectedVersion) == false) {
            throw new GradleException("Expected version [" + expectedVersion + "], in " + mref.descriptor());
        }
    }

    private void checkModuleNamePrefix(ModuleReference mref) {
        getLogger().info("{} checking module name prefix for {}", this, mref.descriptor().name());
        if (mref.descriptor().name().startsWith("org.elasticsearch.") == false &&
            mref.descriptor().name().startsWith("co.elastic.") == false) {
            throw new GradleException("Expected name starting with \"org.elasticsearch.\" or \"co.elastic\" in " + mref.descriptor());
        }
    }

    private void checkModuleServices(ModuleReference mref) {
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
                            throw new GradleException(
                                String.format(
                                    Locale.ROOT,
                                    "Expected provides %s in module %s with provides %s.",
                                    service,
                                    mref.descriptor().name(),
                                    mref.descriptor().provides()
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
