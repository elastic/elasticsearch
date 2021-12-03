/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;

import java.io.IOException;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** Checks for Elasticsearch Java modules. */
public class InternalDistributionModuleCheckTaskProvider {

    private final Logger LOGGER = Logging.getLogger(InternalDistributionModuleCheckTaskProvider.class);  // #### add some logging

    private static final String MODULE_INFO = "module-info.class";

    private InternalDistributionModuleCheckTaskProvider() {};

    /** Registers the checkModules tasks, which contains all checks relevant to ES Java Modules. */
    static TaskProvider<Task> registerCheckModulesTask(Project project, TaskProvider<Copy> checkExtraction) {
        return project.getTasks().register("checkModules", task -> {
            task.dependsOn(checkExtraction);
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    final Path libPath = checkExtraction.get()
                        .getDestinationDir()
                        .toPath()
                        .resolve("elasticsearch-" + VersionProperties.getElasticsearch())
                        .resolve("lib");
                    assertAllESJarsAreModular(libPath);
                    assertAllModulesPresent(libPath);
                    assertModuleVersions(libPath, VersionProperties.getElasticsearch());
                }
            });
        });
    }

    private static final String ES_JAR_PREFIX = "elasticsearch-";
    // ES jars in the lib directory that are not modularized. For now, launchers is the only one
    private static final List<String> ES_JAR_EXCLUDES = List.of("elasticsearch-launchers");

    private static Predicate<Path> isESJar = path -> path.getFileName().toString().startsWith(ES_JAR_PREFIX);
    private static Predicate<Path> isNotExcluded = path -> ES_JAR_EXCLUDES.stream()
        .filter(path.getFileName().toString()::startsWith)
        .findAny()
        .isEmpty();

    /** Checks that all expected ES jar files are modular, i.e. contain a module-info.class in their root. */
    private static void assertAllESJarsAreModular(Path libsPath) {
        try {
            Files.walk(libsPath, 1).filter(Files::isRegularFile).filter(isESJar).filter(isNotExcluded).sorted().forEach(path -> {
                System.out.println("CHECKING assertAllESJarsAreModular " + path);
                try (JarFile jf = new JarFile(path.toFile())) {
                    JarEntry entry = jf.getJarEntry(MODULE_INFO);
                    if (entry == null) {
                        throw new GradleException(MODULE_INFO + " no found in " + path);
                    }
                } catch (IOException e) {
                    throw new GradleException("Failed when reading jar file " + path, e);
                }
            });
        } catch (IOException e) {
            throw new GradleException("Failed when walking path " + libsPath, e);
        }
    }

    private static final Predicate<ModuleReference> isESModule = mref -> mref.descriptor().name().startsWith("org.elasticsearch");
    private static final Function<ModuleReference, String> toName = mref -> mref.descriptor().name();

    /** List of the current Elasticsearch Java Modules. */
    private static final List<String> EXPECTED_ES_MODUlES = List.of(
        "org.elasticsearch.cli",
        "org.elasticsearch.core",
        "org.elasticsearch.geo",
        "org.elasticsearch.lz4",
        "org.elasticsearch.plugin.classloader",
        "org.elasticsearch.secure_sm",
        "org.elasticsearch.server",
        "org.elasticsearch.xcontent"
    );

    /** Checks that all expected Elasticsearch modules are present. */
    private static void assertAllModulesPresent(Path libPath) {
        Set<ModuleReference> modules = ModuleFinder.of(libPath).findAll();
        System.out.println("CHECKING assertAllModulesPresent");
        List<String> actualESModules = modules.stream().filter(isESModule).map(toName).sorted().toList();
        if (actualESModules.equals(EXPECTED_ES_MODUlES) == false) {
            throw new GradleException("expected modules " + EXPECTED_ES_MODUlES + ", actual modules " + actualESModules);
        }
    }

    /** Checks that all expected Elasticsearch modules have the expected versions. */
    private static void assertModuleVersions(Path libPath, String expectedVersion) {
        Set<ModuleReference> modules = ModuleFinder.of(libPath).findAll();
        List<ModuleReference> esModules = modules.stream()
            .filter(isESModule)
            .sorted(Comparator.comparing(ModuleReference::descriptor))
            .toList();
        for (ModuleReference mref : esModules) {
            String mVersion = mref.descriptor()
                .rawVersion()
                .orElseThrow(() -> new GradleException("no version found in module " + mref.descriptor().name()));
            if (mVersion != expectedVersion) {
                new GradleException("Expected version [" + expectedVersion + "], in " + mref.descriptor());
            }
        }
    }

    // ####: eventually assert hashes, services, etc

}
