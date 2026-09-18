/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.foreign;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.jvm.toolchain.JavaLauncher;
import org.gradle.process.ExecOperations;

import java.io.File;

import javax.inject.Inject;

/**
 * Runs {@code org.elasticsearch.foreign.processor.ModuleInfoAugmenter} to inject {@code provides
 * LibraryProvider with ...} directives into a compiled {@code module-info.class}.
 *
 * <p>The augmenter uses the JDK 24+ {@code java.lang.classfile} API and therefore runs in a toolchain
 * JVM rather than the Gradle daemon's bundled JDK.
 */
public abstract class AugmentForeignModuleInfoTask extends DefaultTask {

    private final ExecOperations execOperations;

    @Inject
    public AugmentForeignModuleInfoTask(ExecOperations execOperations) {
        this.execOperations = execOperations;
    }

    @InputFile
    @PathSensitive(PathSensitivity.NAME_ONLY)
    public abstract RegularFileProperty getInputModuleInfo();

    @InputFile
    @PathSensitive(PathSensitivity.NAME_ONLY)
    public abstract RegularFileProperty getServicesFile();

    @Classpath
    public abstract ConfigurableFileCollection getAugmenterClasspath();

    @Nested
    public abstract Property<JavaLauncher> getJavaLauncher();

    @OutputFile
    public abstract RegularFileProperty getOutputModuleInfo();

    @TaskAction
    public void augment() {
        File outputFile = getOutputModuleInfo().get().getAsFile();
        File parent = outputFile.getParentFile();
        if (parent != null && parent.mkdirs() == false && parent.isDirectory() == false) {
            throw new IllegalStateException("Could not create output directory: " + parent);
        }

        String javaExecutable = getJavaLauncher().get().getExecutablePath().getAsFile().getAbsolutePath();

        execOperations.exec(spec -> {
            spec.executable(javaExecutable);
            spec.args("-cp", getAugmenterClasspath().getAsPath());
            spec.args("org.elasticsearch.foreign.processor.ModuleInfoAugmenter");
            spec.args(
                getInputModuleInfo().get().getAsFile().getAbsolutePath(),
                getServicesFile().get().getAsFile().getAbsolutePath(),
                outputFile.getAbsolutePath()
            );
        }).assertNormalExitValue();
    }
}
