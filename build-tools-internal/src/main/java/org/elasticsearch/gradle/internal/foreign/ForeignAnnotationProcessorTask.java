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
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.jvm.toolchain.JavaCompiler;
import org.gradle.process.ExecOperations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

/**
 * Runs the {@code libs/ffm} annotation processor in {@code -proc:only} mode using an external
 * {@code javac} from a configurable toolchain JDK.
 *
 * <p>The processor depends on the JDK 24+ {@code java.lang.classfile} API, which the Gradle daemon's
 * bundled JDK 21 cannot load — hence the out-of-process javac invocation.
 */
public abstract class ForeignAnnotationProcessorTask extends DefaultTask {

    private final ExecOperations execOperations;
    private final FileSystemOperations fileSystemOperations;

    @Inject
    public ForeignAnnotationProcessorTask(ExecOperations execOperations, FileSystemOperations fileSystemOperations) {
        this.execOperations = execOperations;
        this.fileSystemOperations = fileSystemOperations;
    }

    @InputFiles
    @SkipWhenEmpty
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getSources();

    @CompileClasspath
    public abstract ConfigurableFileCollection getModulePath();

    @CompileClasspath
    public abstract ConfigurableFileCollection getProcessorModulePath();

    @Input
    public abstract Property<String> getReleaseVersion();

    @Nested
    public abstract Property<JavaCompiler> getJavaCompiler();

    @OutputDirectory
    public abstract DirectoryProperty getOutputDirectory();

    @TaskAction
    public void run() {
        File outputDir = getOutputDirectory().get().getAsFile();
        // clean the output directory so stale generated classes / services entries do not leak through
        fileSystemOperations.delete(spec -> spec.delete(outputDir));
        if (outputDir.mkdirs() == false && outputDir.isDirectory() == false) {
            throw new IllegalStateException("Could not create output directory: " + outputDir);
        }

        String javacExecutable = getJavaCompiler().get().getExecutablePath().getAsFile().getAbsolutePath();

        List<String> command = new ArrayList<>();
        command.add(javacExecutable);
        command.add("-proc:only");
        command.add("--module-path");
        command.add(getModulePath().getAsPath());
        command.add("--processor-module-path");
        command.add(getProcessorModulePath().getAsPath());
        command.add("-AjavaVersion=" + getReleaseVersion().get());
        command.add("-d");
        command.add(outputDir.getAbsolutePath());
        for (File source : getSources()) {
            command.add(source.getAbsolutePath());
        }

        execOperations.exec(spec -> spec.commandLine(command)).assertNormalExitValue();
    }
}
