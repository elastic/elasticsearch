/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;

import javax.inject.Inject;
import java.io.File;

/**
 * Runs LoggerUsageCheck on a set of directories.
 */
@CacheableTask
public class LoggerUsageTask extends PrecommitTask {

    private FileCollection classpath;
    private ExecOperations execOperations;

    @Inject
    public LoggerUsageTask(ExecOperations execOperations) {
        this.execOperations = execOperations;
        setDescription("Runs LoggerUsageCheck on output directories of all source sets");
    }

    @TaskAction
    public void runLoggerUsageTask() {
        LoggedExec.javaexec(execOperations, spec -> {
            spec.getMainClass().set("org.elasticsearch.test.loggerusage.ESLoggerUsageChecker");
            spec.classpath(getClasspath());
            getClassDirectories().forEach(spec::args);
        });
    }

    @Classpath
    public FileCollection getClasspath() {
        return classpath;
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    @SkipWhenEmpty
    public FileCollection getClassDirectories() {
        return getProject().getExtensions().getByType(JavaPluginExtension.class)
            .getSourceSets()
            .stream()
            // Don't pick up all source sets like the java9 ones as logger-check doesn't support the class format
            .filter(
                sourceSet -> sourceSet.getName().equals(SourceSet.MAIN_SOURCE_SET_NAME)
                    || sourceSet.getName().equals(SourceSet.TEST_SOURCE_SET_NAME)
            )
            .map(sourceSet -> sourceSet.getOutput().getClassesDirs())
            .reduce(FileCollection::plus)
            .orElse(getProject().files())
            .filter(File::exists);
    }

}
