/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.jarhell;

import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import javax.inject.Inject;

/**
 * Runs CheckJarHell on a classpath.
 */
@CacheableTask
public class JarHellTask extends DefaultTask {

    private FileCollection jarHellRuntimeClasspath;

    private FileCollection classpath;
    private ExecOperations execOperations;
    private ProjectLayout projectLayout;

    @Inject
    public JarHellTask(ExecOperations execOperations, ProjectLayout projectLayout) {
        this.execOperations = execOperations;
        this.projectLayout = projectLayout;
        setDescription("Runs CheckJarHell on the configured classpath");
    }

    @OutputFile
    public File getSuccessMarker() {
        return new File(projectLayout.getBuildDirectory().getAsFile().get(), "markers/" + this.getName());
    }

    @TaskAction
    public void runJarHellCheck() throws IOException {
        LoggedExec.javaexec(execOperations, spec -> {
            spec.environment("CLASSPATH", getJarHellRuntimeClasspath().plus(getClasspath()).getAsPath());
            spec.getMainClass().set("org.elasticsearch.jdk.JarHell");
        });
        writeMarker();
    }

    private void writeMarker() throws IOException {
        Files.write(getSuccessMarker().toPath(), new byte[] {}, StandardOpenOption.CREATE);
    }

    // We use compile classpath normalization here because class implementation changes are irrelevant for the purposes of jar hell.
    // We only care about the runtime classpath ABI here.
    @CompileClasspath
    @SkipWhenEmpty
    public FileCollection getClasspath() {
        return classpath == null ? null : classpath.filter(File::exists);
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    @Classpath
    public FileCollection getJarHellRuntimeClasspath() {
        return jarHellRuntimeClasspath;
    }

    public void setJarHellRuntimeClasspath(FileCollection jarHellRuntimeClasspath) {
        this.jarHellRuntimeClasspath = jarHellRuntimeClasspath;
    }
}
