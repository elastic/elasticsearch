/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;

import javax.inject.Inject;
import java.io.File;

/**
 * Runs CheckJarHell on a classpath.
 */
@CacheableTask
public class JarHellTask extends PrecommitTask {

    private FileCollection classpath;
    private ExecOperations execOperations;

    @Inject
    public JarHellTask(ExecOperations execOperations) {
        this.execOperations = execOperations;
        setDescription("Runs CheckJarHell on the configured classpath");
    }

    @TaskAction
    public void runJarHellCheck() {
        LoggedExec.javaexec(execOperations, spec -> {
            spec.environment("CLASSPATH", getClasspath().getAsPath());
            spec.setMain("org.elasticsearch.bootstrap.JarHell");
        });
    }

    // We use compile classpath normalization here because class implementation changes are irrelevant for the purposes of jar hell.
    // We only care about the runtime classpath ABI here.
    @CompileClasspath
    public FileCollection getClasspath() {
        return classpath.filter(File::exists);
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

}
