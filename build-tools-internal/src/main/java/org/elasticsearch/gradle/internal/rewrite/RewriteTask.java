/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rewrite;

import org.gradle.api.DefaultTask;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

import javax.inject.Inject;
import java.io.File;
import java.util.List;

import static java.util.stream.Collectors.toList;

public abstract class RewriteTask extends DefaultTask {

    private final Configuration configuration;
    private WorkerExecutor workerExecutor;
    protected final RewriteExtension extension;

    @InputFiles
    abstract ConfigurableFileCollection getSourceFiles();

    @InputFiles
    abstract ConfigurableFileCollection getDependencyFiles();

    @Input
    abstract SetProperty<String> getActiveRecipes();

    @InputFile
    abstract RegularFileProperty getConfigFile();

    @Inject
    public RewriteTask(
        Configuration configuration,
        RewriteExtension extension,
        WorkerExecutor workerExecutor
    ) {
        this.configuration = configuration;
        this.extension = extension;
        this.workerExecutor = workerExecutor;
        this.setGroup("rewrite");
        this.setDescription("Apply the active refactoring recipes");
    }

    @TaskAction
    public void run() {
        WorkQueue workQueue = workerExecutor.processIsolation(spec -> {
            spec.getClasspath().from(configuration);
            spec.getForkOptions().jvmArgs("--add-exports");
            spec.getForkOptions().jvmArgs("jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED");

            spec.getForkOptions().jvmArgs("--add-exports");
            spec.getForkOptions().jvmArgs("jdk.compiler/com.sun.tools.javac.main=ALL-UNNAMED");

            spec.getForkOptions().jvmArgs("--add-exports");
            spec.getForkOptions().jvmArgs("jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED");

            spec.getForkOptions().jvmArgs("--add-exports");
            spec.getForkOptions().jvmArgs("jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED");

            spec.getForkOptions().jvmArgs("--add-exports");
            spec.getForkOptions().jvmArgs("jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED");

            spec.getForkOptions().jvmArgs("--add-exports");
            spec.getForkOptions().jvmArgs("jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED");

            spec.getForkOptions().jvmArgs("--add-exports");
            spec.getForkOptions().jvmArgs("jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED");

            spec.getForkOptions().jvmArgs("--add-exports");
            spec.getForkOptions().jvmArgs("jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED");
            spec.getForkOptions().workingDir(getProject().getProjectDir());
        });

        List<File> javaPaths = getSourceFiles().getFiles()
            .stream()
            .filter(it -> it.isFile() && it.getName().endsWith(".java"))
            .collect(toList());

        if (javaPaths.size() > 0) {
            List<File> dependencyPaths = getDependencyFiles().getFiles().stream().collect(toList());

            workQueue.submit(RewriteWorker.class, parameters -> {
                parameters.getAllJavaPaths().addAll(javaPaths);
                parameters.getAllDependencyPaths().addAll(dependencyPaths);
                parameters.getActiveRecipes().addAll(getActiveRecipes().get());
                parameters.getProjectDirectory().fileProvider(getProject().getProviders().provider(() -> getProject().getProjectDir()));
                parameters.getConfigFile().value(getConfigFile());
            });
        }
    }
}
