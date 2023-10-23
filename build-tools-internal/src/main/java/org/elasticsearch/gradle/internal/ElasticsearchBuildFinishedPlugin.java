/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.apache.tools.tar.TarOutputStream;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.AntBuilder;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.ConfigurableFileTree;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.flow.FlowAction;
import org.gradle.api.flow.FlowParameters;
import org.gradle.api.flow.FlowProviders;
import org.gradle.api.flow.FlowScope;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.tasks.Input;
import org.gradle.process.ExecOperations;
import org.gradle.api.provider.Property;

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public abstract class ElasticsearchBuildFinishedPlugin implements Plugin<Project> {

    @Inject
    protected abstract FlowScope getFlowScope();

    @Inject
    protected abstract FlowProviders getFlowProviders();

    @Override
    public void apply(Project target) {
        String buildNumber = System.getenv("BUILD_NUMBER") != null ? System.getenv("BUILD_NUMBER") : System.getenv("BUILDKITE_BUILD_NUMBER");
        String performanceTest = System.getenv("BUILD_PERFORMANCE_TEST");



        if (buildNumber != null && performanceTest == null && GradleUtils.isIncludedBuild(target) == false) {

            var fileset = target.fileTree(target.getProjectDir(), files -> {
                files.include("**/*.hprof");
                files.include("**/build/test-results/**/*.xml");
                files.include("**/build/testclusters/**");
                files.include("**/build/testrun/*/temp/**");
                files.include("**/build/**/hs_err_pid*.log");
                files.exclude("**/build/testclusters/**/data/**");
                files.exclude("**/build/testclusters/**/distro/**");
                files.exclude("**/build/testclusters/**/repo/**");
                files.exclude("**/build/testclusters/**/extract/**");
                files.exclude("**/build/testclusters/**/tmp/**");
                files.exclude("**/build/testrun/*/temp/**/data/**");
                files.exclude("**/build/testrun/*/temp/**/distro/**");
                files.exclude("**/build/testrun/*/temp/**/repo/**");
                files.exclude("**/build/testrun/*/temp/**/extract/**");
                files.exclude("**/build/testrun/*/temp/**/tmp/**");
            });



            getFlowScope().always(BuildFinishedFlowAction.class, spec -> {

                spec.getParameters().getUploadFile().set(
                    getFlowProviders().getBuildWorkResult().map(result -> target.file("build/build-finished.txt")));
                spec.getParameters().getProjectDir().set(target.getProjectDir());
                spec.getParameters().getGradleHome().set(target.getGradle().getGradleHomeDir());
            });
        }
    }

    private abstract static class BuildFinishedFlowAction implements FlowAction<BuildFinishedFlowAction.Parameters> {
        interface Parameters extends FlowParameters {
            @Input
            Property<File> getUploadFile();

            Property<File> getProjectDir();

            Property<File> getGradleHome();

        }

        @Inject
        protected abstract FileSystemOperations getFileSystemOperations();
        @Inject
        protected abstract FileOperations getFileOperations();
        @Override
        public void execute(BuildFinishedFlowAction.Parameters parameters) throws FileNotFoundException {
            File uploadFile = parameters.getUploadFile().get();
            if (uploadFile.exists()) {
                getFileSystemOperations().delete(spec -> spec.delete(uploadFile));
            }
            TarOutputStream outputStream = new TarOutputStream(new FileOutputStream(uploadFile));
            getFileOperations().fileTree("projectDir");
            //outputStream.putNextEntry("build-finished.txt", TarOutputStream.LONGFILE_GNU);
        }
    }
}
