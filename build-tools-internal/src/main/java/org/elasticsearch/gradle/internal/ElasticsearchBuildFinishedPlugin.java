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
import org.gradle.api.AntBuilder;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.ArchiveOperations;
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

            getFlowScope().always(BuildFinishedFlowAction.class, spec -> {
                spec.getParameters().getUploadFile().set(
                    getFlowProviders().getBuildWorkResult().map(result -> target.file("build/build-finished.txt"));

            });
        }
    }

    private abstract static class BuildFinishedFlowAction implements FlowAction<BuildFinishedFlowAction.Parameters> {
        interface Parameters extends FlowParameters {
            @Input
            Property<File> getUploadFile();
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
getFileOperations().fileTree("projectDir")
            outputStream.putNextEntry("build-finished.txt", TarOutputStream.LONGFILE_GNU);
        }
    }
}
