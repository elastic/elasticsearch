/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.InternalPlugin;
import org.elasticsearch.gradle.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import javax.inject.Inject;
import java.util.stream.Collectors;

public class LicenseHeadersPrecommitPlugin extends PrecommitPlugin implements InternalPlugin {
    @Inject
    public LicenseHeadersPrecommitPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        return project.getTasks().register("licenseHeaders", LicenseHeadersTask.class, licenseHeadersTask -> {
            project.getPlugins().withType(JavaBasePlugin.class, javaBasePlugin -> {
                final SourceSetContainer sourceSets = GradleUtils.getJavaSourceSets(project);
                licenseHeadersTask.getSourceFolders()
                    .addAll(providerFactory.provider(() -> sourceSets.stream().map(s -> s.getAllJava()).collect(Collectors.toList())));
            });
        });
    }

    private ProviderFactory providerFactory;
}
