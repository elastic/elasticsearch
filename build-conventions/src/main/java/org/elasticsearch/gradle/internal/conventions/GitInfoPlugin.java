/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions;

import org.elasticsearch.gradle.internal.conventions.info.GitInfo;
import org.elasticsearch.gradle.internal.conventions.info.GitInfoValueSource;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;

import java.io.File;

import javax.inject.Inject;

public abstract class GitInfoPlugin implements Plugin<Project> {

    private ProviderFactory factory;
    private Provider<String> revision;

    @Inject
    public GitInfoPlugin(ProviderFactory factory) {
        this.factory = factory;
    }

    @Override
    public void apply(Project project) {
        File rootDir = getGitRootDir(project);
        getGitInfo().convention(factory.of(GitInfoValueSource.class, spec -> { spec.getParameters().getPath().set(rootDir); }));
        revision = getGitInfo().map(info -> info.getRevision() == null ? info.getRevision() : "main");
    }

    private static File getGitRootDir(Project project) {
        File rootDir = project.getRootDir();
        if (new File(rootDir, ".git").exists()) {
            return rootDir;
        }
        return Util.locateElasticsearchWorkspace(project.getGradle());
    }

    public abstract Property<GitInfo> getGitInfo();

    public Provider<String> getRevision() {
        return revision;
    }
}
