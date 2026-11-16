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
import org.elasticsearch.gradle.internal.conventions.info.GitInfoService;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;

import java.io.File;

/**
 * Exposes the {@link GitInfo} of the repository containing the current build.
 *
 * <p>This plugin can be applied to any project. The git lookup itself is shared across all projects of a
 * build via {@link GitInfoService}, so consumers should apply it to the project they configure instead
 * of reaching into the root project, which is not allowed with Gradle's isolated projects.
 */
public abstract class GitInfoPlugin implements Plugin<Project> {

    private Provider<String> revision;

    @Override
    public void apply(Project project) {
        File settingsDir = project.getLayout().getSettingsDirectory().getAsFile();
        Provider<GitInfoService> service = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(
                GitInfoService.NAME,
                GitInfoService.class,
                spec -> spec.getParameters().getGitRootDir().set(findGitRootDir(settingsDir))
            );
        getGitInfo().convention(service.flatMap(GitInfoService::getGitInfo));
        revision = getGitInfo().map(info -> info.getRevision() == null ? info.getRevision() : "main");
    }

    /**
     * Resolves the git working tree root for the build rooted at {@code settingsDir}. For the {@code build-conventions}
     * and {@code build-tools*} included builds, which live directly inside the elasticsearch workspace, this is the
     * workspace directory. Unlike {@code Util#locateElasticsearchWorkspace} this does not navigate the Gradle build
     * hierarchy, which is not allowed with isolated projects.
     */
    static File findGitRootDir(File settingsDir) {
        if (new File(settingsDir, ".git").exists()) {
            return settingsDir;
        }
        File parent = settingsDir.getParentFile();
        if (parent != null && new File(parent, "build-tools-internal/version.properties").exists()) {
            return parent;
        }
        return settingsDir;
    }

    public abstract Property<GitInfo> getGitInfo();

    public Provider<String> getRevision() {
        return revision;
    }
}
