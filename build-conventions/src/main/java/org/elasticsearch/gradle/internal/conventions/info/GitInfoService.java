/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.info;

import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.io.File;

import javax.inject.Inject;

/**
 * Build-scoped holder for the {@link GitInfo} of the git repository that contains the current build.
 *
 * <p>Every project that needs git information shares a single instance of this service, so the git
 * metadata is resolved at most once per build. This replaces applying {@code GitInfoPlugin} to the
 * root project from other projects, which is not compatible with Gradle's isolated projects.
 *
 * <p>The value is still backed by {@link GitInfoValueSource} so that the configuration cache tracks
 * it as a build input whenever it is read at configuration time.
 */
public abstract class GitInfoService implements BuildService<GitInfoService.Params> {

    public static final String NAME = "gitInfo";

    private final Provider<GitInfo> gitInfo;

    @Inject
    public GitInfoService(ProviderFactory providerFactory) {
        File gitRootDir = getParameters().getGitRootDir().get();
        // a value source provider memoizes its value, so sharing this instance means a single lookup per build
        this.gitInfo = providerFactory.of(GitInfoValueSource.class, spec -> spec.getParameters().getPath().set(gitRootDir));
    }

    public Provider<GitInfo> getGitInfo() {
        return gitInfo;
    }

    public interface Params extends BuildServiceParameters {
        Property<File> getGitRootDir();
    }
}
