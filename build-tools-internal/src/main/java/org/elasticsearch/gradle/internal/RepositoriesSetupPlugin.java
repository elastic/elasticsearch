/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.dsl.RepositoryHandler;
import org.gradle.api.artifacts.repositories.MavenArtifactRepository;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RepositoriesSetupPlugin implements Plugin<Project> {

    private static final Pattern LUCENE_SNAPSHOT_REGEX = Pattern.compile("\\w+-snapshot-([a-z0-9]+)");

    @Override
    public void apply(Project project) {
        configureRepositories(project);
    }

    /**
     * Adds repositories used by ES projects and dependencies
     */
    public static void configureRepositories(Project project) {
        RepositoryHandler repos = project.getRepositories();
        if (System.getProperty("repos.mavenLocal") != null) {
            // with -Drepos.mavenLocal=true we can force checking the local .m2 repo which is
            // useful for development ie. bwc tests where we install stuff in the local repository
            // such that we don't have to pass hardcoded files to gradle
            repos.mavenLocal();
        }
        repos.mavenCentral();

        String luceneVersion = VersionProperties.getLucene();
        if (luceneVersion.contains("-snapshot")) {
            // extract the revision number from the version with a regex matcher
            Matcher matcher = LUCENE_SNAPSHOT_REGEX.matcher(luceneVersion);
            if (matcher.find() == false) {
                throw new GradleException("Malformed lucene snapshot version: " + luceneVersion);
            }
            String revision = matcher.group(1);
            MavenArtifactRepository luceneRepo = repos.maven(repo -> {
                repo.setName("lucene-snapshots");
                repo.setUrl("https://s3.amazonaws.com/download.elasticsearch.org/lucenesnapshots/" + revision);
            });
            repos.exclusiveContent(exclusiveRepo -> {
                exclusiveRepo.filter(
                    descriptor -> descriptor.includeVersionByRegex("org\\.apache\\.lucene", ".*", ".*-snapshot-" + revision)
                );
                exclusiveRepo.forRepositories(luceneRepo);
            });
        }
    }
}
