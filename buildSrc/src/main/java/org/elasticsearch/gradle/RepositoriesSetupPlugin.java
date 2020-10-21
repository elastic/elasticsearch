/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle;

import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.dsl.RepositoryHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.artifacts.repositories.MavenArtifactRepository;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RepositoriesSetupPlugin implements Plugin<Project> {

    private static final List<String> SECURE_URL_SCHEMES = Arrays.asList("file", "https", "s3");
    private static final Pattern LUCENE_SNAPSHOT_REGEX = Pattern.compile("\\w+-snapshot-([a-z0-9]+)");

    @Override
    public void apply(Project project) {
        configureRepositories(project);
    }

    /**
     * Adds repositories used by ES projects and dependencies
     */
    public static void configureRepositories(Project project) {
        // ensure all repositories use secure urls
        // TODO: remove this with gradle 7.0, which no longer allows insecure urls
        project.getRepositories().all(repository -> {
            if (repository instanceof MavenArtifactRepository) {
                final MavenArtifactRepository maven = (MavenArtifactRepository) repository;
                assertRepositoryURIIsSecure(maven.getName(), project.getPath(), maven.getUrl());
                for (URI uri : maven.getArtifactUrls()) {
                    assertRepositoryURIIsSecure(maven.getName(), project.getPath(), uri);
                }
            } else if (repository instanceof IvyArtifactRepository) {
                final IvyArtifactRepository ivy = (IvyArtifactRepository) repository;
                assertRepositoryURIIsSecure(ivy.getName(), project.getPath(), ivy.getUrl());
            }
        });
        RepositoryHandler repos = project.getRepositories();
        if (System.getProperty("repos.mavenLocal") != null) {
            // with -Drepos.mavenLocal=true we can force checking the local .m2 repo which is
            // useful for development ie. bwc tests where we install stuff in the local repository
            // such that we don't have to pass hardcoded files to gradle
            repos.mavenLocal();
        }
        repos.jcenter();

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

    private static void assertRepositoryURIIsSecure(final String repositoryName, final String projectPath, final URI uri) {
        if (uri != null && SECURE_URL_SCHEMES.contains(uri.getScheme()) == false) {
            String url;
            try {
                url = uri.toURL().toString();
            } catch (MalformedURLException e) {
                throw new IllegalStateException(e);
            }
            final String message = String.format(
                Locale.ROOT,
                "repository [%s] on project with path [%s] is not using a secure protocol for artifacts on [%s]",
                repositoryName,
                projectPath,
                url
            );
            throw new GradleException(message);
        }
    }

}
