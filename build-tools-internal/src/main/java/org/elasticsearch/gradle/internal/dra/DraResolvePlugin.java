/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.dra;

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.repositories.IvyPatternRepositoryLayout;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;

import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static java.util.Map.Entry;

public class DraResolvePlugin implements Plugin<Project> {

    public static final String USE_DRA_ARTIFACTS_FLAG = "dra.artifacts";
    public static final String DRA_WORKFLOW = "dra.workflow";
    public static final String DRA_ARTIFACTS_DEPENDENCY_PREFIX = "dra.artifacts.dependency";
    private final ProviderFactory providerFactory;

    private final Provider<String> repositoryPrefix;

    @Inject
    public DraResolvePlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
        this.repositoryPrefix = providerFactory.systemProperty("dra.artifacts.url.repo.prefix");
    }

    @Override
    public void apply(Project project) {
        boolean useDra = providerFactory.systemProperty(USE_DRA_ARTIFACTS_FLAG).map(Boolean::parseBoolean).getOrElse(false);
        project.getExtensions().getExtraProperties().set("useDra", useDra);
        if (useDra) {
            DraWorkflow workflow = providerFactory.systemProperty(DRA_WORKFLOW).map(String::toUpperCase).map(DraWorkflow::valueOf).get();
            resolveBuildIdProperties().get().forEach((key, buildId) -> {
                System.out.println("key = " + key);
                System.out.println("buildId = " + buildId);
                System.out.println("workflow = " + workflow);
                configureDraRepository(
                    project,
                    "dra-" + workflow.name().toLowerCase() + "-artifacts-" + key,
                    key,
                    buildId,
                    repositoryPrefix.orElse(workflow.repository),
                    workflow.versionRegex
                );
            });
        }
    }

    private void configureDraRepository(
            Project project,
            String repositoryName,
            String draKey,
            String buildId,
            Provider<String> repoPrefix,
            String includeVersionRegex
    ) {

//        project.getRepositories().maven(new Action<MavenArtifactRepository>() {
//            @Override
//            public void execute(MavenArtifactRepository mavenArtifactRepository) {
//                mavenArtifactRepository.setUrl("https://artifacts-snapshot.elastic.co/ml-cpp/"+ buildId + "/maven");
//            }
//        });

        project.getRepositories().ivy(repo -> {
            repo.setName(repositoryName);
            repo.setUrl(repoPrefix.get() + "/" + draKey + "/" + buildId + "/downloads/");
            repo.metadataSources(sources -> sources.artifact());
//            repo.layout(repoPrefix.get() + "/" + draKey + "/" + buildId + "/downloads/[module]-[revision]-[classifier].[ext]");
            repo.patternLayout(patternLayout ->  {

                patternLayout.artifact("[module]-[revision]-[classifier].[ext]");
            });

//            repo.patternLayout(new Action<IvyPatternRepositoryLayout>() {
//                @Override
//                public void execute(IvyPatternRepositoryLayout ivyPatternRepositoryLayout) {
//                    String format1 = String.format("/%s/%s/downloads/%s/[module]-[revision]-[classifier].[ext]", draKey, buildId, draKey);
//                    String format2 = String.format(
//                        "/%s/%s/downloads/%s/[module]/[module]-[revision]-[classifier].[ext]",
//                        draKey,
//                        buildId,
//                        draKey
//                    );
//                    //
////                    ivyPatternRepositoryLayout.artifact(format1);
////                    ivyPatternRepositoryLayout.artifact(format2);
////                    ivyPatternRepositoryLayout.setM2compatible(true);
//                }
            });
//            String format1 = String.format("/%s/%s/downloads/%s/[module]-[revision]-[classifier].[ext]", draKey, buildId, draKey);
////            String format2 = String.format("/%s/%s/downloads/%s/[module]/[module]-[revision]-[classifier].[ext]", draKey, buildId, draKey);
//
//            repo.artifactPattern(format1);
//            repo.artifactPattern(format2);
//                System.out.println("format2 = " + format2);
//                patternLayout.artifact(format2);
//            });
//            System.out.println("repo = " + repoPrefix.get());
//            repo.metadataSources(metadataSources -> metadataSources.artifact());
//            repo.content(repositoryContentDescriptor -> repositoryContentDescriptor.includeVersionByRegex(".*", ".*", includeVersionRegex));
//            format1 = /ml-cpp/7.17.9-aff9331e/downloads/ml-cpp/[module]-[revision]-[classifier].[ext]
//            format2 = /ml-cpp/7.17.9-aff9331e/downloads/ml-cpp/[module]/[module]-[revision]-[classifier].[ext]
//            repo = https://artifacts-snapshot.elastic.co/
//                   https://artifacts-snapshot.elastic.co/ml-cpp/7.17.9-aff9331e/downloads/ml-cpp/ml-cpp-7.17.9-SNAPSHOT.zip
            ///      https://artifacts-snapshot.elastic.co/ml-cpp/7.17.9-aff9331e/downloads/ml-cpp/ml-cpp-7.17.9-SNAPSHOT.zip
//                     https://artifacts-snapshot.elastic.co/ml-cpp/7.17.9-aff9331e/maven/org/elasticsearch/ml/ml-cpp/7.17.9-SNAPSHOT/ml-cpp-7.17.9-SNAPSHOT.pom
//        });
//        });
//        });
    }

    private Provider<Map<String, String>> resolveBuildIdProperties() {
        return providerFactory.systemPropertiesPrefixedBy(DRA_ARTIFACTS_DEPENDENCY_PREFIX)
            .map(
                stringStringMap -> stringStringMap.entrySet()
                    .stream()
                    .collect(
                        Collectors.toMap(entry -> entry.getKey().substring(DRA_ARTIFACTS_DEPENDENCY_PREFIX.length() + 1), Entry::getValue)
                    )
            );
    }

    enum DraWorkflow {
        SNAPSHOT("https://artifacts-snapshot.elastic.co/", ".*SNAPSHOT"),
        STAGING("https://artifacts-staging.elastic.co/", "^(.(?!SNAPSHOT))*$"),
        RELEASE("https://artifacts.elastic.co/", "^(.(?!SNAPSHOT))*$");

        private final String repository;
        public String versionRegex;

        DraWorkflow(String repository, String versionRegex) {
            this.repository = repository;
            this.versionRegex = versionRegex;
        }
    }

}
