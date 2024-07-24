/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes;
import org.elasticsearch.gradle.transform.SymbolicLinkPreservingUntarTransform;
import org.elasticsearch.gradle.transform.UnzipTransform;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

/**
 * A plugin to manage getting and extracting distributions of Elasticsearch.
 * <p>
 * The plugin provides hooks to register custom distribution resolutions.
 * This plugin resolves distributions from the Elastic downloads service if
 * no registered resolution strategy can resolve to a distribution.
 */
public class DistributionDownloadPlugin implements Plugin<Project> {

    static final String RESOLUTION_CONTAINER_NAME = "elasticsearch_distributions_resolutions";
    private static final String CONTAINER_NAME = "elasticsearch_distributions";
    private static final String FAKE_IVY_GROUP = "elasticsearch-distribution";
    private static final String FAKE_SNAPSHOT_IVY_GROUP = "elasticsearch-distribution-snapshot";
    private static final String DOWNLOAD_REPO_NAME = "elasticsearch-downloads";
    private static final String SNAPSHOT_REPO_NAME = "elasticsearch-snapshots";

    public static final String ES_DISTRO_CONFIG_PREFIX = "es_distro_";
    public static final String DISTRO_EXTRACTED_CONFIG_PREFIX = ES_DISTRO_CONFIG_PREFIX + "extracted_";
    public static final String DISTRO_CONFIG_PREFIX = ES_DISTRO_CONFIG_PREFIX + "file_";

    private final ObjectFactory objectFactory;
    private NamedDomainObjectContainer<ElasticsearchDistribution> distributionsContainer;
    private List<DistributionResolution> distributionsResolutionStrategies;

    private Property<Boolean> dockerAvailability;

    private boolean writingDependencies = false;

    @Inject
    public DistributionDownloadPlugin(ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;
        this.dockerAvailability = objectFactory.property(Boolean.class).value(false);
    }

    public void setDockerAvailability(Provider<Boolean> dockerAvailability) {
        this.dockerAvailability.set(dockerAvailability);
    }

    @Override
    public void apply(Project project) {
        writingDependencies = project.getGradle().getStartParameter().getWriteDependencyVerifications().isEmpty() == false;
        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.ZIP_TYPE);
            transformSpec.getTo().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        var tarArtifactTypeDefinition = project.getDependencies().getArtifactTypes().maybeCreate("tar.gz");
        project.getDependencies().registerTransform(SymbolicLinkPreservingUntarTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, tarArtifactTypeDefinition.getName());
            transformSpec.getTo().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        setupResolutionsContainer(project);
        setupDistributionContainer(project);
        setupDownloadServiceRepo(project);
    }

    private void setupDistributionContainer(Project project) {
        distributionsContainer = project.container(ElasticsearchDistribution.class, name -> {
            var fileConfiguration = project.getConfigurations().create(DISTRO_CONFIG_PREFIX + name);
            var extractedConfiguration = project.getConfigurations().create(DISTRO_EXTRACTED_CONFIG_PREFIX + name);
            extractedConfiguration.getAttributes()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
            var distribution = new ElasticsearchDistribution(
                name,
                objectFactory,
                dockerAvailability,
                objectFactory.fileCollection().from(fileConfiguration),
                objectFactory.fileCollection().from(extractedConfiguration)
            );

            // when running with --write-dependency-verification to update dependency verification data,
            // we do not register the dependencies as we ignore elasticsearch internal dependencies anyhow and
            // want to reduce general resolution time
            if (writingDependencies == false) {
                registerDistributionDependencies(project, distribution);
            }
            return distribution;
        });
        project.getExtensions().add(CONTAINER_NAME, distributionsContainer);
    }

    private void registerDistributionDependencies(Project project, ElasticsearchDistribution distribution) {
        Configuration distroConfig = project.getConfigurations().getByName(DISTRO_CONFIG_PREFIX + distribution.getName());
        distroConfig.getDependencies()
            .addLater(
                project.provider(() -> distribution.maybeFreeze())
                    .map(
                        frozenDistro -> project.getDependencies()
                            .create(resolveDependencyNotation(project, frozenDistro).getDefaultNotation())
                    )
            );

        Configuration extractedDistroConfig = project.getConfigurations()
            .getByName(DISTRO_EXTRACTED_CONFIG_PREFIX + distribution.getName());
        extractedDistroConfig.getDependencies()
            .addAllLater(
                project.provider(() -> distribution.maybeFreeze())
                    .map(
                        frozenDistro -> distribution.getType().shouldExtract()
                            ? List.of(
                                project.getDependencies().create(resolveDependencyNotation(project, frozenDistro).getExtractedNotation())
                            )
                            : Collections.emptyList()
                    )
            );
    }

    private DistributionDependency resolveDependencyNotation(Project project, ElasticsearchDistribution distro) {
        return distributionsResolutionStrategies.stream()
            .map(r -> r.getResolver().resolve(project, distro))
            .filter(d -> d != null)
            .findFirst()
            .orElseGet(() -> DistributionDependency.of(dependencyNotation(distro)));
    }

    /**
     * Returns a dependency object representing the given distribution.
     * <p>
     * The returned object is suitable to be passed to {@link DependencyHandler}.
     * The concrete type of the object will be a set of maven coordinates as a {@link String}.
     * Maven coordinates point to either the integ-test-zip coordinates on maven central, or a set of artificial
     * coordinates that resolve to the Elastic download service through an ivy repository.
     */
    private static String dependencyNotation(ElasticsearchDistribution distribution) {
        if (distribution.getType() == ElasticsearchDistributionTypes.INTEG_TEST_ZIP) {
            return "org.elasticsearch.distribution.integ-test-zip:elasticsearch:" + distribution.getVersion() + "@zip";
        }
        var distroVersion = Version.fromString(distribution.getVersion());
        var extension = distribution.getType().getExtension(distribution.getPlatform());
        var classifier = distribution.getType().getClassifier(distribution.getPlatform(), distroVersion);
        var group = distribution.getVersion().endsWith("-SNAPSHOT") ? FAKE_SNAPSHOT_IVY_GROUP : FAKE_IVY_GROUP;
        return group + ":elasticsearch" + ":" + distribution.getVersion() + classifier + "@" + extension;
    }

    private void setupResolutionsContainer(Project project) {
        distributionsResolutionStrategies = new ArrayList<>();
        project.getExtensions().add(RESOLUTION_CONTAINER_NAME, distributionsResolutionStrategies);
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<ElasticsearchDistribution> getContainer(Project project) {
        return (NamedDomainObjectContainer<ElasticsearchDistribution>) project.getExtensions().getByName(CONTAINER_NAME);
    }

    @SuppressWarnings("unchecked")
    public static List<DistributionResolution> getRegistrationsContainer(Project project) {
        return (List<DistributionResolution>) project.getExtensions().getByName(RESOLUTION_CONTAINER_NAME);
    }

    private static void addIvyRepo(Project project, String name, String url, String group) {
        IvyArtifactRepository ivyRepo = project.getRepositories().ivy(repo -> {
            repo.setName(name);
            repo.setUrl(url);
            repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
            repo.patternLayout(layout -> layout.artifact("/downloads/elasticsearch/[module]-[revision](-[classifier]).[ext]"));
        });
        project.getRepositories().exclusiveContent(exclusiveContentRepository -> {
            exclusiveContentRepository.filter(config -> config.includeGroup(group));
            exclusiveContentRepository.forRepositories(ivyRepo);
        });
    }

    private static void setupDownloadServiceRepo(Project project) {
        if (project.getRepositories().findByName(DOWNLOAD_REPO_NAME) != null) {
            return;
        }
        addIvyRepo(project, DOWNLOAD_REPO_NAME, "https://artifacts-no-kpi.elastic.co", FAKE_IVY_GROUP);
        addIvyRepo(project, SNAPSHOT_REPO_NAME, "https://snapshots-no-kpi.elastic.co", FAKE_SNAPSHOT_IVY_GROUP);
    }

}
