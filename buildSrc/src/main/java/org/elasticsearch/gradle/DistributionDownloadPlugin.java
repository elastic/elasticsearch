/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.ElasticsearchDistribution.Platform;
import org.elasticsearch.gradle.internal.docker.DockerSupportPlugin;
import org.elasticsearch.gradle.internal.docker.DockerSupportService;
import org.elasticsearch.gradle.transform.SymbolicLinkPreservingUntarTransform;
import org.elasticsearch.gradle.transform.UnzipTransform;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.internal.artifacts.ArtifactAttributes;
import org.gradle.api.provider.Provider;

import java.util.Comparator;

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
    public static final String DISTRO_EXTRACTED_CONFIG_PREFIX = "es_distro_extracted_";

    private NamedDomainObjectContainer<ElasticsearchDistribution> distributionsContainer;
    private NamedDomainObjectContainer<DistributionResolution> distributionsResolutionStrategiesContainer;

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(DockerSupportPlugin.class);
        Provider<DockerSupportService> dockerSupport = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME
        );

        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.ZIP_TYPE);
            transformSpec.getTo().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        ArtifactTypeDefinition tarArtifactTypeDefinition = project.getDependencies().getArtifactTypes().maybeCreate("tar.gz");
        project.getDependencies().registerTransform(SymbolicLinkPreservingUntarTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactAttributes.ARTIFACT_FORMAT, tarArtifactTypeDefinition.getName());
            transformSpec.getTo().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        setupResolutionsContainer(project);
        setupDistributionContainer(project, dockerSupport);
        setupDownloadServiceRepo(project);
    }

    private void setupDistributionContainer(Project project, Provider<DockerSupportService> dockerSupport) {
        distributionsContainer = project.container(ElasticsearchDistribution.class, name -> {
            Configuration fileConfiguration = project.getConfigurations().create("es_distro_file_" + name);
            Configuration extractedConfiguration = project.getConfigurations().create(DISTRO_EXTRACTED_CONFIG_PREFIX + name);
            extractedConfiguration.getAttributes().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
            return new ElasticsearchDistribution(
                name,
                project.getObjects(),
                dockerSupport,
                fileConfiguration,
                extractedConfiguration,
                (dist) -> finalizeDistributionDependencies(project, dist)
            );
        });
        project.getExtensions().add(CONTAINER_NAME, distributionsContainer);
    }

    private void setupResolutionsContainer(Project project) {
        distributionsResolutionStrategiesContainer = project.container(DistributionResolution.class);
        // We want this ordered in the same resolution strategies are added
        distributionsResolutionStrategiesContainer.whenObjectAdded(
            resolveDependencyNotation -> resolveDependencyNotation.setPriority(distributionsResolutionStrategiesContainer.size())
        );
        project.getExtensions().add(RESOLUTION_CONTAINER_NAME, distributionsResolutionStrategiesContainer);
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<ElasticsearchDistribution> getContainer(Project project) {
        return (NamedDomainObjectContainer<ElasticsearchDistribution>) project.getExtensions().getByName(CONTAINER_NAME);
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<DistributionResolution> getRegistrationsContainer(Project project) {
        return (NamedDomainObjectContainer<DistributionResolution>) project.getExtensions().getByName(RESOLUTION_CONTAINER_NAME);
    }

    private void finalizeDistributionDependencies(Project project, ElasticsearchDistribution distribution) {
        DependencyHandler dependencies = project.getDependencies();
        // for the distribution as a file, just depend on the artifact directly
        DistributionDependency distributionDependency = resolveDependencyNotation(project, distribution);
        dependencies.add(distribution.configuration.getName(), distributionDependency.getDefaultNotation());
        // no extraction needed for rpm, deb or docker
        if (distribution.getType().shouldExtract()) {
            // The extracted configuration depends on the artifact directly but has
            // an artifact transform registered to resolve it as an unpacked folder.
            dependencies.add(distribution.getExtracted().getName(), distributionDependency.getExtractedNotation());
        }
    }

    private DistributionDependency resolveDependencyNotation(Project p, ElasticsearchDistribution distribution) {
        return distributionsResolutionStrategiesContainer.stream()
            .sorted(Comparator.comparingInt(DistributionResolution::getPriority))
            .map(r -> r.getResolver().resolve(p, distribution))
            .filter(d -> d != null)
            .findFirst()
            .orElseGet(() -> DistributionDependency.of(dependencyNotation(distribution)));
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

    /**
     * Returns a dependency object representing the given distribution.
     * <p>
     * The returned object is suitable to be passed to {@link DependencyHandler}.
     * The concrete type of the object will be a set of maven coordinates as a {@link String}.
     * Maven coordinates point to either the integ-test-zip coordinates on maven central, or a set of artificial
     * coordinates that resolve to the Elastic download service through an ivy repository.
     */
    private String dependencyNotation(ElasticsearchDistribution distribution) {
        if (distribution.getType().isIntegTestZip()) {
            return "org.elasticsearch.distribution.integ-test-zip:elasticsearch:" + distribution.getVersion() + "@zip";
        }

        Version distroVersion = Version.fromString(distribution.getVersion());
        String extension = distribution.getType().toString();
        String classifier = ":" + Architecture.current().classifier;
        if (distribution.getType().isArchive()) {
            extension = distribution.getPlatform() == Platform.WINDOWS ? "zip" : "tar.gz";
            if (distroVersion.onOrAfter("7.0.0")) {
                classifier = ":" + distribution.getPlatform() + "-" + Architecture.current().classifier;
            } else {
                classifier = "";
            }
        } else if (distribution.getType().isDeb()) {
            classifier = ":amd64";
        }
        String group = distribution.getVersion().endsWith("-SNAPSHOT") ? FAKE_SNAPSHOT_IVY_GROUP : FAKE_IVY_GROUP;
        return group + ":elasticsearch" + ":" + distribution.getVersion() + classifier + "@" + extension;
    }
}
