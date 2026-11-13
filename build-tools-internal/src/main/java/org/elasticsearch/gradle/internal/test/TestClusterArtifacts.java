/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.ElasticsearchDistributionType;
import org.elasticsearch.gradle.plugin.BasePluginBuildPlugin;
import org.elasticsearch.gradle.plugin.PluginBuildPlugin;
import org.elasticsearch.gradle.plugin.PluginPropertiesExtension;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Attribute;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Helpers for resolving the distributions, plugins and modules consumed by clusters started with the {@code :test:test-clusters}
 * library, shared by the REST test plugins and the {@code run} tasks so both resolve cluster artifacts the same way.
 */
public final class TestClusterArtifacts {

    private static final Attribute<String> CONFIGURATION_ATTRIBUTE = Attribute.of("test-cluster-artifacts", String.class);

    private TestClusterArtifacts() {}

    /**
     * Returns the distribution with the given name, registering it with the given version and type if it doesn't exist yet.
     * A {@code null} type uses the default distribution type.
     */
    public static ElasticsearchDistribution createDistribution(
        Project project,
        String name,
        String version,
        ElasticsearchDistributionType type,
        boolean detachedVersion
    ) {
        NamedDomainObjectContainer<ElasticsearchDistribution> distributions = DistributionDownloadPlugin.getContainer(project);
        ElasticsearchDistribution maybeDistro = distributions.findByName(name);
        if (maybeDistro == null) {
            return distributions.create(name, distro -> {
                distro.setVersion(version);
                distro.setDetachedVersion(detachedVersion);
                distro.setArchitecture(Architecture.current());
                if (type != null) {
                    distro.setType(type);
                }
            });
        } else {
            return maybeDistro;
        }
    }

    /**
     * Creates a configuration for plugin or module bundles.
     * <p>
     * Unless {@code isExtended} is set, bare project dependencies are swapped for the project's bundle (exploded or zip), and the
     * modules of any plugins they extend are added as well.
     */
    public static Configuration createPluginConfiguration(Project project, String name, boolean useExploded, boolean isExtended) {
        return project.getConfigurations().create(name, c -> {
            c.attributes(a -> a.attribute(CONFIGURATION_ATTRIBUTE, name));
            if (useExploded) {
                c.attributes(a -> a.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE));
            } else {
                c.attributes(a -> a.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.ZIP_TYPE));
            }
            if (isExtended == false) {
                c.withDependencies(dependencies -> {
                    // Add dependencies of any modules
                    Collection<Dependency> additionalDependencies = new LinkedHashSet<>();
                    for (Iterator<Dependency> iterator = dependencies.iterator(); iterator.hasNext();) {
                        Dependency dependency = iterator.next();
                        // this logic of relying on other projects metadata should probably live in a build service
                        if (dependency instanceof ProjectDependency projectDependency) {
                            Project dependencyProject = project.project(projectDependency.getPath());
                            List<String> extendedPlugins = dependencyProject.getExtensions()
                                .getByType(PluginPropertiesExtension.class)
                                .getExtendedPlugins();

                            // Replace project dependency with explicit dependency on exploded configuration to workaround variant bug
                            if (projectDependency.getTargetConfiguration() == null) {
                                iterator.remove();
                                additionalDependencies.add(
                                    useExploded
                                        ? getExplodedBundleDependency(project, projectDependency.getPath())
                                        : getBundleZipTaskDependency(project, projectDependency.getPath())
                                );
                            }

                            for (String extendedPlugin : extendedPlugins) {
                                findModulePath(project, extendedPlugin).ifPresent(
                                    modulePath -> additionalDependencies.add(
                                        useExploded
                                            ? getExplodedBundleDependency(project, modulePath)
                                            : getBundleZipTaskDependency(project, modulePath)
                                    )
                                );
                            }
                        }
                    }

                    dependencies.addAll(additionalDependencies);
                });
            }
        });
    }

    /**
     * A dependency on the exploded plugin bundle of the given project.
     */
    public static Dependency getExplodedBundleDependency(Project project, String projectPath) {
        return project.getDependencies()
            .project(Map.of("path", projectPath, "configuration", BasePluginBuildPlugin.EXPLODED_BUNDLE_CONFIG));
    }

    /**
     * A dependency on the plugin bundle zip produced by the given project.
     */
    public static Dependency getBundleZipTaskDependency(Project project, String projectPath) {
        Project dependencyProject = project.findProject(projectPath);
        return project.getDependencies()
            .create(project.files(dependencyProject.getTasks().named(BasePluginBuildPlugin.BUNDLE_PLUGIN_TASK_NAME)));
    }

    private static Optional<String> findModulePath(Project project, String pluginName) {
        return project.getRootProject()
            .getAllprojects()
            .stream()
            .filter(p -> GradleUtils.isModuleProject(p.getPath()))
            .filter(p -> p.getPlugins().hasPlugin(PluginBuildPlugin.class))
            .filter(p -> p.getExtensions().getByType(PluginPropertiesExtension.class).getName().equals(pluginName))
            .findFirst()
            .map(Project::getPath);
    }
}
