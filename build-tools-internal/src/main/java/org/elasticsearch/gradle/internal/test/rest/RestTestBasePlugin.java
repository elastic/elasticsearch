/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest;

import groovy.lang.Closure;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.ElasticsearchDistributionType;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes;
import org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin;
import org.elasticsearch.gradle.internal.InternalDistributionDownloadPlugin;
import org.elasticsearch.gradle.internal.test.ClusterFeaturesMetadataPlugin;
import org.elasticsearch.gradle.internal.test.ErrorReportingTestListener;
import org.elasticsearch.gradle.plugin.BasePluginBuildPlugin;
import org.elasticsearch.gradle.plugin.PluginBuildPlugin;
import org.elasticsearch.gradle.plugin.PluginPropertiesExtension;
import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask;
import org.elasticsearch.gradle.transform.UnzipTransform;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.DependencySet;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.internal.artifacts.dependencies.ProjectDependencyInternal;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.ClasspathNormalizer;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.util.PatternFilterable;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * Base plugin used for wiring up build tasks to REST testing tasks using new JUnit rule-based test clusters framework.
 */
public class RestTestBasePlugin implements Plugin<Project> {

    private static final String TESTS_MAX_PARALLEL_FORKS_SYSPROP = "tests.max.parallel.forks";
    private static final String DEFAULT_DISTRIBUTION_SYSPROP = "tests.default.distribution";
    private static final String INTEG_TEST_DISTRIBUTION_SYSPROP = "tests.integ-test.distribution";
    private static final String BWC_SNAPSHOT_DISTRIBUTION_SYSPROP_PREFIX = "tests.snapshot.distribution.";
    private static final String BWC_RELEASED_DISTRIBUTION_SYSPROP_PREFIX = "tests.release.distribution.";
    private static final String TESTS_CLUSTER_MODULES_PATH_SYSPROP = "tests.cluster.modules.path";
    private static final String TESTS_CLUSTER_PLUGINS_PATH_SYSPROP = "tests.cluster.plugins.path";
    private static final String DEFAULT_REST_INTEG_TEST_DISTRO = "default_distro";
    private static final String INTEG_TEST_REST_INTEG_TEST_DISTRO = "integ_test_distro";
    private static final String MODULES_CONFIGURATION = "clusterModules";
    private static final String PLUGINS_CONFIGURATION = "clusterPlugins";
    private static final String EXTRACTED_PLUGINS_CONFIGURATION = "extractedPlugins";
    private static final Attribute<String> CONFIGURATION_ATTRIBUTE = Attribute.of("test-cluster-artifacts", String.class);
    private static final String FEATURES_METADATA_CONFIGURATION = "featuresMetadataDeps";
    private static final String DEFAULT_DISTRO_FEATURES_METADATA_CONFIGURATION = "defaultDistrofeaturesMetadataDeps";
    private static final String TESTS_FEATURES_METADATA_PATH = "tests.features.metadata.path";
    private static final String MINIMUM_WIRE_COMPATIBLE_VERSION_SYSPROP = "tests.minimum.wire.compatible";

    private final ProviderFactory providerFactory;

    @Inject
    public RestTestBasePlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        project.getPluginManager().apply(InternalDistributionDownloadPlugin.class);
        var bwcVersions = loadBuildParams(project).get().getBwcVersions();

        // Register integ-test and default distributions
        ElasticsearchDistribution defaultDistro = createDistribution(
            project,
            DEFAULT_REST_INTEG_TEST_DISTRO,
            VersionProperties.getElasticsearch()
        );
        ElasticsearchDistribution integTestDistro = createDistribution(
            project,
            INTEG_TEST_REST_INTEG_TEST_DISTRO,
            VersionProperties.getElasticsearch(),
            ElasticsearchDistributionTypes.INTEG_TEST_ZIP
        );

        // Create configures for module and plugin dependencies
        Configuration modulesConfiguration = createPluginConfiguration(project, MODULES_CONFIGURATION, true, false);
        Configuration pluginsConfiguration = createPluginConfiguration(project, PLUGINS_CONFIGURATION, false, false);
        Configuration extractedPluginsConfiguration = createPluginConfiguration(project, EXTRACTED_PLUGINS_CONFIGURATION, true, true);
        extractedPluginsConfiguration.extendsFrom(pluginsConfiguration);
        configureArtifactTransforms(project);

        // Create configuration for aggregating feature metadata
        FileCollection featureMetadataConfig = project.getConfigurations().create(FEATURES_METADATA_CONFIGURATION, c -> {
            c.setCanBeConsumed(false);
            c.setCanBeResolved(true);
            c.attributes(
                a -> a.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ClusterFeaturesMetadataPlugin.FEATURES_METADATA_TYPE)
            );
            c.defaultDependencies(d -> d.add(project.getDependencies().project(Map.of("path", ":server"))));
            c.withDependencies(dependencies -> {
                // We can't just use Configuration#extendsFrom() here as we'd inherit the wrong project configuration
                copyDependencies(project, dependencies, modulesConfiguration);
                copyDependencies(project, dependencies, pluginsConfiguration);
            });
        });

        FileCollection defaultDistroFeatureMetadataConfig = project.getConfigurations()
            .create(DEFAULT_DISTRO_FEATURES_METADATA_CONFIGURATION, c -> {
                c.setCanBeConsumed(false);
                c.setCanBeResolved(true);
                c.attributes(
                    a -> a.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ClusterFeaturesMetadataPlugin.FEATURES_METADATA_TYPE)
                );
                c.defaultDependencies(
                    d -> d.add(project.getDependencies().project(Map.of("path", ":distribution", "configuration", "featuresMetadata")))
                );
            });

        // For plugin and module projects, register the current project plugin bundle as a dependency
        project.getPluginManager().withPlugin("elasticsearch.esplugin", plugin -> {
            if (GradleUtils.isModuleProject(project.getPath())) {
                project.getDependencies().add(MODULES_CONFIGURATION, getExplodedBundleDependency(project, project.getPath()));
            } else {
                project.getDependencies().add(PLUGINS_CONFIGURATION, getBundleZipTaskDependency(project, project.getPath()));
            }

        });

        project.getTasks().withType(StandaloneRestIntegTestTask.class).configureEach(task -> {
            SystemPropertyCommandLineArgumentProvider nonInputSystemProperties = task.getExtensions()
                .getByType(SystemPropertyCommandLineArgumentProvider.class);

            task.dependsOn(integTestDistro, modulesConfiguration);
            registerDistributionInputs(task, integTestDistro);

            // Pass feature metadata on to tests
            task.getInputs().files(featureMetadataConfig).withPathSensitivity(PathSensitivity.NONE);
            nonInputSystemProperties.systemProperty(TESTS_FEATURES_METADATA_PATH, () -> featureMetadataConfig.getAsPath());

            // Enable parallel execution for these tests since each test gets its own cluster
            task.setMaxParallelForks(task.getProject().getGradle().getStartParameter().getMaxWorkerCount() / 2);
            nonInputSystemProperties.systemProperty(TESTS_MAX_PARALLEL_FORKS_SYSPROP, () -> String.valueOf(task.getMaxParallelForks()));

            // Disable test failure reporting since this stuff is now captured in build scans
            task.getExtensions().getByType(ErrorReportingTestListener.class).setDumpOutputOnFailure(false);

            // Disable the security manager and syscall filter since the test framework needs to fork processes
            task.systemProperty("tests.security.manager", "false");
            task.systemProperty("tests.system_call_filter", "false");

            // Pass minimum wire compatible version which is used by upgrade tests
            task.systemProperty(MINIMUM_WIRE_COMPATIBLE_VERSION_SYSPROP, bwcVersions.getMinimumWireCompatibleVersion());

            // Register plugins and modules as task inputs and pass paths as system properties to tests
            var modulePath = project.getObjects().fileCollection().from(modulesConfiguration);
            nonInputSystemProperties.systemProperty(TESTS_CLUSTER_MODULES_PATH_SYSPROP, modulePath::getAsPath);
            registerConfigurationInputs(task, modulesConfiguration.getName(), modulePath);
            var pluginPath = project.getObjects().fileCollection().from(pluginsConfiguration);
            nonInputSystemProperties.systemProperty(TESTS_CLUSTER_PLUGINS_PATH_SYSPROP, pluginPath::getAsPath);
            registerConfigurationInputs(
                task,
                extractedPluginsConfiguration.getName(),
                project.getObjects().fileCollection().from(extractedPluginsConfiguration)
            );

            // Wire up integ-test distribution by default for all test tasks
            FileCollection extracted = integTestDistro.getExtracted();
            nonInputSystemProperties.systemProperty(INTEG_TEST_DISTRIBUTION_SYSPROP, () -> extracted.getSingleFile().getPath());

            // Add `usesDefaultDistribution()` extension method to test tasks to indicate they require the default distro
            task.getExtensions().getExtraProperties().set("usesDefaultDistribution", new Closure<Void>(task) {
                @Override
                public Void call(Object... args) {
                    task.dependsOn(defaultDistro);
                    registerDistributionInputs(task, defaultDistro);

                    nonInputSystemProperties.systemProperty(
                        DEFAULT_DISTRIBUTION_SYSPROP,
                        providerFactory.provider(() -> defaultDistro.getExtracted().getSingleFile().getPath())
                    );

                    // If we are using the default distribution we need to register all module feature metadata
                    task.getInputs().files(defaultDistroFeatureMetadataConfig).withPathSensitivity(PathSensitivity.NONE);
                    nonInputSystemProperties.systemProperty(TESTS_FEATURES_METADATA_PATH, defaultDistroFeatureMetadataConfig::getAsPath);

                    return null;
                }
            });

            // Add `usesBwcDistribution(version)` extension method to test tasks to indicate they require a BWC distribution
            task.getExtensions().getExtraProperties().set("usesBwcDistribution", new Closure<Void>(task) {
                @Override
                public Void call(Object... args) {
                    if (args.length != 1 || args[0] instanceof Version == false) {
                        throw new IllegalArgumentException("Expected exactly one argument of type org.elasticsearch.gradle.Version");
                    }

                    Version version = (Version) args[0];
                    boolean isReleased = bwcVersions.unreleasedInfo(version) == null;
                    String versionString = version.toString();
                    ElasticsearchDistribution bwcDistro = createDistribution(project, "bwc_" + versionString, versionString);

                    task.dependsOn(bwcDistro);
                    registerDistributionInputs(task, bwcDistro);

                    nonInputSystemProperties.systemProperty(
                        (isReleased ? BWC_RELEASED_DISTRIBUTION_SYSPROP_PREFIX : BWC_SNAPSHOT_DISTRIBUTION_SYSPROP_PREFIX) + versionString,
                        providerFactory.provider(() -> bwcDistro.getExtracted().getSingleFile().getPath())
                    );

                    if (version.getMajor() > 0 && version.before(bwcVersions.getMinimumWireCompatibleVersion())) {
                        // If we are upgrade testing older versions we also need to upgrade to 7.last
                        this.call(bwcVersions.getMinimumWireCompatibleVersion());
                    }
                    return null;
                }
            });
        });
    }

    private void copyDependencies(Project project, DependencySet dependencies, Configuration configuration) {
        configuration.getDependencies()
            .stream()
            .filter(d -> d instanceof ProjectDependency)
            .map(d -> project.getDependencies().project(Map.of("path", ((ProjectDependencyInternal) d).getPath())))
            .forEach(dependencies::add);
    }

    private ElasticsearchDistribution createDistribution(Project project, String name, String version) {
        return createDistribution(project, name, version, null);
    }

    private ElasticsearchDistribution createDistribution(Project project, String name, String version, ElasticsearchDistributionType type) {
        NamedDomainObjectContainer<ElasticsearchDistribution> distributions = DistributionDownloadPlugin.getContainer(project);
        ElasticsearchDistribution maybeDistro = distributions.findByName(name);
        if (maybeDistro == null) {
            return distributions.create(name, distro -> {
                distro.setVersion(version);
                distro.setArchitecture(Architecture.current());
                if (type != null) {
                    distro.setType(type);
                }
            });
        } else {
            return maybeDistro;
        }
    }

    private FileTree getDistributionFiles(ElasticsearchDistribution distribution, Action<PatternFilterable> patternFilter) {
        return distribution.getExtracted().getAsFileTree().matching(patternFilter);
    }

    private void registerConfigurationInputs(Task task, String configurationName, ConfigurableFileCollection configuration) {
        task.getInputs()
            .files(providerFactory.provider(() -> configuration.getAsFileTree().filter(f -> f.getName().endsWith(".jar") == false)))
            .withPropertyName(configurationName + "-files")
            .withPathSensitivity(PathSensitivity.RELATIVE);

        task.getInputs()
            .files(providerFactory.provider(() -> configuration.getAsFileTree().filter(f -> f.getName().endsWith(".jar"))))
            .withPropertyName(configurationName + "-classpath")
            .withNormalizer(ClasspathNormalizer.class);
    }

    private void registerDistributionInputs(Task task, ElasticsearchDistribution distribution) {
        task.getInputs()
            .files(providerFactory.provider(() -> getDistributionFiles(distribution, filter -> filter.exclude("**/*.jar"))))
            .withPropertyName(distribution.getName() + "-files")
            .withPathSensitivity(PathSensitivity.RELATIVE);

        task.getInputs()
            .files(providerFactory.provider(() -> getDistributionFiles(distribution, filter -> filter.include("**/*.jar"))))
            .withPropertyName(distribution.getName() + "-classpath")
            .withNormalizer(ClasspathNormalizer.class);
    }

    private Optional<String> findModulePath(Project project, String pluginName) {
        return project.getRootProject()
            .getAllprojects()
            .stream()
            .filter(p -> GradleUtils.isModuleProject(p.getPath()))
            .filter(p -> p.getPlugins().hasPlugin(PluginBuildPlugin.class))
            .filter(p -> p.getExtensions().getByType(PluginPropertiesExtension.class).getName().equals(pluginName))
            .findFirst()
            .map(Project::getPath);
    }

    private Configuration createPluginConfiguration(Project project, String name, boolean useExploded, boolean isExtended) {
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

    private Dependency getExplodedBundleDependency(Project project, String projectPath) {
        return project.getDependencies()
            .project(Map.of("path", projectPath, "configuration", BasePluginBuildPlugin.EXPLODED_BUNDLE_CONFIG));
    }

    private Dependency getBundleZipTaskDependency(Project project, String projectPath) {
        Project dependencyProject = project.findProject(projectPath);
        return project.getDependencies()
            .create(project.files(dependencyProject.getTasks().named(BasePluginBuildPlugin.BUNDLE_PLUGIN_TASK_NAME)));
    }

    private void configureArtifactTransforms(Project project) {
        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.ZIP_TYPE);
            transformSpec.getTo().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
            transformSpec.getParameters().setAsFiletreeOutput(false);
        });
    }
}
