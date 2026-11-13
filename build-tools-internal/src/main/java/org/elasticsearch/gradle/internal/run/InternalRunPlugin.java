/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.run;

import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes;
import org.elasticsearch.gradle.internal.InternalDistributionDownloadPlugin;
import org.elasticsearch.gradle.internal.InternalReaperPlugin;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.test.TestClusterArtifacts;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.ProviderFactory;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * Applies conventions to every {@link RunTask} in a project: the cluster runner classpath, the distribution, per-task plugin and
 * module configurations, and the defaults for ports and options.
 */
public class InternalRunPlugin implements Plugin<Project> {

    public static final String CLUSTER_RUNNER_CONFIGURATION = "clusterRunner";
    public static final String CLUSTER_RUNNER_PROJECT = ":test:test-clusters-cli";
    static final String TESTS_ES_SETTINGS_PREFIX = "tests.es.";
    // Fallback for the --license option
    private static final String RUN_LICENSE_TYPE_SYSPROP = "run.license_type";

    private static final String DEFAULT_DISTRO = "default_distro";
    private static final String INTEG_TEST_DISTRO = "integ_test_distro";
    private static final String PLUGINS_PROJECT_PREFIX = ":plugins:";
    private static final String TLS_RESOURCES = "build-tools-internal/src/main/resources/run.ssl";

    private final ProviderFactory providers;

    @Inject
    public InternalRunPlugin(ProviderFactory providers) {
        this.providers = providers;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(InternalDistributionDownloadPlugin.class);
        project.getPluginManager().apply(InternalReaperPlugin.class);
        BuildParameterExtension buildParams = loadBuildParams(project).get();

        Configuration runnerClasspath = project.getConfigurations().create(CLUSTER_RUNNER_CONFIGURATION, c -> {
            c.setCanBeConsumed(false);
            c.setCanBeResolved(true);
            c.setDescription("Classpath of the cluster runner used by run tasks");
            c.defaultDependencies(d -> d.add(project.getDependencies().project(Map.of("path", CLUSTER_RUNNER_PROJECT))));
        });

        String version = VersionProperties.getElasticsearch();
        ElasticsearchDistribution defaultDistro = TestClusterArtifacts.createDistribution(project, DEFAULT_DISTRO, version, null, false);
        ElasticsearchDistribution integTestDistro = TestClusterArtifacts.createDistribution(
            project,
            INTEG_TEST_DISTRO,
            version,
            ElasticsearchDistributionTypes.INTEG_TEST_ZIP,
            false
        );

        project.getTasks().withType(RunTask.class).configureEach(task -> {
            Configuration plugins = project.getConfigurations().create(task.getName() + "Plugins", c -> {
                c.setCanBeConsumed(false);
                c.setCanBeResolved(true);
                c.setDescription("Plugin bundles installed into the cluster started by " + task.getPath());
                c.attributes(a -> a.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.ZIP_TYPE));
                c.getDependencies()
                    .addAllLater(
                        task.getWithPlugins()
                            .orElse("")
                            .map(
                                value -> RunTask.splitPluginNames(value)
                                    .stream()
                                    .map(name -> pluginDependency(project, PLUGINS_PROJECT_PREFIX + name))
                                    .toList()
                            )
                    );
                c.getDependencies()
                    .addAllLater(
                        task.getPluginProjects().map(paths -> paths.stream().map(path -> pluginDependency(project, path)).toList())
                    );
            });
            Configuration modules = TestClusterArtifacts.createPluginConfiguration(project, task.getName() + "Modules", true, false);
            modules.setDescription("Module bundles installed into the cluster started by " + task.getPath());
            modules.getDependencies()
                .addAllLater(
                    task.getModuleProjects()
                        .map(paths -> paths.stream().map(path -> project.getDependencies().project(Map.of("path", path))).toList())
                );
            task.setConfigurationNames(plugins.getName(), modules.getName());

            task.getClusterName().convention("runTask");
            task.getDistribution().convention("DEFAULT");
            task.getLicense().convention(providers.systemProperty(RUN_LICENSE_TYPE_SYSPROP));
            task.getNumberOfNodes().convention(1);
            task.getHttpPort().convention(9200);
            task.getTransportPort().convention(9300);
            task.getDebugServerPort().convention(5007);
            task.getDebugCliPort().convention(5107);
            task.getHeapSize().convention(providers.systemProperty("tests.heap.size").orElse("512m"));
            task.getJvmArgline().convention(providers.systemProperty("tests.jvm.argline"));
            task.getCommandLineSettings()
                .convention(
                    providers.systemPropertiesPrefixedBy(TESTS_ES_SETTINGS_PREFIX)
                        .map(
                            properties -> properties.entrySet()
                                .stream()
                                .collect(
                                    Collectors.toMap(e -> e.getKey().substring(TESTS_ES_SETTINGS_PREFIX.length()), Map.Entry::getValue)
                                )
                        )
                );
            task.getWorkingDir().convention(project.getLayout().getBuildDirectory().dir("testclusters"));
            task.getTlsResourcesDir().convention(project.getLayout().getSettingsDirectory().dir(TLS_RESOURCES));

            task.getDebugJvm().convention(false);
            task.getDebugCliJvm().convention(false);
            task.getPreserveData().convention(false);
            task.getUseHttps().convention(false);
            task.getUseTransportTls().convention(false);
            task.getApmServerEnabled().convention(false);

            task.getRunnerClasspath().from(runnerClasspath);
            task.getDistributionDir().from(task.getDistribution().map(d -> distributionFiles(d, defaultDistro, integTestDistro)));
            task.getPluginsPath().from(plugins);
            task.getModulesPath().from(modules);
            task.getRuntimeJavaHome().convention(buildParams.getRuntimeJavaHome());
            task.getSnapshotBuild().convention(buildParams.getSnapshotBuild());
        });
    }

    /**
     * Registers a {@code run} task for a plugin or module project which runs a cluster with the project's bundle installed.
     * Must be called before the plugin build plugin is applied, which otherwise registers a legacy {@code run} task. Does nothing
     * if the cluster runner project isn't part of the build.
     */
    public static void registerPluginRunTask(Project project) {
        if (project.findProject(CLUSTER_RUNNER_PROJECT) == null) {
            return;
        }
        project.getPluginManager().apply(InternalRunPlugin.class);
        project.getTasks().register("run", RunTask.class, task -> {
            task.setDescription("Runs elasticsearch in the foreground with this plugin installed");
            task.setGroup("Verification");
            task.getDistribution().convention("INTEG_TEST");
            if (GradleUtils.isModuleProject(project.getPath())) {
                project.getDependencies()
                    .add(task.modulesConfigurationName(), TestClusterArtifacts.getExplodedBundleDependency(project, project.getPath()));
            } else {
                project.getDependencies()
                    .add(task.pluginsConfigurationName(), TestClusterArtifacts.getBundleZipTaskDependency(project, project.getPath()));
            }
        });
    }

    private static FileCollection distributionFiles(
        String distribution,
        ElasticsearchDistribution defaultDistro,
        ElasticsearchDistribution integTestDistro
    ) {
        return switch (distribution.toUpperCase(Locale.ROOT)) {
            case "DEFAULT" -> defaultDistro.getExtracted();
            case "INTEG_TEST" -> integTestDistro.getExtracted();
            default -> throw new IllegalArgumentException(
                "Unsupported run distribution [" + distribution + "], expected [default] or [integ_test]"
            );
        };
    }

    private static Dependency pluginDependency(Project project, String projectPath) {
        return project.getDependencies().project(Map.of("path", projectPath, "configuration", "zip"));
    }
}
