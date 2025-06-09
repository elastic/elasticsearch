/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTaskPlugin;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.test.MutedTestPlugin;
import org.elasticsearch.gradle.internal.test.TestUtil;
import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ResolutionStrategy;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.compile.AbstractCompile;
import org.gradle.api.tasks.compile.CompileOptions;
import org.gradle.api.tasks.compile.GroovyCompile;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.inject.Inject;

/**
 * A wrapper around Gradle's Java Base plugin that applies our
 * common configuration for production code.
 */
public class ElasticsearchJavaBasePlugin implements Plugin<Project> {

    private final JavaToolchainService javaToolchains;
    private BuildParameterExtension buildParams;

    @Inject
    ElasticsearchJavaBasePlugin(JavaToolchainService javaToolchains) {
        this.javaToolchains = javaToolchains;
    }

    @Override
    public void apply(Project project) {
        // make sure the global build info plugin is applied to the root project
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        buildParams = project.getRootProject().getExtensions().getByType(BuildParameterExtension.class);
        project.getPluginManager().apply(JavaBasePlugin.class);
        // common repositories setup
        project.getPluginManager().apply(RepositoriesSetupPlugin.class);
        project.getPluginManager().apply(ElasticsearchTestBasePlugin.class);
        project.getPluginManager().apply(PrecommitTaskPlugin.class);
        project.getPluginManager().apply(MutedTestPlugin.class);

        configureConfigurations(project);
        configureCompile(project);
        configureInputNormalization(project);
        configureNativeLibraryPath(project);
        configureEntitlementAgent(project);

        // convenience access to common versions used in dependencies
        project.getExtensions().getExtraProperties().set("versions", VersionProperties.getVersions());
    }

    /**
     * Makes dependencies non-transitive.
     * <p>
     * Gradle allows setting all dependencies as non-transitive very easily.
     * Sadly this mechanism does not translate into maven pom generation. In order
     * to effectively make the pom act as if it has no transitive dependencies,
     * we must exclude each transitive dependency of each direct dependency.
     * <p>
     * Determining the transitive deps of a dependency which has been resolved as
     * non-transitive is difficult because the process of resolving removes the
     * transitive deps. To sidestep this issue, we create a configuration per
     * direct dependency version. This specially named and unique configuration
     * will contain all of the transitive dependencies of this particular
     * dependency. We can then use this configuration during pom generation
     * to iterate the transitive dependencies and add excludes.
     */
    public static void configureConfigurations(Project project) {
        // we are not shipping these jars, we act like dumb consumers of these things
        if (project.getPath().startsWith(":test:fixtures") || project.getPath().equals(":build-tools")) {
            return;
        }
        // fail on any conflicting dependency versions
        project.getConfigurations().all(configuration -> {
            if (configuration.getName().endsWith("Fixture")) {
                // just a self contained test-fixture configuration, likely transitive and hellacious
                return;
            }
            configuration.resolutionStrategy(ResolutionStrategy::failOnVersionConflict);
        });

        // disable transitive dependency management
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        sourceSets.all(sourceSet -> disableTransitiveDependenciesForSourceSet(project, sourceSet));
    }

    private static void disableTransitiveDependenciesForSourceSet(Project project, SourceSet sourceSet) {
        List<String> sourceSetConfigurationNames = List.of(
            sourceSet.getApiConfigurationName(),
            sourceSet.getImplementationConfigurationName(),
            sourceSet.getCompileOnlyConfigurationName(),
            sourceSet.getRuntimeOnlyConfigurationName()
        );

        project.getConfigurations()
            .matching(c -> sourceSetConfigurationNames.contains(c.getName()))
            .configureEach(GradleUtils::disableTransitiveDependencies);
    }

    /**
     * Adds compiler settings to the project
     */
    public void configureCompile(Project project) {
        project.getExtensions().getExtraProperties().set("compactProfile", "full");
        JavaPluginExtension java = project.getExtensions().getByType(JavaPluginExtension.class);
        if (buildParams.getJavaToolChainSpec().getOrNull() != null) {
            java.toolchain(buildParams.getJavaToolChainSpec().get());
        }
        java.setSourceCompatibility(buildParams.getMinimumRuntimeVersion());
        java.setTargetCompatibility(buildParams.getMinimumRuntimeVersion());
        project.getTasks().withType(JavaCompile.class).configureEach(compileTask -> {
            compileTask.getJavaCompiler().set(javaToolchains.compilerFor(spec -> {
                spec.getLanguageVersion().set(JavaLanguageVersion.of(buildParams.getMinimumRuntimeVersion().getMajorVersion()));
            }));

            CompileOptions compileOptions = compileTask.getOptions();
            /*
             * -path because gradle will send in paths that don't always exist.
             * -missing because we have tons of missing @returns and @param.
             * -serial because we don't use java serialization.
             */
            // don't even think about passing args with -J-xxx, oracle will ask you to submit a bug report :)
            // fail on all javac warnings.
            // TODO Discuss moving compileOptions.getCompilerArgs() to use provider api with Gradle team.
            List<String> compilerArgs = compileOptions.getCompilerArgs();
            compilerArgs.add("-Werror");
            compilerArgs.add("-Xlint:all,-path,-serial,-options,-deprecation,-try,-removal");
            compilerArgs.add("-Xdoclint:all");
            compilerArgs.add("-Xdoclint:-missing");
            compileOptions.setEncoding("UTF-8");
            compileOptions.setIncremental(true);
            // workaround for https://github.com/gradle/gradle/issues/14141
            compileTask.getConventionMapping().map("sourceCompatibility", () -> java.getSourceCompatibility().toString());
            compileTask.getConventionMapping().map("targetCompatibility", () -> java.getTargetCompatibility().toString());
            compileOptions.getRelease().set(releaseVersionProviderFromCompileTask(project, compileTask));
            compileOptions.setIncremental(buildParams.getCi() == false);
        });
        // also apply release flag to groovy, which is used in build-tools
        project.getTasks().withType(GroovyCompile.class).configureEach(compileTask -> {
            // TODO: this probably shouldn't apply to groovy at all?
            compileTask.getOptions().getRelease().set(releaseVersionProviderFromCompileTask(project, compileTask));
        });
    }

    /**
     * Apply runtime classpath input normalization so that changes in JAR manifests don't break build cacheability
     */
    public static void configureInputNormalization(Project project) {
        project.getNormalization().getRuntimeClasspath().ignore("META-INF/MANIFEST.MF");
        project.getNormalization().getRuntimeClasspath().ignore("IMPL-JARS/**/META-INF/MANIFEST.MF");
    }

    private static void configureNativeLibraryPath(Project project) {
        String nativeProject = ":libs:native:native-libraries";
        Configuration nativeConfig = project.getConfigurations().create("nativeLibs");
        nativeConfig.defaultDependencies(deps -> {
            deps.add(project.getDependencies().project(Map.of("path", nativeProject, "configuration", "default")));
        });
        // This input to the following lambda needs to be serializable. Configuration is not serializable, but FileCollection is.
        FileCollection nativeConfigFiles = nativeConfig;

        project.getTasks().withType(Test.class).configureEach(test -> {
            var systemProperties = test.getExtensions().getByType(SystemPropertyCommandLineArgumentProvider.class);
            var libraryPath = (Supplier<String>) () -> TestUtil.getTestLibraryPath(nativeConfigFiles.getAsPath());

            test.dependsOn(nativeConfigFiles);
            systemProperties.systemProperty("es.nativelibs.path", libraryPath);
        });
    }

    private static void configureEntitlementAgent(Project project) {
        String agentProject = ":libs:entitlement:agent";
        Configuration agentJarConfig = project.getConfigurations().create("entitlementAgentJar");
        agentJarConfig.defaultDependencies(deps -> {
            deps.add(project.getDependencies().project(Map.of("path", agentProject, "configuration", "default")));
        });
        // This input to the following lambda needs to be serializable. Configuration is not serializable, but FileCollection is.
        FileCollection agentFiles = agentJarConfig;

        String bridgeProject = ":libs:entitlement:bridge";
        Configuration bridgeJarConfig = project.getConfigurations().create("entitlementBridgeJar");
        bridgeJarConfig.defaultDependencies(deps -> {
            deps.add(project.getDependencies().project(Map.of("path", bridgeProject, "configuration", "default")));
        });
        // This input to the following lambda needs to be serializable. Configuration is not serializable, but FileCollection is.
        FileCollection bridgeFiles = bridgeJarConfig;

        // project.getGradle().allprojects(subproject -> {
        // subproject.getPluginManager().withPlugin("java", plugin -> {
        // subproject.getDependencies().add("testRuntimeOnly", project.project(bridgeProject));
        // });
        // });

        project.getTasks().withType(Test.class).configureEach(test -> {
            // See also SystemJvmOptions.maybeAttachEntitlementAgent.

            // Agent
            var systemProperties = test.getExtensions().getByType(SystemPropertyCommandLineArgumentProvider.class);
            test.dependsOn(agentFiles);
            systemProperties.systemProperty("es.entitlement.agentJar", agentFiles.getAsPath());
            systemProperties.systemProperty("jdk.attach.allowAttachSelf", true);

            // Bridge
            String modulesContainingEntitlementInstrumentation = "java.logging,java.net.http,java.naming,jdk.net";
            test.dependsOn(bridgeFiles);
            // Tests may not be modular, but the JDK still is
            System.out.println("PATDOYLE: --patch-module=java.base=" + bridgeFiles.getAsPath());
            System.out.println(
                "--add-exports=java.base/org.elasticsearch.entitlement.bridge=org.elasticsearch.entitlement,"
                    + modulesContainingEntitlementInstrumentation
            );
            // test.jvmArgs("--patch-module=java.base=" + bridgeFiles.getAsPath());
            test.jvmArgs(
                "--add-exports=java.base/org.elasticsearch.entitlement.bridge=ALL-UNNAMED," + modulesContainingEntitlementInstrumentation
            );
        });
    }

    private static Provider<Integer> releaseVersionProviderFromCompileTask(Project project, AbstractCompile compileTask) {
        return project.provider(() -> {
            JavaVersion javaVersion = JavaVersion.toVersion(compileTask.getTargetCompatibility());
            return Integer.parseInt(javaVersion.getMajorVersion());
        });
    }

}
