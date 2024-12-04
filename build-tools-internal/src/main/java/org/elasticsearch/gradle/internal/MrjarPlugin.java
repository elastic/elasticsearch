/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.precommit.CheckForbiddenApisTask;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.compile.CompileOptions;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.api.tasks.testing.Test;
import org.gradle.external.javadoc.CoreJavadocOptions;
import org.gradle.jvm.tasks.Jar;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.tree.ClassNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.inject.Inject;

import static de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin.FORBIDDEN_APIS_TASK_NAME;
import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;
import static org.objectweb.asm.Opcodes.V_PREVIEW;

public class MrjarPlugin implements Plugin<Project> {

    private static final Pattern MRJAR_SOURCESET_PATTERN = Pattern.compile("main(\\d{2})");
    private static final String MRJAR_IDEA_ENABLED = "org.gradle.mrjar.idea.enabled";

    private final JavaToolchainService javaToolchains;

    @Inject
    MrjarPlugin(JavaToolchainService javaToolchains) {
        this.javaToolchains = javaToolchains;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        project.getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        var buildParams = loadBuildParams(project).get();
        var javaExtension = project.getExtensions().getByType(JavaPluginExtension.class);
        var isIdeaSync = System.getProperty("idea.sync.active", "false").equals("true");
        var ideaSourceSetsEnabled = project.hasProperty(MRJAR_IDEA_ENABLED) && project.property(MRJAR_IDEA_ENABLED).equals("true");

        // Ignore version-specific source sets if we are importing into IntelliJ and have not explicitly enabled this.
        // Avoids an IntelliJ bug:
        // https://youtrack.jetbrains.com/issue/IDEA-285640/Compiler-Options-Settings-language-level-is-set-incorrectly-with-JDK-19ea
        if (isIdeaSync == false || ideaSourceSetsEnabled) {
            List<Integer> mainVersions = findSourceVersions(project);
            List<String> mainSourceSets = new ArrayList<>();
            mainSourceSets.add(SourceSet.MAIN_SOURCE_SET_NAME);
            configurePreviewFeatures(project, javaExtension.getSourceSets().getByName(SourceSet.MAIN_SOURCE_SET_NAME), 21);
            List<String> testSourceSets = new ArrayList<>(mainSourceSets);
            testSourceSets.add(SourceSet.TEST_SOURCE_SET_NAME);
            configurePreviewFeatures(project, javaExtension.getSourceSets().getByName(SourceSet.TEST_SOURCE_SET_NAME), 21);
            for (int javaVersion : mainVersions) {
                String mainSourceSetName = SourceSet.MAIN_SOURCE_SET_NAME + javaVersion;
                SourceSet mainSourceSet = addSourceSet(project, javaExtension, mainSourceSetName, mainSourceSets, javaVersion);
                configureSourceSetInJar(project, mainSourceSet, javaVersion);
                addJar(project, mainSourceSet, javaVersion);
                mainSourceSets.add(mainSourceSetName);
                testSourceSets.add(mainSourceSetName);

                String testSourceSetName = SourceSet.TEST_SOURCE_SET_NAME + javaVersion;
                SourceSet testSourceSet = addSourceSet(project, javaExtension, testSourceSetName, testSourceSets, javaVersion);
                testSourceSets.add(testSourceSetName);
                createTestTask(project, buildParams, testSourceSet, javaVersion, mainSourceSets);
            }
        }

        configureMrjar(project);
    }

    private void configureMrjar(Project project) {
        var jarTask = project.getTasks().withType(Jar.class).named(JavaPlugin.JAR_TASK_NAME);
        jarTask.configure(task -> { task.manifest(manifest -> { manifest.attributes(Map.of("Multi-Release", "true")); }); });

        project.getTasks().withType(Test.class).named(JavaPlugin.TEST_TASK_NAME).configure(testTask -> {
            testTask.dependsOn(jarTask);

            SourceSetContainer sourceSets = GradleUtils.getJavaSourceSets(project);
            FileCollection mainRuntime = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME).getOutput();
            FileCollection testRuntime = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME).getRuntimeClasspath();
            testTask.setClasspath(testRuntime.minus(mainRuntime).plus(project.files(jarTask)));
        });
    }

    private SourceSet addSourceSet(
        Project project,
        JavaPluginExtension javaExtension,
        String sourceSetName,
        List<String> parentSourceSets,
        int javaVersion
    ) {
        SourceSet sourceSet = javaExtension.getSourceSets().maybeCreate(sourceSetName);
        for (String parentSourceSetName : parentSourceSets) {
            GradleUtils.extendSourceSet(project, parentSourceSetName, sourceSetName);
        }

        project.getTasks().withType(JavaCompile.class).named(sourceSet.getCompileJavaTaskName()).configure(compileTask -> {
            compileTask.getJavaCompiler()
                .set(javaToolchains.compilerFor(spec -> { spec.getLanguageVersion().set(JavaLanguageVersion.of(javaVersion)); }));
            compileTask.setSourceCompatibility(Integer.toString(javaVersion));
            CompileOptions compileOptions = compileTask.getOptions();
            compileOptions.getRelease().set(javaVersion);
        });
        configurePreviewFeatures(project, sourceSet, javaVersion);

        // Since we configure MRJAR sourcesets to allow preview apis, class signatures for those
        // apis are not known by forbidden apis, so we must ignore all missing classes. We could, in theory,
        // run forbidden apis in a separate jvm matching the sourceset jvm, but it's not worth
        // the complexity (according to forbidden apis author!)
        String forbiddenApisTaskName = sourceSet.getTaskName(FORBIDDEN_APIS_TASK_NAME, null);
        project.getTasks().withType(CheckForbiddenApisTask.class).named(forbiddenApisTaskName).configure(forbiddenApisTask -> {
            forbiddenApisTask.setIgnoreMissingClasses(true);
        });

        return sourceSet;
    }

    private void addJar(Project project, SourceSet sourceSet, int javaVersion) {
        project.getConfigurations().register("java" + javaVersion);
        TaskProvider<Jar> jarTask = project.getTasks().register("java" + javaVersion + "Jar", Jar.class, task -> {
            task.from(sourceSet.getOutput());
        });
        project.getArtifacts().add("java" + javaVersion, jarTask);
    }

    private void configurePreviewFeatures(Project project, SourceSet sourceSet, int javaVersion) {
        project.getTasks().withType(JavaCompile.class).named(sourceSet.getCompileJavaTaskName()).configure(compileTask -> {
            CompileOptions compileOptions = compileTask.getOptions();
            compileOptions.getCompilerArgs().add("--enable-preview");
            compileOptions.getCompilerArgs().add("-Xlint:-preview");

            compileTask.doLast(t -> { stripPreviewFromFiles(compileTask.getDestinationDirectory().getAsFile().get().toPath()); });
        });
        project.getTasks().withType(Javadoc.class).named(name -> name.equals(sourceSet.getJavadocTaskName())).configureEach(javadocTask -> {
            CoreJavadocOptions options = (CoreJavadocOptions) javadocTask.getOptions();
            options.addBooleanOption("-enable-preview", true);
            options.addStringOption("-release", String.valueOf(javaVersion));
        });
    }

    private void configureSourceSetInJar(Project project, SourceSet sourceSet, int javaVersion) {
        var jarTask = project.getTasks().withType(Jar.class).named(JavaPlugin.JAR_TASK_NAME);
        jarTask.configure(task -> task.into("META-INF/versions/" + javaVersion, copySpec -> copySpec.from(sourceSet.getOutput())));
    }

    private void createTestTask(
        Project project,
        BuildParameterExtension buildParams,
        SourceSet sourceSet,
        int javaVersion,
        List<String> mainSourceSets
    ) {
        var jarTask = project.getTasks().withType(Jar.class).named(JavaPlugin.JAR_TASK_NAME);
        var testTaskProvider = project.getTasks().register(JavaPlugin.TEST_TASK_NAME + javaVersion, Test.class);
        testTaskProvider.configure(testTask -> {
            testTask.dependsOn(jarTask);

            SourceSetContainer sourceSets = GradleUtils.getJavaSourceSets(project);
            FileCollection testRuntime = sourceSet.getRuntimeClasspath();
            for (String mainSourceSetName : mainSourceSets) {
                FileCollection mainRuntime = sourceSets.getByName(mainSourceSetName).getOutput();
                testRuntime = testRuntime.minus(mainRuntime);
            }
            testTask.setClasspath(testRuntime.plus(project.files(jarTask)));
            testTask.setTestClassesDirs(sourceSet.getOutput().getClassesDirs());

            // only set the jdk if runtime java isn't set because setting the toolchain is incompatible with
            // runtime java setting the executable directly
            if (buildParams.getIsRuntimeJavaHomeSet()) {
                testTask.onlyIf("runtime java must support java " + javaVersion, t -> {
                    JavaVersion runtimeJavaVersion = buildParams.getRuntimeJavaVersion().get();
                    return runtimeJavaVersion.isCompatibleWith(JavaVersion.toVersion(javaVersion));
                });
            } else {
                testTask.getJavaLauncher()
                    .set(javaToolchains.launcherFor(spec -> spec.getLanguageVersion().set(JavaLanguageVersion.of(javaVersion))));
            }
        });

        project.getTasks().named("check").configure(checkTask -> checkTask.dependsOn(testTaskProvider));
    }

    private static List<Integer> findSourceVersions(Project project) {
        var srcDir = project.getProjectDir().toPath().resolve("src");
        List<Integer> versions = new ArrayList<>();
        try (var subdirStream = Files.list(srcDir)) {
            for (Path sourceSetPath : subdirStream.toList()) {
                assert Files.isDirectory(sourceSetPath);
                String sourcesetName = sourceSetPath.getFileName().toString();
                Matcher sourcesetMatcher = MRJAR_SOURCESET_PATTERN.matcher(sourcesetName);
                if (sourcesetMatcher.matches()) {
                    versions.add(Integer.parseInt(sourcesetMatcher.group(1)));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Collections.sort(versions);
        return versions;
    }

    private static void stripPreviewFromFiles(Path compileDir) {
        try (Stream<Path> fileStream = Files.walk(compileDir)) {
            fileStream.filter(p -> p.toString().endsWith(".class")).forEach(MrjarPlugin::maybeStripPreview);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void maybeStripPreview(Path file) {
        ClassWriter classWriter = null;
        try (var in = Files.newInputStream(file)) {
            ClassReader classReader = new ClassReader(in);
            ClassNode classNode = new ClassNode();
            classReader.accept(classNode, 0);

            if ((classNode.version & V_PREVIEW) != 0) {
                classNode.version = classNode.version & ~V_PREVIEW;
                classWriter = new ClassWriter(0);
                classNode.accept(classWriter);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (classWriter != null) {
            try (var out = Files.newOutputStream(file)) {
                out.write(classWriter.toByteArray());
            } catch (IOException e) {
                throw new org.gradle.api.UncheckedIOException(e);
            }
        }

    }
}
