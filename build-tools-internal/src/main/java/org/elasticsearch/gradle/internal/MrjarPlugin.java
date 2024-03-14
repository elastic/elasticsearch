/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.compile.CompileOptions;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;
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

import static org.objectweb.asm.Opcodes.V_PREVIEW;

public class MrjarPlugin implements Plugin<Project> {

    private static final Pattern MRJAR_SOURCESET_PATTERN = Pattern.compile("main(\\d{2})");

    private final JavaToolchainService javaToolchains;

    @Inject
    MrjarPlugin(JavaToolchainService javaToolchains) {
        this.javaToolchains = javaToolchains;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        var javaExtension = project.getExtensions().getByType(JavaPluginExtension.class);

        var srcDir = project.getProjectDir().toPath().resolve("src");
        List<Integer> mainVersions = new ArrayList<>();
        try (var subdirStream = Files.list(srcDir)) {
            for (Path sourceset : subdirStream.toList()) {
                assert Files.isDirectory(sourceset);
                String sourcesetName = sourceset.getFileName().toString();
                Matcher sourcesetMatcher = MRJAR_SOURCESET_PATTERN.matcher(sourcesetName);
                if (sourcesetMatcher.matches()) {
                    mainVersions.add(Integer.parseInt(sourcesetMatcher.group(1)));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Collections.sort(mainVersions);
        List<String> parentSourceSets = new ArrayList<>();
        parentSourceSets.add(SourceSet.MAIN_SOURCE_SET_NAME);
        for (int javaVersion : mainVersions) {
            String sourcesetName = "main" + javaVersion;
            addMrjarSourceset(project, javaExtension, sourcesetName, parentSourceSets, javaVersion);
            parentSourceSets.add(sourcesetName);
        }
    }

    private void addMrjarSourceset(
        Project project,
        JavaPluginExtension javaExtension,
        String sourcesetName,
        List<String> parentSourceSets,
        int javaVersion
    ) {
        SourceSet sourceSet = javaExtension.getSourceSets().maybeCreate(sourcesetName);
        for (String parentSourceSetName : parentSourceSets) {
            GradleUtils.extendSourceSet(project, parentSourceSetName, sourcesetName);
        }

        var jarTask = project.getTasks().withType(Jar.class).named(JavaPlugin.JAR_TASK_NAME);
        jarTask.configure(task -> {
            task.into("META-INF/versions/" + javaVersion, copySpec -> copySpec.from(sourceSet.getOutput()));
            task.manifest(manifest -> { manifest.attributes(Map.of("Multi-Release", "true")); });
        });

        project.getTasks().withType(Test.class).named(JavaPlugin.TEST_TASK_NAME).configure(testTask -> {
            testTask.dependsOn(jarTask);

            SourceSetContainer sourceSets = GradleUtils.getJavaSourceSets(project);
            FileCollection mainRuntime = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME).getRuntimeClasspath();
            FileCollection testRuntime = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME).getRuntimeClasspath();
            testTask.setClasspath(testRuntime.minus(mainRuntime).plus(project.files(jarTask)));
        });

        project.getTasks().withType(JavaCompile.class).named(sourceSet.getCompileJavaTaskName()).configure(compileTask -> {
            compileTask.getJavaCompiler()
                .set(javaToolchains.compilerFor(spec -> { spec.getLanguageVersion().set(JavaLanguageVersion.of(javaVersion)); }));
            compileTask.setSourceCompatibility(Integer.toString(javaVersion));
            CompileOptions compileOptions = compileTask.getOptions();
            compileOptions.getRelease().set(javaVersion);
            compileOptions.getCompilerArgs().add("--enable-preview");
            compileOptions.getCompilerArgs().add("-Xlint:-preview");

            compileTask.doLast(t -> { stripPreviewFromFiles(compileTask.getDestinationDirectory().getAsFile().get().toPath()); });
        });
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
