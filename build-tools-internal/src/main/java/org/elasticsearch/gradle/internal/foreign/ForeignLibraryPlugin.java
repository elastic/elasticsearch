/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.foreign;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;

import java.io.File;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

/**
 * Wires up FFM annotation processing and post-compile {@code module-info.class} augmentation for
 * modules that contain {@code @LibrarySpecification} interfaces from {@code libs/foreign-library}.
 * The augment step injects {@code provides org.elasticsearch.foreign.LibraryProvider with ...}
 * directives that reference generated classes the source {@code module-info.java} cannot name.
 *
 * <p>All non-main source sets are processed too, so bindings that only make sense in tests
 * can live next to the tests that use them instead of adding production surface. Those source sets need
 * no {@code module-info.class} augmentation: tests run on the classpath, where {@code ServiceLoader}
 * discovers the generated providers through the {@code META-INF/services} file the annotation processor
 * emits.
 */
public class ForeignLibraryPlugin implements Plugin<Project> {

    private static final JavaLanguageVersion PROCESSOR_TOOLCHAIN = JavaLanguageVersion.of(25);
    private static final String FOREIGN_LIBRARY_PROJECT = ":libs:foreign-library";
    private static final String PROCESSOR_PROJECT = ":libs:foreign-library:processor";

    static final String PROCESSOR_CONFIGURATION_NAME = "foreignLibraryProcessor";
    static final String PROCESS_ANNOTATIONS_TASK_NAME = "processForeignAnnotations";
    static final String AUGMENT_MODULE_INFO_TASK_NAME = "augmentForeignModuleInfo";

    private static final String GENERATED_CLASSES_DIR = "generated-foreign-library-classes";
    private static final String AUGMENTED_MODULE_INFO_DIR = "augmented-foreign-module-info";
    private static final String SERVICES_FILE = "META-INF/services/org.elasticsearch.foreign.LibraryProvider";
    private static final String MODULE_INFO_CLASS = "module-info.class";

    private final JavaToolchainService javaToolchains;

    @Inject
    public ForeignLibraryPlugin(JavaToolchainService javaToolchains) {
        this.javaToolchains = javaToolchains;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(JavaLibraryPlugin.class);

        Configuration processorConfiguration = addDependencies(project);

        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet mainSourceSet = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME);

        TaskProvider<JavaCompile> processAnnotations = registerProcessAnnotationsTask(project, mainSourceSet, processorConfiguration);
        TaskProvider<AugmentForeignModuleInfoTask> augmentModuleInfo = registerAugmentModuleInfoTask(
            project,
            processAnnotations,
            processorConfiguration
        );

        mainSourceSet.getOutput().dir(processAnnotations.flatMap(JavaCompile::getDestinationDirectory));

        swapModuleInfoInJar(project, augmentModuleInfo);

        // All non-main source sets get the same annotation processing, but no module-info augmentation
        // and no jar swap: they are non-modular and run on the classpath.
        sourceSets.matching(sourceSet -> SourceSet.MAIN_SOURCE_SET_NAME.equals(sourceSet.getName()) == false).all(sourceSet -> {
            TaskProvider<JavaCompile> task = registerProcessAnnotationsTask(project, sourceSet, processorConfiguration);
            sourceSet.getOutput().dir(task.flatMap(JavaCompile::getDestinationDirectory));
        });
    }

    private Configuration addDependencies(Project project) {
        // Project lookups are guarded so the plugin tolerates serverless composite builds, where
        // the foreign-library projects don't exist locally and consumers supply the deps via GAV.
        if (project.findProject(FOREIGN_LIBRARY_PROJECT) != null) {
            project.getDependencies()
                .add(JavaPlugin.API_CONFIGURATION_NAME, project.getDependencies().project(Map.of("path", FOREIGN_LIBRARY_PROJECT)));
        }
        Configuration processorConfiguration = project.getConfigurations().create(PROCESSOR_CONFIGURATION_NAME);
        if (project.findProject(PROCESSOR_PROJECT) != null) {
            project.getDependencies()
                .add(PROCESSOR_CONFIGURATION_NAME, project.getDependencies().project(Map.of("path", PROCESSOR_PROJECT)));
        }
        return processorConfiguration;
    }

    private TaskProvider<JavaCompile> registerProcessAnnotationsTask(
        Project project,
        SourceSet sourceSet,
        Configuration processorConfiguration
    ) {
        JavaPluginExtension javaExtension = project.getExtensions().getByType(JavaPluginExtension.class);
        Provider<String> releaseVersion = project.provider(() -> javaExtension.getTargetCompatibility().getMajorVersion());

        String taskName = sourceSet.getTaskName("process", "ForeignAnnotations");
        // Sibling directories rather than one nested under main's: nesting would pull the generated test
        // classes into main's output.
        String destinationDir = SourceSet.MAIN_SOURCE_SET_NAME.equals(sourceSet.getName())
            ? GENERATED_CLASSES_DIR
            : GENERATED_CLASSES_DIR + "-" + sourceSet.getName();

        TaskProvider<JavaCompile> task = project.getTasks().register(taskName, JavaCompile.class, t -> {
            t.setSource(sourceSet.getJava());
            t.setClasspath(sourceSet.getCompileClasspath());
            t.getOptions().setAnnotationProcessorPath(processorConfiguration);
            t.getDestinationDirectory().set(project.getLayout().getBuildDirectory().dir(destinationDir));
            t.getOptions().getCompilerArgs().add("-proc:only");
            t.getOptions().getCompilerArgumentProviders().add(() -> List.of("-AjavaVersion=" + releaseVersion.get()));
        });

        // ElasticsearchJavaBasePlugin's withType(JavaCompile) configureEach pins the toolchain to the
        // project's minimum runtime (JDK 21) and sets options.release to it; the processor needs the
        // JDK 24+ classfile API and pinning --release would reject sources referencing now-finalized
        // preview APIs. We also re-enable inferModulePath, which ElasticsearchJavaModulePathPlugin
        // disables globally. TaskProvider.configure runs after configureEach, so these overrides win.
        task.configure(t -> {
            t.getJavaCompiler().set(javaToolchains.compilerFor(spec -> spec.getLanguageVersion().set(PROCESSOR_TOOLCHAIN)));
            t.getOptions().getRelease().unset();
            t.setSourceCompatibility(PROCESSOR_TOOLCHAIN.toString());
            t.setTargetCompatibility(PROCESSOR_TOOLCHAIN.toString());
            t.getModularity().getInferModulePath().set(true);
        });

        return task;
    }

    private TaskProvider<AugmentForeignModuleInfoTask> registerAugmentModuleInfoTask(
        Project project,
        TaskProvider<JavaCompile> processAnnotations,
        Configuration augmenterClasspath
    ) {
        TaskProvider<JavaCompile> compileJava = project.getTasks().named(JavaPlugin.COMPILE_JAVA_TASK_NAME, JavaCompile.class);

        return project.getTasks().register(AUGMENT_MODULE_INFO_TASK_NAME, AugmentForeignModuleInfoTask.class, task -> {
            task.getInputModuleInfo().set(compileJava.flatMap(t -> t.getDestinationDirectory().file(MODULE_INFO_CLASS)));
            task.getServicesFile().set(processAnnotations.flatMap(t -> t.getDestinationDirectory().file(SERVICES_FILE)));
            task.getAugmenterClasspath().from(augmenterClasspath);
            task.getJavaLauncher().set(javaToolchains.launcherFor(spec -> spec.getLanguageVersion().set(PROCESSOR_TOOLCHAIN)));
            task.getOutputModuleInfo()
                .set(project.getLayout().getBuildDirectory().file(AUGMENTED_MODULE_INFO_DIR + "/" + MODULE_INFO_CLASS));
        });
    }

    private void swapModuleInfoInJar(Project project, TaskProvider<AugmentForeignModuleInfoTask> augmentModuleInfo) {
        Provider<File> compiledModuleInfo = project.getTasks()
            .named(JavaPlugin.COMPILE_JAVA_TASK_NAME, JavaCompile.class)
            .flatMap(t -> t.getDestinationDirectory().file(MODULE_INFO_CLASS))
            .map(f -> f.getAsFile());
        Provider<File> augmentedModuleInfo = augmentModuleInfo.flatMap(AugmentForeignModuleInfoTask::getOutputModuleInfo)
            .map(f -> f.getAsFile());

        project.getTasks().named(JavaPlugin.JAR_TASK_NAME, Jar.class).configure(task -> {
            task.exclude(element -> element.getFile().equals(compiledModuleInfo.get()));
            task.from(augmentedModuleInfo);
        });
    }
}
