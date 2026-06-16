/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.foreign;

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class ForeignLibraryPluginTests {

    private Project rootProject;
    private Project consumer;
    private Project foreignLibraryProject;
    private Project processorProject;

    @Before
    public void setUp() {
        rootProject = ProjectBuilder.builder().withName("root").build();
        Project libsProject = ProjectBuilder.builder().withParent(rootProject).withName("libs").build();
        foreignLibraryProject = ProjectBuilder.builder().withParent(libsProject).withName("foreign-library").build();
        processorProject = ProjectBuilder.builder().withParent(foreignLibraryProject).withName("processor").build();
        consumer = ProjectBuilder.builder().withParent(rootProject).withName("consumer").build();

        // Apply java-library to the stub libs so they expose the api/runtime configurations
        // that the consumer project resolves through.
        foreignLibraryProject.getPluginManager().apply(JavaLibraryPlugin.class);
        processorProject.getPluginManager().apply(JavaLibraryPlugin.class);

        consumer.getPluginManager().apply(ForeignLibraryPlugin.class);
    }

    @Test
    public void appliesJavaLibraryPlugin() {
        assertTrue(consumer.getPluginManager().hasPlugin("java-library"));
    }

    @Test
    public void declaresForeignLibraryAsApiDependency() {
        Configuration api = consumer.getConfigurations().getByName(JavaPlugin.API_CONFIGURATION_NAME);
        Dependency dependency = api.getDependencies().iterator().next();
        assertThat(dependency, instanceOf(ProjectDependency.class));
        assertThat(((ProjectDependency) dependency).getPath(), equalTo(":libs:foreign-library"));
    }

    @Test
    public void createsProcessorConfigurationWithProcessorProjectDependency() {
        Configuration processorConfiguration = consumer.getConfigurations()
            .getByName(ForeignLibraryPlugin.PROCESSOR_CONFIGURATION_NAME);
        Dependency dependency = processorConfiguration.getDependencies().iterator().next();
        assertThat(dependency, instanceOf(ProjectDependency.class));
        assertThat(((ProjectDependency) dependency).getPath(), equalTo(":libs:foreign-library:processor"));
    }

    @Test
    public void registersProcessAnnotationsTaskWithExpectedWiring() {
        ForeignAnnotationProcessorTask task = (ForeignAnnotationProcessorTask) consumer.getTasks()
            .getByName(ForeignLibraryPlugin.PROCESS_ANNOTATIONS_TASK_NAME);

        assertThat(task.getReleaseVersion().get(), notNullValue());
        assertThat(
            task.getOutputDirectory().get().getAsFile(),
            equalTo(new File(consumer.getLayout().getBuildDirectory().get().getAsFile(), "generated-foreign-library-classes"))
        );
    }

    @Test
    public void registersAugmentModuleInfoTaskWithExpectedWiring() {
        AugmentForeignModuleInfoTask task = (AugmentForeignModuleInfoTask) consumer.getTasks()
            .getByName(ForeignLibraryPlugin.AUGMENT_MODULE_INFO_TASK_NAME);

        File buildDir = consumer.getLayout().getBuildDirectory().get().getAsFile();
        assertThat(
            task.getOutputModuleInfo().get().getAsFile(),
            equalTo(new File(buildDir, "augmented-foreign-module-info/module-info.class"))
        );
        assertThat(
            task.getServicesFile().get().getAsFile(),
            equalTo(new File(buildDir, "generated-foreign-library-classes/META-INF/services/org.elasticsearch.foreign.LibraryProvider"))
        );
        // input module-info points at the compileJava output
        JavaCompile compileJava = (JavaCompile) consumer.getTasks().getByName(JavaPlugin.COMPILE_JAVA_TASK_NAME);
        assertThat(
            task.getInputModuleInfo().get().getAsFile(),
            equalTo(new File(compileJava.getDestinationDirectory().get().getAsFile(), "module-info.class"))
        );
    }

    @Test
    public void compileJavaIsConfiguredWithProcNoneAndDependsOnProcessAnnotations() {
        JavaCompile compileJava = (JavaCompile) consumer.getTasks().getByName(JavaPlugin.COMPILE_JAVA_TASK_NAME);
        assertThat(compileJava.getOptions().getCompilerArgs(), hasItem("-proc:none"));

        boolean dependsOnProcessAnnotations = compileJava.getTaskDependencies()
            .getDependencies(compileJava)
            .stream()
            .map(Task::getName)
            .anyMatch(name -> name.equals(ForeignLibraryPlugin.PROCESS_ANNOTATIONS_TASK_NAME));
        assertTrue("compileJava should depend on processForeignAnnotations", dependsOnProcessAnnotations);
    }

    @Test
    public void mainSourceSetOutputIncludesGeneratedClassesDir() {
        SourceSetContainer sourceSets = consumer.getExtensions().getByType(SourceSetContainer.class);
        SourceSet main = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME);

        File expected = new File(consumer.getLayout().getBuildDirectory().get().getAsFile(), "generated-foreign-library-classes");
        boolean present = main.getOutput().getDirs().getFiles().contains(expected);
        assertTrue("main source set output should include the generated classes dir; got " + main.getOutput().getDirs().getFiles(), present);
    }

    @Test
    public void jarTaskDependsOnAugmentModuleInfo() {
        Jar jar = (Jar) consumer.getTasks().getByName(JavaPlugin.JAR_TASK_NAME);
        boolean dependsOnAugment = jar.getTaskDependencies()
            .getDependencies(jar)
            .stream()
            .map(Task::getName)
            .anyMatch(name -> name.equals(ForeignLibraryPlugin.AUGMENT_MODULE_INFO_TASK_NAME));
        assertTrue("jar should depend on augmentForeignModuleInfo", dependsOnAugment);
    }
}
