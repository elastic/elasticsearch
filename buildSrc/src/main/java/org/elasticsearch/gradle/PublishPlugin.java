/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin;
import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin;
import groovy.util.Node;
import groovy.util.NodeList;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.precommit.PomValidationPrecommitPlugin;
import org.elasticsearch.gradle.util.Util;
import org.gradle.api.NamedDomainObjectSet;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.XmlProvider;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.BasePluginConvention;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import org.gradle.api.publish.maven.tasks.GenerateMavenPom;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.language.base.plugins.LifecycleBasePlugin;
import java.util.concurrent.Callable;

import static org.elasticsearch.gradle.util.GradleUtils.maybeConfigure;

public class PublishPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(BasePlugin.class);
        project.getPluginManager().apply(MavenPublishPlugin.class);
        project.getPluginManager().apply(PomValidationPrecommitPlugin.class);
        configurePublications(project);
        configureJavadocJar(project);
        configureSourcesJar(project);
        configurePomGeneration(project);
    }

    private void configurePublications(Project project) {
        PublishingExtension publishingExtension = project.getExtensions().getByType(PublishingExtension.class);
        MavenPublication publication = publishingExtension.getPublications().create("elastic", MavenPublication.class);
        project.getPlugins().withType(JavaPlugin.class, plugin -> publication.from(project.getComponents().getByName("java")));
        project.getPlugins().withType(ShadowPlugin.class, plugin -> configureWithShadowPlugin(project, publication));
    }

    private static String getArchivesBaseName(Project project) {
        return project.getConvention().getPlugin(BasePluginConvention.class).getArchivesBaseName();
    }

    /**
     * Configuration generation of maven poms.
     */
    private static void configurePomGeneration(Project project) {
        TaskProvider<Task> generatePomTask = project.getTasks().register("generatePom");
        project.getTasks().named(LifecycleBasePlugin.ASSEMBLE_TASK_NAME).configure(assemble -> assemble.dependsOn(generatePomTask));
        project.getTasks()
            .withType(GenerateMavenPom.class)
            .configureEach(
                pomTask -> pomTask.setDestination(
                    (Callable<String>) () -> String.format(
                        "%s/distributions/%s-%s.pom",
                        project.getBuildDir(),
                        getArchivesBaseName(project),
                        project.getVersion()
                    )
                )
            );
        PublishingExtension publishing = project.getExtensions().getByType(PublishingExtension.class);
        final var mavenPublications = publishing.getPublications().withType(MavenPublication.class);
        addNameAndDescriptiontoPom(project, mavenPublications);

        mavenPublications.all(publication -> {
            // Add git origin info to generated POM files for internal builds
            BuildParams.withInternalBuild(() -> publication.getPom().withXml(PublishPlugin::addScmInfo));
            // have to defer this until archivesBaseName is set
            project.afterEvaluate(p -> publication.setArtifactId(getArchivesBaseName(project)));
            generatePomTask.configure(t -> t.dependsOn(project.getTasks().withType(GenerateMavenPom.class)));
        });
    }

    private static void addNameAndDescriptiontoPom(Project project, NamedDomainObjectSet<MavenPublication> mavenPublications) {
        mavenPublications.all(p -> p.getPom().withXml(xml -> {
            Node root = xml.asNode();
            root.appendNode("name", project.getName());
            String description = project.getDescription() != null ? project.getDescription() : "";
            root.appendNode("description", project.getDescription());
        }));
    }

    private static void configureWithShadowPlugin(Project project, MavenPublication publication) {
        // Workaround for https://github.com/johnrengelman/shadow/issues/334
        // Here we manually add any project dependencies in the "shadow" configuration to our generated POM
        publication.getPom().withXml(xml -> {
            Node root = xml.asNode();
            NodeList dependencies = (NodeList) root.get("dependencies");
            Node dependenciesNode = (dependencies.size() == 0)
                ? root.appendNode("dependencies")
                : (Node) ((NodeList) root.get("dependencies")).get(0);
            project.getConfigurations().getByName(ShadowBasePlugin.getCONFIGURATION_NAME()).getAllDependencies().all(dependency -> {
                if (dependency instanceof ProjectDependency) {
                    Node dependencyNode = dependenciesNode.appendNode("dependency");
                    dependencyNode.appendNode("groupId", dependency.getGroup());
                    ProjectDependency projectDependency = (ProjectDependency) dependency;
                    String artifactId = getArchivesBaseName(projectDependency.getDependencyProject());
                    dependencyNode.appendNode("artifactId", artifactId);
                    dependencyNode.appendNode("version", dependency.getVersion());
                    dependencyNode.appendNode("scope", "compile");
                }
            });
        });
    }

    private static void addScmInfo(XmlProvider xml) {
        Node root = xml.asNode();
        root.appendNode("url", Util.urlFromOrigin(BuildParams.getGitOrigin()));
        Node scmNode = root.appendNode("scm");
        scmNode.appendNode("url", BuildParams.getGitOrigin());
    }

    /**
     * Adds a javadocJar task to generate a jar containing javadocs.
     */
    private static void configureJavadocJar(Project project) {
        project.getPlugins().withId("elasticsearch.java", p -> {
            TaskProvider<Jar> javadocJarTask = project.getTasks().register("javadocJar", Jar.class);
            javadocJarTask.configure(jar -> {
                jar.getArchiveClassifier().set("javadoc");
                jar.setGroup("build");
                jar.setDescription("Assembles a jar containing javadocs.");
                jar.from(project.getTasks().named(JavaPlugin.JAVADOC_TASK_NAME));
            });
            maybeConfigure(project.getTasks(), BasePlugin.ASSEMBLE_TASK_NAME, t -> t.dependsOn(javadocJarTask));
        });
    }

    static void configureSourcesJar(Project project) {
        project.getPlugins().withId("elasticsearch.java", p -> {
            TaskProvider<Jar> sourcesJarTask = project.getTasks().register("sourcesJar", Jar.class);
            sourcesJarTask.configure(jar -> {
                jar.getArchiveClassifier().set("sources");
                jar.setGroup("build");
                jar.setDescription("Assembles a jar containing source files.");
                SourceSet mainSourceSet = Util.getJavaMainSourceSet(project).get();
                jar.from(mainSourceSet.getAllSource());
            });
            maybeConfigure(project.getTasks(), BasePlugin.ASSEMBLE_TASK_NAME, t -> t.dependsOn(sourcesJarTask));
        });
    }
}
