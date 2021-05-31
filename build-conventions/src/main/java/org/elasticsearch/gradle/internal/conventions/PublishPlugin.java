/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions;

import org.elasticsearch.gradle.internal.conventions.precommit.PomValidationPrecommitPlugin;
import com.github.jengelman.gradle.plugins.shadow.ShadowExtension;
import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin;
import groovy.util.Node;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.conventions.info.GitInfo;
import org.gradle.api.NamedDomainObjectSet;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.XmlProvider;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.BasePluginConvention;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import org.gradle.api.publish.maven.tasks.GenerateMavenPom;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;

public class PublishPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(BasePlugin.class);
        project.getPluginManager().apply(MavenPublishPlugin.class);
        project.getPluginManager().apply(PomValidationPrecommitPlugin.class);
        project.getPluginManager().apply(LicensingPlugin.class);

        configureJavadocJar(project);
        configureSourcesJar(project);
        configurePomGeneration(project);
        configurePublications(project);
    }

    private void configurePublications(Project project) {
        var publishingExtension = project.getExtensions().getByType(PublishingExtension.class);
        var publication = publishingExtension.getPublications().create("elastic", MavenPublication.class);

        project.afterEvaluate(project1 -> {
            if (project1.getPlugins().hasPlugin(ShadowPlugin.class)) {
                configureWithShadowPlugin(project1, publication);
            } else if (project1.getPlugins().hasPlugin(JavaPlugin.class)) {
                publication.from(project.getComponents().getByName("java"));
            }
        });
        publication.getPom().withXml(xml -> {
            var node = xml.asNode();
            node.appendNode("inceptionYear", "2009");
            var licensesNode = node.appendNode("licenses");
            var projectLicenses = (MapProperty<String, String>) project.getExtensions().getExtraProperties().get("projectLicenses");
            projectLicenses.get().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                Node license = licensesNode.appendNode("license");
                license.appendNode("name", entry.getKey());
                license.appendNode("url", entry.getValue());
                license.appendNode("distribution", "repo");
            });
            var developer = node.appendNode("developers").appendNode("developer");
            developer.appendNode("name", "Elastic");
            developer.appendNode("url", "https://www.elastic.co");
        });
        publishingExtension.getRepositories().maven(mavenArtifactRepository -> {
            mavenArtifactRepository.setName("test");
            mavenArtifactRepository.setUrl(new File(project.getRootProject().getBuildDir(), "local-test-repo"));
        });
    }

    private static String getArchivesBaseName(Project project) {
        return project.getConvention().getPlugin(BasePluginConvention.class).getArchivesBaseName();
    }

    /**
     * Configuration generation of maven poms.
     */
    private static void configurePomGeneration(Project project) {
        Property<GitInfo> gitInfo = project.getRootProject().getPlugins().apply(GitInfoPlugin.class).getGitInfo();

        var generatePomTask = project.getTasks().register("generatePom");
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
        var publishing = project.getExtensions().getByType(PublishingExtension.class);
        final var mavenPublications = publishing.getPublications().withType(MavenPublication.class);
        addNameAndDescriptiontoPom(project, mavenPublications);

        mavenPublications.all(publication -> {
            // Add git origin info to generated POM files for internal builds
            publication.getPom().withXml((xmlProvider) -> addScmInfo(xmlProvider, gitInfo.get()));
            // have to defer this until archivesBaseName is set
            project.afterEvaluate(p -> publication.setArtifactId(getArchivesBaseName(project)));
            generatePomTask.configure(t -> t.dependsOn(project.getTasks().withType(GenerateMavenPom.class)));
        });
    }

    private static void addNameAndDescriptiontoPom(Project project, NamedDomainObjectSet<MavenPublication> mavenPublications) {
        mavenPublications.all(p -> p.getPom().withXml(xml -> {
            var root = xml.asNode();
            root.appendNode("name", project.getName());
            var description = project.getDescription() != null ? project.getDescription() : "";
            root.appendNode("description", description);
        }));
    }

    private static void configureWithShadowPlugin(Project project, MavenPublication publication) {
        ShadowExtension shadow = project.getExtensions().getByType(ShadowExtension.class);
        shadow.component(publication);
    }

    private static void addScmInfo(XmlProvider xml, GitInfo gitInfo) {
        var root = xml.asNode();
        root.appendNode("url", gitInfo.urlFromOrigin());
        var scmNode = root.appendNode("scm");
        scmNode.appendNode("url", gitInfo.getOrigin());
    }

    /**
     * Adds a javadocJar task to generate a jar containing javadocs.
     */
    private static void configureJavadocJar(Project project) {
        project.getPlugins().withType(JavaLibraryPlugin.class, p -> {
            var javadocJarTask = project.getTasks().register("javadocJar", Jar.class);
            javadocJarTask.configure(jar -> {
                jar.getArchiveClassifier().set("javadoc");
                jar.setGroup("build");
                jar.setDescription("Assembles a jar containing javadocs.");
                jar.from(project.getTasks().named(JavaPlugin.JAVADOC_TASK_NAME));
            });
            project.getTasks().named(BasePlugin.ASSEMBLE_TASK_NAME).configure(t -> t.dependsOn(javadocJarTask));
        });
    }

    static void configureSourcesJar(Project project) {
        project.getPlugins().withType(JavaLibraryPlugin.class, p -> {
            TaskProvider<Jar> sourcesJarTask = project.getTasks().register("sourcesJar", Jar.class);
            sourcesJarTask.configure(jar -> {
                jar.getArchiveClassifier().set("sources");
                jar.setGroup("build");
                jar.setDescription("Assembles a jar containing source files.");
                SourceSet mainSourceSet = Util.getJavaMainSourceSet(project).get();
                jar.from(mainSourceSet.getAllSource());
            });
            project.getTasks().named(BasePlugin.ASSEMBLE_TASK_NAME).configure(t -> t.dependsOn(sourcesJarTask));
        });
    }
}
