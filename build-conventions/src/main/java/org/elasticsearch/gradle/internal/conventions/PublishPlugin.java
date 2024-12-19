/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions;

import groovy.util.Node;

import com.github.jengelman.gradle.plugins.shadow.ShadowExtension;
import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin;

import org.elasticsearch.gradle.internal.conventions.info.GitInfo;
import org.elasticsearch.gradle.internal.conventions.precommit.PomValidationPrecommitPlugin;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.gradle.api.NamedDomainObjectSet;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.XmlProvider;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.BasePluginExtension;
import org.gradle.api.plugins.ExtensionContainer;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import org.gradle.api.publish.maven.tasks.GenerateMavenPom;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.initialization.layout.BuildLayout;
import org.gradle.language.base.plugins.LifecycleBasePlugin;
import org.w3c.dom.Element;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.inject.Inject;

public class PublishPlugin implements Plugin<Project> {

    private ProjectLayout projectLayout;
    private BuildLayout buildLayout;
    private ProviderFactory providerFactory;

    @Inject
    public PublishPlugin(ProjectLayout projectLayout, BuildLayout buildLayout, ProviderFactory providerFactory) {
        this.projectLayout = projectLayout;
        this.buildLayout = buildLayout;
        this.providerFactory = providerFactory;
    }

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
        formatGeneratedPom(project);
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
        @SuppressWarnings("unchecked")
        var projectLicenses = (MapProperty<String, String>) project.getExtensions().getExtraProperties().get("projectLicenses");
        publication.getPom().withXml(xml -> {
            var node = xml.asNode();
            node.appendNode("inceptionYear", "2009");
            var licensesNode = node.appendNode("licenses");
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
            mavenArtifactRepository.setUrl(new File(buildLayout.getRootDirectory(), "build/local-test-repo"));
        });
    }

    private static String getArchivesBaseName(ExtensionContainer extensions) {
        return extensions.getByType(BasePluginExtension.class).getArchivesName().get();
    }

    /**
     * Configuration generation of maven poms.
     */
    private void configurePomGeneration(Project project) {
        var gitInfo = project.getRootProject().getPlugins().apply(GitInfoPlugin.class).getGitInfo();
        var generatePomTask = project.getTasks().register("generatePom");
        project.getTasks().named(LifecycleBasePlugin.ASSEMBLE_TASK_NAME).configure(assemble -> assemble.dependsOn(generatePomTask));
        var extensions = project.getExtensions();
        var archivesBaseName = providerFactory.provider(() -> getArchivesBaseName(extensions));
        var projectVersion = providerFactory.provider(() -> project.getVersion());
        var generateMavenPoms = project.getTasks().withType(GenerateMavenPom.class);
        generateMavenPoms.configureEach(pomTask -> {
            pomTask.setDestination(
                (Callable<String>) () -> String.format(
                    "%s/distributions/%s-%s.pom",
                    projectLayout.getBuildDirectory().get().getAsFile().getPath(),
                    archivesBaseName.get(),
                    projectVersion.get()
                )
            );
        });

        var publishing = extensions.getByType(PublishingExtension.class);
        final var mavenPublications = publishing.getPublications().withType(MavenPublication.class);
        addNameAndDescriptionToPom(project, mavenPublications);
        mavenPublications.configureEach(publication -> {
            publication.getPom().withXml(xml -> {
                // Add git origin info to generated POM files for internal builds
                addScmInfo(xml, gitInfo.get());
            });
            // have to defer this until archivesBaseName is set
            project.afterEvaluate(p -> publication.setArtifactId(archivesBaseName.get()));
            generatePomTask.configure(t -> t.dependsOn(generateMavenPoms));
        });
    }

    private void addNameAndDescriptionToPom(Project project, NamedDomainObjectSet<MavenPublication> mavenPublications) {
        var name = project.getName();
        var description = providerFactory.provider(() -> project.getDescription() != null ? project.getDescription() : "");
        mavenPublications.configureEach(p -> p.getPom().withXml(xml -> {
            var root = xml.asNode();
            root.appendNode("name", name);
            root.appendNode("description", description.get());
        }));
    }

    private static void configureWithShadowPlugin(Project project, MavenPublication publication) {
        var shadow = project.getExtensions().getByType(ShadowExtension.class);
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
            var sourcesJarTask = project.getTasks().register("sourcesJar", Jar.class);
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


    /**
     * Format the generated pom files to be in a sort of reproducible order.
     */
    private void formatGeneratedPom(Project project) {
        var publishing = project.getExtensions().getByType(PublishingExtension.class);
        final var mavenPublications = publishing.getPublications().withType(MavenPublication.class);
        mavenPublications.configureEach(publication -> {
            publication.getPom().withXml(xml -> {
                // Add some pom formatting
                formatDependencies(xml);
            });
        });
    }

    /**
     * just ensure we put dependencies to the end. more a cosmetic thing than anything else
     * */
    private void formatDependencies(XmlProvider xml) {
        Element rootElement = xml.asElement();
        var dependencies = rootElement.getElementsByTagName("dependencies");
        if (dependencies.getLength() == 1 && dependencies.item(0) != null) {
            org.w3c.dom.Node item = dependencies.item(0);
            rootElement.removeChild(item);
            rootElement.appendChild(item);
        }
    }
}
