/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin;
import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin;

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.ArtifactCollection;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.artifacts.ResolvableDependencies;
import org.gradle.api.artifacts.component.ProjectComponentIdentifier;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.attributes.Category;
import org.gradle.api.attributes.DocsType;
import org.gradle.api.attributes.VerificationType;
import org.gradle.api.file.FileCollection;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.BasePluginExtension;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.external.javadoc.StandardJavadocDocletOptions;

import java.io.File;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Wires inter-project javadoc linking using variant-aware dependency resolution rather than
 * cross-project task wiring.
 *
 * <p>Each project that applies this plugin (together with the {@code java} plugin) <em>publishes</em>
 * its javadoc output directory as a consumable variant ({@code javadocDirElements}). A consuming
 * project then <em>resolves</em> that variant from its own {@code compileClasspath} project
 * dependencies via an {@link org.gradle.api.artifacts.ArtifactView} with variant reselection. The
 * resolved artifact files are registered as task inputs, so Gradle derives the producing
 * {@code :upstream:javadoc} task dependency from the artifact's {@code builtBy} automatically. This
 * removes the need for {@code Project#afterEvaluate}, {@code Project#evaluationDependsOn} and
 * explicit cross-project {@code dependsOn(":upstream:javadoc")} wiring.
 *
 * <p>Order matters: the {@code linksOffline} entry for {@code org.elasticsearch:elasticsearch} must
 * be the last one, otherwise all links for the other packages (e.g. {@code org.elasticsearch.client})
 * would point to server rather than their own artifacts. We therefore sort the links by their
 * published link path (which begins with the producer's group).
 */
public class ElasticsearchJavadocPlugin implements Plugin<Project> {

    /**
     * Marks the producer variant that exposes the javadoc <em>directory</em> (as opposed to the
     * standard {@code javadocElements} variant which exposes the packaged javadoc jar). Requesting
     * this attribute on the consumer side disambiguates the directory from any jar variant.
     */
    private static final Attribute<Boolean> JAVADOC_DIR_ATTRIBUTE = Attribute.of("elasticsearch.javadoc-dir", Boolean.class);

    /**
     * Carries the producer-computed offline-link path ({@code group/archivesName/version}). Publishing
     * it as a (lazy) attribute lets the consumer build the external link URL without reading the
     * upstream project's model, keeping resolution project-isolation friendly.
     */
    private static final Attribute<String> JAVADOC_LINK_PATH_ATTRIBUTE = Attribute.of("elasticsearch.javadoc-link-path", String.class);

    @Override
    public void apply(Project project) {
        // ignore missing javadocs
        project.getTasks().withType(Javadoc.class).configureEach(javadoc -> {
            // the -quiet here is because of a bug in gradle, in that adding a string option
            // by itself is not added to the options. By adding quiet, both this option and
            // the "value" -quiet is added, separated by a space. This is ok since the javadoc
            // command already adds -quiet, so we are just duplicating it
            // see https://discuss.gradle.org/t/add-custom-javadoc-option-that-does-not-take-an-argument/5959
            javadoc.getOptions().setEncoding("UTF8");
            ((StandardJavadocDocletOptions) javadoc.getOptions()).addStringOption("Xdoclint:all,-missing", "-quiet");

            // ensure that modular dependencies can be found on the module path
            javadoc.doFirst(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    javadoc.getOptions().modulePath(javadoc.getClasspath().getFiles().stream().toList());
                }
            });
        });

        // Relying on configurations introduced by the java plugin
        project.getPlugins().withType(JavaPlugin.class, javaPlugin -> {
            registerJavadocDirVariant(project);
            configureInterProjectJavadocLinks(project);
            configureShadowedSources(project);
        });
    }

    /**
     * Producer side: expose this project's javadoc output directory as a consumable variant so that
     * dependent projects can resolve it through variant-aware dependency resolution.
     */
    private void registerJavadocDirVariant(Project project) {
        ObjectFactory objects = project.getObjects();
        TaskProvider<Javadoc> javadocTask = project.getTasks().named("javadoc", Javadoc.class);

        // Lazily computed so that the build script's `version` and `archivesName` are picked up: the
        // provider is only queried at resolution time, after the build script has run. This is the
        // variant-aware replacement for the afterEvaluate hook the plugin used previously.
        Provider<String> linkPath = project.getExtensions()
            .getByType(BasePluginExtension.class)
            .getArchivesName()
            .map(
                archivesName -> project.getGroup().toString().replace('.', '/') + '/' + archivesName.replace('.', '/') + '/' + project
                    .getVersion()
            );

        project.getConfigurations().consumable("javadocDirElements", c -> {
            c.attributes(a -> {
                a.attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.class, Category.DOCUMENTATION));
                a.attribute(DocsType.DOCS_TYPE_ATTRIBUTE, objects.named(DocsType.class, DocsType.JAVADOC));
                a.attribute(JAVADOC_DIR_ATTRIBUTE, true);
                a.attributeProvider(JAVADOC_LINK_PATH_ATTRIBUTE, linkPath);
            });
            // builtBy makes Gradle run :upstream:javadoc whenever a consumer resolves this artifact.
            c.getOutgoing().artifact(javadocTask.map(Javadoc::getDestinationDir), artifact -> {
                artifact.builtBy(javadocTask);
                artifact.setType("javadoc-directory");
            });
        });
    }

    /**
     * Consumer side: resolve the javadoc directory variant of every (non-shadowed) project dependency
     * on the compile classpath and turn it into {@code linksOffline} entries.
     */
    private void configureInterProjectJavadocLinks(Project project) {
        ObjectFactory objects = project.getObjects();
        Configuration compileClasspath = project.getConfigurations().getByName("compileClasspath");
        ResolvableDependencies incoming = compileClasspath.getIncoming();

        // Shadowed project dependencies have their source inlined (see configureShadowedSources) and
        // must therefore not be linked. Resolved lazily so no afterEvaluate / plugin probe is needed.
        Provider<Set<String>> shadowedProjectPaths = project.provider(() -> {
            Configuration shadow = project.getConfigurations().findByName(ShadowBasePlugin.SHADOW);
            if (shadow == null) {
                return Set.of();
            }
            return shadow.getAllDependencies()
                .stream()
                .filter(d -> d instanceof ProjectDependency)
                .map(d -> ((ProjectDependency) d).getPath())
                .collect(Collectors.toSet());
        });

        ArtifactCollection javadocDirs = incoming.artifactView(v -> {
            // Reselect the javadoc-directory variant in place of the compile (jar) variant.
            v.withVariantReselection();
            // Tolerate dependencies that do not publish a javadoc variant or whose javadoc is disabled.
            v.setLenient(true);
            v.componentFilter(
                id -> id instanceof ProjectComponentIdentifier pci && shadowedProjectPaths.get().contains(pci.getProjectPath()) == false
            );
            v.attributes(a -> {
                a.attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.class, Category.DOCUMENTATION));
                a.attribute(DocsType.DOCS_TYPE_ATTRIBUTE, objects.named(DocsType.class, DocsType.JAVADOC));
                a.attribute(JAVADOC_DIR_ATTRIBUTE, true);
            });
        }).getArtifacts();

        File projectDir = project.getProjectDir();
        String artifactHost = artifactHost(project);

        project.getTasks().named("javadoc", Javadoc.class).configure(javadoc -> {
            // Registering the resolved artifact files as inputs wires the producing :upstream:javadoc
            // tasks via their builtBy metadata. This replaces dependsOn(":upstream:javadoc") and
            // evaluationDependsOn(upstream). We bind to the public Task#getInputs() (rather than
            // Javadoc's covariant override, which returns the internal TaskInputsInternal type).
            ((Task) javadoc).getInputs().files(javadocDirs.getArtifactFiles());

            // linksOffline lives on StandardJavadocDocletOptions, which is read while the javadoc tool
            // runs. doFirst is the safe point at which the resolved ArtifactCollection is available and
            // the options still feed into the generated javadoc.options file.
            javadoc.doFirst("configure inter-project javadoc links", new Action<Task>() {
                @Override
                public void execute(Task task) {
                    var options = (StandardJavadocDocletOptions) javadoc.getOptions();
                    javadocDirs.getArtifacts()
                        .stream()
                        // Sort by the published link path (which begins with the producer's group) so
                        // that org.elasticsearch:elasticsearch is linked last (see class javadoc).
                        .sorted(Comparator.comparing(a -> a.getVariant().getAttributes().getAttribute(JAVADOC_LINK_PATH_ATTRIBUTE)))
                        .forEach(artifact -> {
                            String linkPath = artifact.getVariant().getAttributes().getAttribute(JAVADOC_LINK_PATH_ATTRIBUTE);
                            if (linkPath == null) {
                                return;
                            }
                            String relativeDir = projectDir.toPath().relativize(artifact.getFile().toPath()) + "/";
                            options.linksOffline(artifactHost + "/javadoc/" + linkPath, relativeDir);
                        });
                    /*
                     * Some dependent javadoc tasks are explicitly skipped. We need to ignore those
                     * external links as javadoc would fail otherwise.
                     */
                    options.setLinksOffline(
                        options.getLinksOffline().stream().filter(link -> new File(projectDir, link.getPackagelistLoc()).exists()).toList()
                    );
                }
            });
        });
    }

    /**
     * Include the source of shadowed upstream projects so we don't have to publish their javadoc.
     * Sources are pulled through the standard {@code mainSourceElements} variant and the shadowed
     * classes/dependencies via the {@code shadow} configuration, both resolved lazily.
     */
    private void configureShadowedSources(Project project) {
        ObjectFactory objects = project.getObjects();
        project.getPlugins().withType(ShadowPlugin.class, shadowPlugin -> {
            Configuration shadow = project.getConfigurations().getByName(ShadowBasePlugin.SHADOW);

            FileCollection shadowedSources = shadow.getIncoming().artifactView(v -> {
                v.withVariantReselection();
                v.setLenient(true);
                v.componentFilter(id -> id instanceof ProjectComponentIdentifier);
                v.attributes(a -> {
                    a.attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.class, Category.VERIFICATION));
                    a.attribute(
                        VerificationType.VERIFICATION_TYPE_ATTRIBUTE,
                        objects.named(VerificationType.class, VerificationType.MAIN_SOURCES)
                    );
                });
            }).getFiles();

            project.getTasks().named("javadoc", Javadoc.class).configure(javadoc -> {
                // Inline the shadowed project's sources and put its classes/dependencies on the
                // javadoc classpath so references resolve.
                javadoc.source(shadowedSources);
                javadoc.setClasspath(javadoc.getClasspath().plus(shadow));
            });
        });
    }

    private String artifactHost(Project project) {
        return project.getVersion().toString().endsWith("-SNAPSHOT") ? "https://snapshots.elastic.co" : "https://artifacts.elastic.co";
    }
}
