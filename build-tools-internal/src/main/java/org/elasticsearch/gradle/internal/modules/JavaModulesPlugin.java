/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.modules;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.attributes.Bundling;
import org.gradle.api.attributes.Category;
import org.gradle.api.attributes.LibraryElements;
import org.gradle.api.attributes.Usage;
import org.gradle.api.attributes.java.TargetJvmVersion;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetOutput;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;

import java.util.Set;

import javax.inject.Inject;

import static org.gradle.api.attributes.Category.LIBRARY;
import static org.gradle.api.attributes.LibraryElements.JAR;
import static org.gradle.api.attributes.LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE;

/**
 * This plugin provides an outgoing variant that only exposes public api classes that are calculated
 * from the module-info.java file.
 *
 * This variant is meant to replace apiElements variant for consuming. In addition to apiElements
 * this moduleApiElements variant exposes the attribute 'org.elasticsearch.java-module-api` of type
 * boolean value 'true'.
 *
 * See {@link JavaModulesConsumerPlugin} as an example how to request this variant.
 *
 * TODO: provide a plain classes variant in addition to jar variant
 * */
public class JavaModulesPlugin implements Plugin<Project> {

    private ObjectFactory objectFactory;

    @Inject
    public JavaModulesPlugin(ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().withPlugin("java-base", p -> {
            // TODO handle all sourceSets
            SourceSet main = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().getByName("main");
            Configuration moduleApiElements = createModuleApisVariant(project, main);

            SourceSetOutput mainOutput = main.getOutput();
            TaskProvider<ReadModuleExports> exportModuleInfo = project.getTasks().register("exportModuleInfo", ReadModuleExports.class);
            exportModuleInfo.configure(e -> { e.setClassFiles(mainOutput.getClassesDirs()); });

            TaskProvider<Jar> moduleApiJar = project.getTasks().register("modulesApiJar", Jar.class, t -> {
                t.dependsOn(exportModuleInfo);
                t.getArchiveAppendix().set("api");
                t.from(mainOutput);
                t.include(e -> {
                    Set<String> exports = exportModuleInfo.get().getExports();
                    String path = e.getRelativePath().getPathString();
                    if (path.endsWith(".class")) {
                        int lastSlash = path.lastIndexOf('/');
                        if (lastSlash == -1) {
                            lastSlash = 0;
                        }
                        String packagePath = path.substring(0, lastSlash);
                        if (exports.contains(packagePath) == false) {
                            return false;
                        }
                    }
                    return true;
                });
            });
            moduleApiElements.getOutgoing().artifact(moduleApiJar);
        });
    }

    /**
     * creates a consumable configuration with similar attributes as gradles apiElements.
     *
     * In addition we add an attribute 'org.elasticsearch.java-module-api` of type boolean
     * */
    private Configuration createModuleApisVariant(Project project, SourceSet sourceSet) {
        Attribute<Boolean> javaModuleAttribute = Attribute.of("org.elasticsearch.java-module-api", Boolean.class);

        return project.getConfigurations().create("moduleApiElements", moduleApiElements -> {
            moduleApiElements.extendsFrom(project.getConfigurations().getByName(sourceSet.getApiConfigurationName()));
            moduleApiElements.setCanBeConsumed(true);
            moduleApiElements.setCanBeResolved(false);
            moduleApiElements.getAttributes()
                .attribute(Category.CATEGORY_ATTRIBUTE, objectFactory.named(Category.class, LIBRARY))
                .attribute(LIBRARY_ELEMENTS_ATTRIBUTE, objectFactory.named(LibraryElements.class, JAR))
                .attribute(Bundling.BUNDLING_ATTRIBUTE, objectFactory.named(Bundling.class, Bundling.EXTERNAL))
                    // TODO rework hard coded jvm version
                .attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 17)
                .attribute(Usage.USAGE_ATTRIBUTE, objectFactory.named(Usage.class, Usage.JAVA_API))
                .attribute(javaModuleAttribute, Boolean.TRUE);
        });

    }
}
