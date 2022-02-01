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
import org.gradle.api.attributes.LibraryElements;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;

import javax.inject.Inject;

import static org.gradle.api.attributes.LibraryElements.JAR;

/**
 * This plugin tweaks the compile classpath of a project to request module api jars that do not
 * expose internal api of a dependent project and its transitive api.
 * */
public class JavaModulesConsumerPlugin implements Plugin<Project> {

    private final ObjectFactory objectFactory;

    @Inject
    public JavaModulesConsumerPlugin(ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;
    }

    @Override
    public void apply(Project project) {
        // TODO handle all sourcesets compile

        SourceSet main = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().getByName("main");
        Configuration compileConfig = project.getConfigurations().getByName(main.getCompileClasspathConfigurationName());
        Attribute<Boolean> javaModuleAttribute = Attribute.of("org.elasticsearch.java-module", Boolean.class);
        compileConfig.getAttributes().attribute(javaModuleAttribute, Boolean.TRUE);
        compileConfig.getAttributes()
            .attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objectFactory.named(LibraryElements.class, JAR));

    }
}
