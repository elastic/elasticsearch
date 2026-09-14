/*
 * @notice
 * Copyright 2011-2019 the original author or authors.
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is derived from the nebula gradle-ospackage-plugin
 * (https://github.com/nebula-plugins/gradle-ospackage-plugin), reduced to the
 * functionality used by the Elasticsearch build and rewritten in Java.
 */

package org.elasticsearch.gradle.internal.ospackage;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.BasePlugin;

/**
 * Provides the {@code ospackage} extension holding project-wide packaging defaults and applies
 * those defaults to every {@link org.elasticsearch.gradle.internal.ospackage.rpm.Rpm} and
 * {@link org.elasticsearch.gradle.internal.ospackage.deb.Deb} task of the project.
 * <p>
 * This plugin and its supporting classes are a trimmed-down, configuration-cache compatible Java
 * extraction of the parts of the nebula gradle-ospackage-plugin that the Elasticsearch
 * distribution build actually uses. The docker, application and daemon packaging features of the
 * original plugin were dropped, as were the Groovy metaclass based DSL aliases.
 */
public abstract class OsPackageBasePlugin implements Plugin<Project> {

    public static final String EXTENSION_NAME = "ospackage";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(BasePlugin.class);
        ProjectPackagingExtension extension = project.getExtensions()
            .create(EXTENSION_NAME, ProjectPackagingExtension.class, project.copySpec());

        project.getTasks()
            .withType(SystemPackagingTask.class)
            .configureEach(task -> task.initDefaults(extension, project.getVersion().toString()));
    }
}
