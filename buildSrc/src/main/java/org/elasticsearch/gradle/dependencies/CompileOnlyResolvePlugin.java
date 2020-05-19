/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.dependencies;

import org.gradle.api.NamedDomainObjectProvider;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.JavaPlugin;

public class CompileOnlyResolvePlugin implements Plugin<Project> {
    public static final String RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME = "resolveableCompileOnly";

    @Override
    public void apply(Project project) {
        project.getConfigurations().all(configuration -> {
            if (configuration.getName().equals(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME)) {
                NamedDomainObjectProvider<Configuration> resolvableCompileOnly = project.getConfigurations()
                    .register(RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME);
                resolvableCompileOnly.configure((c) -> {
                    c.setCanBeResolved(true);
                    c.setCanBeConsumed(false);
                    c.extendsFrom(configuration);
                });
            }
        });
    }
}
