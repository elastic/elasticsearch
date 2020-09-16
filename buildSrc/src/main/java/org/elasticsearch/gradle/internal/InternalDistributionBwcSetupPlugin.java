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

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.BwcVersions;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;

import java.io.File;

public class InternalDistributionBwcSetupPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        Provider<String> remote = project.getProviders().systemProperty("bwc.remote").forUseAtConfigurationTime().orElse("elastic");

        BuildParams.getBwcVersions()
            .forPreviousUnreleased(
                (BwcVersions.UnreleasedVersionInfo unreleasedVersion) -> {
                    configureProject(project.project(unreleasedVersion.gradleProjectPath), unreleasedVersion, remote);
                }
            );
    }

    private void configureProject(Project bwcProject, BwcVersions.UnreleasedVersionInfo unreleasedVersion, Provider<String> remote) {

        BwcGitExtension gitExtension = bwcProject.getPlugins().apply(InternalBwcGitPlugin.class).getGitExtension();

        String bwcBranch = unreleasedVersion.branch;
        File checkoutDir = new File(bwcProject.getBuildDir(), "bwc/checkout-" + bwcBranch);

        // using extra properties temporally while porting build script code over to this plugin
        ExtraPropertiesExtension extraProps = bwcProject.getExtensions().getExtraProperties();
        extraProps.set("bwcVersion", unreleasedVersion.version);
        extraProps.set("bwcBranch", bwcBranch);
        extraProps.set("checkoutDir", checkoutDir);
        extraProps.set("remote", remote.get());

        gitExtension.bwcVersion.set(unreleasedVersion.version);
        gitExtension.bwcBranch.set(bwcBranch);
        gitExtension.checkoutDir.set(checkoutDir);

        bwcProject.getPlugins().apply("distribution");
        // Not published so no need to assemble
        bwcProject.getTasks().named("assemble").configure(t -> t.setEnabled(false));
    }
}
