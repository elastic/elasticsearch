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

import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;

import java.io.ByteArrayOutputStream;

import static java.util.Arrays.asList;

public class InternalBwcGitPlugin implements Plugin<Project> {

    BwcGitExtension gitExtension;

    @Override
    public void apply(Project project) {
        gitExtension = new BwcGitExtension(project.getObjects());
        project.getExtensions().add("bwcGitConfig", new BwcGitExtension(project.getObjects()));
        Provider<String> remote = project.getProviders().systemProperty("bwc.remote").forUseAtConfigurationTime().orElse("elastic");

        boolean gitFetchLatest;
        final String gitFetchLatestProperty = System.getProperty("tests.bwc.git_fetch_latest", "true");
        if ("true".equals(gitFetchLatestProperty)) {
            gitFetchLatest = true;
        } else if ("false".equals(gitFetchLatestProperty)) {
            gitFetchLatest = false;
        } else {
            throw new GradleException("tests.bwc.git_fetch_latest must be [true] or [false] but was [" + gitFetchLatestProperty + "]");
        }

        RegularFileProperty checkoutDir = gitExtension.checkoutDir;

        TaskContainer tasks = project.getTasks();
        TaskProvider<LoggedExec> createCloneTaskProvider = tasks.register("createClone", LoggedExec.class, createClone -> {
            createClone.onlyIf(task -> gitExtension.checkoutDir.get().getAsFile().exists() == false);
            createClone.setCommandLine(asList("git", "clone", project.getRootDir(), gitExtension.checkoutDir.get().getAsFile()));
        });

        TaskProvider<LoggedExec> findRemoteTaskProvider = tasks.register("findRemote", LoggedExec.class, findRemote -> {
            findRemote.dependsOn(createCloneTaskProvider);
            // TODO Gradle should provide property based configuration here
            findRemote.setWorkingDir(checkoutDir.get());

            findRemote.setCommandLine(asList("git", "remote", "-v"));
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            findRemote.setStandardOutput(output);
            findRemote.doLast(t -> {
                ExtraPropertiesExtension extraProperties = project.getExtensions().getExtraProperties();
                extraProperties.set("remoteExists", isRemoteAvailable(remote, output));
            });
        });

        TaskProvider<LoggedExec> addRemoteTaskProvider = tasks.register("addRemote", LoggedExec.class, addRemote -> {
            addRemote.dependsOn(findRemoteTaskProvider);
            addRemote.onlyIf(task -> ((boolean) project.getExtensions().getExtraProperties().get("remoteExists")) == false);
            addRemote.setWorkingDir(checkoutDir.get());
            String remoteRepo = remote.get();
            addRemote.setCommandLine(asList("git", "remote", "add", remoteRepo, "https://github.com/" + remoteRepo + "/elasticsearch.git"));
        });

        tasks.register("fetchLatest", LoggedExec.class, fetchLatest -> {
            fetchLatest.onlyIf(t -> project.getGradle().getStartParameter().isOffline() == false && gitFetchLatest);
            fetchLatest.dependsOn(addRemoteTaskProvider);
            fetchLatest.setWorkingDir(checkoutDir.get());
            fetchLatest.setCommandLine(asList("git", "fetch", "--all"));
        });
    }

    private static boolean isRemoteAvailable(Provider<String> remote, ByteArrayOutputStream output) {
        return new String(output.toByteArray()).lines().anyMatch(l -> l.contains(remote.get() + "\t"));
    }

    public BwcGitExtension getGitExtension() {
        return gitExtension;
    }
}
