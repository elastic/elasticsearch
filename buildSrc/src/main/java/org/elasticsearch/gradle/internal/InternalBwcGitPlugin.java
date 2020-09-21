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
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.process.ExecResult;
import org.gradle.process.ExecSpec;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static java.util.Arrays.asList;

public class InternalBwcGitPlugin implements Plugin<Project> {

    private BwcGitExtension gitExtension;
    private Project project;

    @Override
    public void apply(Project project) {
        gitExtension = new BwcGitExtension(project.getObjects());
        this.project = project;
        project.getExtensions().add("bwcGitConfig", new BwcGitExtension(project.getObjects()));
        ProviderFactory providers = project.getProviders();
        Provider<String> remote = project.getProviders().systemProperty("bwc.remote").forUseAtConfigurationTime().orElse("elastic");

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

        TaskProvider<LoggedExec> fetchLatestTaskProvider = tasks.register("fetchLatest", LoggedExec.class, fetchLatest -> {
            var gitFetchLatest = project.getProviders()
                .systemProperty("tests.bwc.git_fetch_latest")
                .forUseAtConfigurationTime()
                .orElse("true")
                .map(fetchProp -> {
                    if ("true".equals(fetchProp)) {
                        return true;
                    }
                    if ("false".equals(fetchProp)) {
                        return false;
                    }
                    throw new GradleException("tests.bwc.git_fetch_latest must be [true] or [false] but was [" + fetchProp + "]");
                });
            fetchLatest.onlyIf(t -> project.getGradle().getStartParameter().isOffline() == false && gitFetchLatest.get());
            fetchLatest.dependsOn(addRemoteTaskProvider);
            fetchLatest.setWorkingDir(checkoutDir.get());
            fetchLatest.setCommandLine(asList("git", "fetch", "--all"));
        });

        tasks.register("checkoutBwcBranch", checkoutBwcBranch -> {
            checkoutBwcBranch.dependsOn(fetchLatestTaskProvider);
            checkoutBwcBranch.doLast(t -> {
                Logger logger = project.getLogger();

                String bwcBranch = gitExtension.bwcBranch.get();
                final String refspec = providers.systemProperty("bwc.refspec." + bwcBranch)
                    .orElse(providers.systemProperty("tests.bwc.refspec." + bwcBranch))
                    .getOrElse(remote.get() + "/" + bwcBranch);

                String effectiveRefSpec = maybeAlignedRefSpec(project, logger, refspec);

                logger.lifecycle("Performing checkout of {}...", refspec);
                LoggedExec.exec(project, spec -> {
                    spec.workingDir(checkoutDir);
                    spec.commandLine("git", "checkout", effectiveRefSpec);
                });

                String checkoutHash = GlobalBuildInfoPlugin.gitInfo(checkoutDir.get().getAsFile()).getRevision();
                logger.lifecycle("Checkout hash for {} is {}", project.getPath(), checkoutHash);
                writeFile(new File(project.getBuildDir(), "refspec"), checkoutHash);
            });
        });
    }

    public BwcGitExtension getGitExtension() {
        return gitExtension;
    }

    /**
     * We use a time based approach to make the bwc versions built deterministic and compatible with the current hash.
     * Most of the time we want to test against latest, but when running delayed exhaustive tests or wanting
     * reproducible builds we want this to be deterministic by using a hash that was the latest when the current
     * commit was made.
     * <p>
     * This approach doesn't work with merge commits as these can introduce commits in the chronological order
     * after the fact e.x. a merge done today can add commits dated with yesterday so the result will no longer be
     * deterministic.
     * <p>
     * We don't use merge commits, but for additional safety we check that no such commits exist in the time period
     * we are interested in.
     * <p>
     * Timestamps are at seconds resolution. rev-parse --before and --after are inclusive w.r.t the second
     * passed as input. This means the results might not be deterministic in the current second, but this
     * should not matter in practice.
     */
    private String maybeAlignedRefSpec(Project project, Logger logger, String defaultRefSpec) {
        if (project.getProviders().systemProperty("bwc.checkout.align").isPresent() == false) {
            return defaultRefSpec;
        }

        String timeOfCurrent = execGit(execSpec -> {
            execSpec.commandLine(asList("git", "show", "--no-patch", "--no-notes", "--pretty='%cD'"));
            execSpec.workingDir(project.getRootDir());
        });

        logger.lifecycle("Commit date of current: {}", timeOfCurrent);

        String mergeCommits = execGit(
            spec -> spec.commandLine(asList("git", "rev-list", defaultRefSpec, "--after", timeOfCurrent, "--merges"))
        );
        if (mergeCommits.isEmpty() == false) {
            throw new IllegalStateException("Found the following merge commits which prevent determining bwc commits: " + mergeCommits);
        }
        return execGit(
            spec -> spec.commandLine(asList("git", "rev-list", defaultRefSpec, "-n", "1", "--before", timeOfCurrent, "--date-order"))
        );
    }

    private void writeFile(File file, String content) {
        try {
            FileWriter myWriter = new FileWriter(file, false);
            myWriter.write(content);
            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String execGit(Action<ExecSpec> execSpecConfig) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ExecResult exec = project.exec(execSpec -> {
            execSpec.setStandardOutput(os);
            execSpec.workingDir(gitExtension.checkoutDir.getAsFile());
            execSpecConfig.execute(execSpec);
        });
        exec.assertNormalExitValue();
        return os.toString().trim();
    }

    private static boolean isRemoteAvailable(Provider<String> remote, ByteArrayOutputStream output) {
        return new String(output.toByteArray()).lines().anyMatch(l -> l.contains(remote.get() + "\t"));
    }
}
