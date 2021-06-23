/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.internal.conventions.info.GitInfo;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.logging.Logger;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;
import org.gradle.process.ExecSpec;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.Arrays.asList;

public class InternalBwcGitPlugin implements Plugin<Project> {

    private final ProviderFactory providerFactory;
    private final ExecOperations execOperations;

    private BwcGitExtension gitExtension;
    private Project project;

    @Inject
    public InternalBwcGitPlugin(ProviderFactory providerFactory, ExecOperations execOperations) {
        this.providerFactory = providerFactory;
        this.execOperations = execOperations;
    }

    @Override
    public void apply(Project project) {
        this.project = project;
        this.gitExtension = project.getExtensions().create("bwcGitConfig", BwcGitExtension.class);
        Provider<String> remote = providerFactory.systemProperty("bwc.remote").forUseAtConfigurationTime().orElse("elastic");

        TaskContainer tasks = project.getTasks();
        TaskProvider<LoggedExec> createCloneTaskProvider = tasks.register("createClone", LoggedExec.class, createClone -> {
            createClone.onlyIf(task -> this.gitExtension.getCheckoutDir().get().exists() == false);
            createClone.setCommandLine(asList("git", "clone", project.getRootDir(), gitExtension.getCheckoutDir().get()));
        });

        TaskProvider<LoggedExec> findRemoteTaskProvider = tasks.register("findRemote", LoggedExec.class, findRemote -> {
            findRemote.dependsOn(createCloneTaskProvider);
            // TODO Gradle should provide property based configuration here
            findRemote.setWorkingDir(gitExtension.getCheckoutDir().get());

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
            addRemote.setWorkingDir(gitExtension.getCheckoutDir().get());
            String remoteRepo = remote.get();
            // for testing only we can override the base remote url
            String remoteRepoUrl = providerFactory.systemProperty("testRemoteRepo")
                .forUseAtConfigurationTime()
                .getOrElse("https://github.com/" + remoteRepo + "/elasticsearch.git");
            addRemote.setCommandLine(asList("git", "remote", "add", remoteRepo, remoteRepoUrl));
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
            fetchLatest.setWorkingDir(gitExtension.getCheckoutDir().get());
            fetchLatest.setCommandLine(asList("git", "fetch", "--all"));
        });

        TaskProvider<Task> checkoutBwcBranchTaskProvider = tasks.register("checkoutBwcBranch", checkoutBwcBranch -> {
            checkoutBwcBranch.dependsOn(fetchLatestTaskProvider);
            checkoutBwcBranch.doLast(t -> {
                File checkoutDir = gitExtension.getCheckoutDir().get();
                Logger logger = project.getLogger();
                String bwcBranch = this.gitExtension.getBwcBranch().get();
                final String refspec = providerFactory.systemProperty("bwc.refspec." + bwcBranch)
                    .orElse(providerFactory.systemProperty("tests.bwc.refspec." + bwcBranch))
                    .getOrElse(remote.get() + "/" + bwcBranch);

                String effectiveRefSpec = maybeAlignedRefSpec(logger, refspec);

                logger.lifecycle("Performing checkout of {}...", refspec);
                LoggedExec.exec(execOperations, spec -> {
                    spec.workingDir(checkoutDir);
                    spec.commandLine("git", "checkout", effectiveRefSpec);
                });

                String checkoutHash = GitInfo.gitInfo(checkoutDir).getRevision();
                logger.lifecycle("Checkout hash for {} is {}", project.getPath(), checkoutHash);
                writeFile(new File(project.getBuildDir(), "refspec"), checkoutHash);
            });
        });

        String checkoutConfiguration = "checkout";
        project.getConfigurations().create(checkoutConfiguration);

        project.getArtifacts().add(checkoutConfiguration, gitExtension.getCheckoutDir(), configurablePublishArtifact -> {
            configurablePublishArtifact.builtBy(checkoutBwcBranchTaskProvider);
            configurablePublishArtifact.setType("directory");
            configurablePublishArtifact.setName("checkoutDir");
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
    private String maybeAlignedRefSpec(Logger logger, String defaultRefSpec) {
        if (providerFactory.systemProperty("bwc.checkout.align").isPresent() == false) {
            return defaultRefSpec;
        }

        String timeOfCurrent = execInCheckoutDir(execSpec -> {
            execSpec.commandLine(asList("git", "show", "--no-patch", "--no-notes", "--pretty='%cD'"));
            execSpec.workingDir(project.getRootDir());
        });

        logger.lifecycle("Commit date of current: {}", timeOfCurrent);

        String mergeCommits = execInCheckoutDir(
            spec -> spec.commandLine(asList("git", "rev-list", defaultRefSpec, "--after", timeOfCurrent, "--merges"))
        );
        if (mergeCommits.isEmpty() == false) {
            throw new IllegalStateException("Found the following merge commits which prevent determining bwc commits: " + mergeCommits);
        }
        return execInCheckoutDir(
            spec -> spec.commandLine(asList("git", "rev-list", defaultRefSpec, "-n", "1", "--before", timeOfCurrent, "--date-order"))
        );
    }

    private void writeFile(File file, String content) {
        try {
            Files.writeString(file.toPath(), content, CREATE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String execInCheckoutDir(Action<ExecSpec> execSpecConfig) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ExecResult exec = execOperations.exec(execSpec -> {
            execSpec.setStandardOutput(os);
            execSpec.workingDir(gitExtension.getCheckoutDir().get());
            execSpecConfig.execute(execSpec);
        });
        exec.assertNormalExitValue();
        return os.toString().trim();
    }

    private static boolean isRemoteAvailable(Provider<String> remote, ByteArrayOutputStream output) {
        return new String(output.toByteArray()).lines().anyMatch(l -> l.contains(remote.get() + "\t"));
    }
}
