/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.transport.TransportVersionId.Component.PATCH;
import static org.elasticsearch.gradle.internal.transport.TransportVersionId.Component.SERVER;
import static org.elasticsearch.gradle.internal.transport.TransportVersionId.Component.SUBSIDIARY;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.LATEST_DIR;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.SERVERLESS_BRANCH;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.latestFilePath;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.readLatestFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.writeDefinitionFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.writeLatestFile;

/**
 * This task generates transport version definition files. These files
 * are runtime resources that TransportVersion loads statically.
 * They contain a comma separated list of integer ids. Each file is named the same
 * as the transport version name itself (with the .csv suffix).
 */
public abstract class GenerateTransportVersionDefinitionTask extends DefaultTask {

    /**
     * Specifies the directory in which contains all TransportVersionSet data files.
     *
     * @return
     */
    @InputDirectory
    // @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getResourcesDirectory();// The plugin should always set this, not optional

    // assumption: this task is always run on main, so we can determine the name by diffing with main and looking for new files added in the
    // definition directory. (not true: once we generate the file, this will no longer hold true if we then need to update it)

    /**
     * Used to set the name of the TransportVersionSet for which a data file will be generated.
     */
    @Input
    // @Optional
    @Option(option = "name", description = "TBD")
    public abstract Property<String> getTransportVersionName(); // The plugin should always set this, not optional

    /**
     * Used to set the `major.minor` release version for which the specific TransportVersion ID will be generated.
     * E.g.: "9.2", "8.18", etc.
     */
    @Optional // This is optional as we will look for labels in an env var if this is not set.
    @Input
    @Option(option = "branches", description = "The branches for which to generate IDs, e.g. --branch=\"9.2,9.1,SERVERLESS\"")
    public abstract ListProperty<String> getBranches();

    // @Optional
    // @Input
    // public abstract Property<Function<String, Component>> getIdIncrementSupplier();

    private final Path rootPath;
    private final ExecOperations execOperations;

    @Inject
    public GenerateTransportVersionDefinitionTask(Project project, ExecOperations execOperations) {
        this.execOperations = execOperations;
        this.rootPath = getProject().getRootProject().getLayout().getProjectDirectory().getAsFile().toPath();
    }

    @TaskAction
    public void generateTransportVersionData() throws IOException {
        getLogger().lifecycle("Name: " + getTransportVersionName().get());
        getLogger().lifecycle("Versions: " + getBranches().get());
        Path resourcesDir = Objects.requireNonNull(getResourcesDirectory().getAsFile().get()).toPath();
        String name = getTransportVersionName().isPresent() ? getTransportVersionName().get() : findLocalTransportVersionName();
        Set<String> targetMinorVersions = new HashSet<>(
            getBranches().isPresent() ? new HashSet<>(getBranches().get()) : findTargetMinorVersions()
        );

        List<TransportVersionId> ids = new ArrayList<>();
        for (String branchName : getKnownBranchNames(resourcesDir)) {
            TransportVersionLatest latest = readLatestFile(resourcesDir, branchName);

            if (name.equals(latest.name())) {
                if (targetMinorVersions.contains(branchName) == false) {
                    // Regenerate to make this operation idempotent. Need to undo prior updates to the latest files if the list of minor
                    // versions has changed.

                    // TODO should we just always regen?
                    writeLatestFile(resourcesDir, readExistingLatest(branchName));
                }
            } else {
                if (targetMinorVersions.contains(branchName)) {
                    // increment
                    var incrementedId = incrementTVId(latest.id(), branchName);
                    ids.add(incrementedId);
                    writeLatestFile(resourcesDir, new TransportVersionLatest(branchName, name, incrementedId));
                }
            }
        }
        // (Re)write the definition file.
        writeDefinitionFile(resourcesDir, new TransportVersionDefinition(name, ids));
    }

    private TransportVersionId incrementTVId(TransportVersionId id, String branchName) {
        // We can only run this task on main, so the ElasticsearchVersion will be for main.
        Version mainVersion = VersionProperties.getElasticsearchVersion();
        String latestFileNameForMain = mainVersion.getMajor() + "." + mainVersion.getMinor();

        final var isMain = branchName.equals(latestFileNameForMain);
        if (isMain) {
            return id.bumpComponent(SERVER);
        } else if (branchName.equals(SERVERLESS_BRANCH)) {
            return id.bumpComponent(SUBSIDIARY);
        } else {
            return id.bumpComponent(PATCH);
        }
    }

    private Set<String> getKnownBranchNames(Path resourcesDir) {
        // list files under latest
        // TODO add serverless? Otherwise just delete this function if it that doesn't make sense here.
        return recordExistingResources(resourcesDir.resolve(LATEST_DIR));
    }

    private String findLocalTransportVersionName() {
        // check for missing
        // if none missing, look at git diff against main
        return "";
    }

    private List<String> findTargetMinorVersions() {
        // look for env var indicating github PR link from CI
        // use github api to find current labels, filter down to version labels
        // map version labels to branches
        return List.of();
    }

    // TODO duplicated
    private Set<String> recordExistingResources(Path resourcesPath) {
        String output = gitCommand("ls-tree", "--name-only", "-r", "main", resourcesPath.toString());
        return Set.of(output.split(System.lineSeparator()));
    }

    // TODO duplicated
    private TransportVersionLatest readExistingLatest(String branch) {
        return readExistingFile(branch, this::latestRelativePath, TransportVersionLatest::fromString);
    }

    // TODO duplicated
    private <T> T readExistingFile(String name, Function<String, String> pathFunction, BiFunction<String, String, T> parser) {
        String relativePath = pathFunction.apply(name);
        String content = gitCommand("show", "main:" + relativePath).strip();
        return parser.apply(relativePath, content);
    }

    // TODO duplicated
    private String latestRelativePath(String branch) {
        return relativePath(latestFilePath(getResourcesDirectory().get(), branch));
    }

    // TODO duplicated
    private Path resourcesDirPath() {
        return getResourcesDirectory().get().getAsFile().toPath();
    }

    // TODO duplicated
    private String relativePath(Path file) {
        return rootPath.relativize(file).toString();
    }

    // TODO duplicated
    public String gitCommand(String... args) {
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream();

        List<String> command = new ArrayList<>();
        Collections.addAll(command, "git", "-C", rootPath.toAbsolutePath().toString());
        Collections.addAll(command, args);

        ExecResult result = execOperations.exec(spec -> {
            spec.setCommandLine(command);
            spec.setStandardOutput(stdout);
            spec.setErrorOutput(stdout);
            spec.setIgnoreExitValue(true);
        });

        if (result.getExitValue() != 0) {
            throw new RuntimeException(
                "git command failed with exit code "
                    + result.getExitValue()
                    + System.lineSeparator()
                    + "command: "
                    + String.join(" ", command)
                    + System.lineSeparator()
                    + "output:"
                    + System.lineSeparator()
                    + stdout.toString(StandardCharsets.UTF_8)
            );
        }

        return stdout.toString(StandardCharsets.UTF_8);
    }
}
