/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.transport.TransportVersionReference.listFromFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.LATEST_DIR;
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
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getResourcesDirectory();// The plugin should always set this, not optional

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getReferencesFiles();

    // assumption: this task is always run on main, so we can determine the name by diffing with main and looking for new files added in the
    // definition directory. (not true: once we generate the file, this will no longer hold true if we then need to update it)

    /**
     * Used to set the name of the TransportVersionSet for which a data file will be generated.
     */
    @Input
    @Optional
    @Option(option = "name", description = "TBD")
    public abstract Property<String> getTransportVersionName(); // The plugin should always set this, not optional

    /**
     * Used to set the `major.minor` release version for which the specific TransportVersion ID will be generated.
     * E.g.: "9.2", "8.18", etc.
     */
    @Optional // This is optional as we will look for labels in an env var if this is not set.
    @Input
    @Option(option = "branches", description = "The branches for which to generate IDs, e.g. --releaseBranch=\"main,9.1\"")
    public abstract ListProperty<String> getBranches();

    @Input
    @Optional
    @Option(option = "increment", description = "TBD")
    public abstract Property<Integer> getPrimaryIncrement();

    @Input
    public abstract Property<String> getMainReleaseBranch();

    @Input
    public abstract Property<String> getResourcesProjectDir();
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
    public void run() throws IOException {
        getLogger().lifecycle("Name: " + getTransportVersionName().get());
        getLogger().lifecycle("Versions: " + getBranches().get());
        Path resourcesDir = Objects.requireNonNull(getResourcesDirectory().getAsFile().get()).toPath();
        Set<String> referencedNames = getReferencedNames();
        List<String> changedDefinitionNames = getChangedDefinitionNames();
        String name = getTransportVersionName().isPresent()
            ? getTransportVersionName().get()
            : findAddedTransportVersionName(referencedNames, changedDefinitionNames);

        if (name.isEmpty()) {
            resetAllLatestFiles(resourcesDir);
        } else {
            List<TransportVersionId> ids = updateLatestFiles(resourcesDir, name);
            // (Re)write the definition file.
            writeDefinitionFile(resourcesDir, new TransportVersionDefinition(name, ids));
        }

        removeUnusedDefinitions(referencedNames, changedDefinitionNames);
    }

    private Set<String> getReferencedNames() throws IOException {
        Set<String> referencedNames = new HashSet<>();
        for (var referencesFile : getReferencesFiles()) {
            listFromFile(referencesFile.toPath()).stream().map(TransportVersionReference::name).forEach(referencedNames::add);
        }
        return referencedNames;
    }

    private List<TransportVersionId> updateLatestFiles(Path resourcesDir, String name) throws IOException {
        Set<String> targetReleaseBranches = new HashSet<>(
            getBranches().isPresent() ? mapBranchesToReleaseBranches(getBranches().get()) : findTargetReleaseBranches()
        );
        String mainReleaseBranch = getMainReleaseBranch().get();
        int primaryIncrement = getPrimaryIncrement().get();
        List<TransportVersionId> ids = new ArrayList<>();

        // TODO: this should be reading from main, in case there are merge conflicts that have messed up the latest files
        for (TransportVersionLatest latest : getKnownLatestFiles(resourcesDir)) {

            if (name.equals(latest.name())) {
                if (targetReleaseBranches.contains(latest.releaseBranch()) == false) {
                    // we don't want to target this latest file but we already changed it
                    // Regenerate to make this operation idempotent. Need to undo prior updates to the latest files if the list of minor
                    // versions has changed.
                    writeLatestFile(resourcesDir, readLatestFromMain(latest.releaseBranch()));
                }
            } else {
                if (targetReleaseBranches.contains(latest.releaseBranch())) {
                    int increment = latest.releaseBranch().equals(mainReleaseBranch) ? primaryIncrement : 1;
                    TransportVersionId id = TransportVersionId.fromInt(latest.id().complete() + increment);
                    ids.add(id);
                    writeLatestFile(resourcesDir, new TransportVersionLatest(latest.releaseBranch(), name, id));
                }
            }
        }

        return ids;
    }

    private void resetAllLatestFiles(Path resourcesDir) throws IOException {
        for (TransportVersionLatest latest : getKnownLatestFiles(resourcesDir)) {
            writeLatestFile(resourcesDir, readLatestFromMain(latest.releaseBranch()));
        }
    }

    private List<String> getChangedDefinitionNames() throws IOException {
        List<String> changedDefinitionNames = new ArrayList<>();
        String namedDefinitionsBasePath = TransportVersionUtils.getResourcePath(getResourcesProjectDir().get(), "definitions/named/");
        for (String changedResource : getChangedResources(namedDefinitionsBasePath)) {
            if (changedResource.startsWith(namedDefinitionsBasePath)) {
                String definitionName = changedResource.substring(
                    changedResource.lastIndexOf(File.pathSeparator) + 1,
                    changedResource.length() - 4 /* .csv */
                );
                changedDefinitionNames.add(definitionName);
            }
        }
        return changedDefinitionNames;
    }

    private void removeUnusedDefinitions(Set<String> referencedNames, List<String> changedDefinitionNames) throws IOException {
        for (String definitionName : changedDefinitionNames) {
            if (referencedNames.contains(definitionName) == false) {
                // we added this definition file, but it's now unreferenced, so delete it
                getLogger().lifecycle("Deleting unreferenced named transport version definition [" + definitionName + "]");
                Path definitionPath = TransportVersionUtils.definitionFilePath(getResourcesDirectory().get(), definitionName);
                Files.delete(definitionPath);
            }
        }
    }

    private List<String> mapBranchesToReleaseBranches(List<String> strings) {
        return strings.stream().map(branch -> branch.equals("main") ? getMainReleaseBranch().get() : branch).toList();
    }

    private List<TransportVersionLatest> getKnownLatestFiles(Path resourcesDir) throws IOException {
        List<TransportVersionLatest> latestFiles = new ArrayList<>();
        try (Stream<Path> stream = Files.list(resourcesDir.resolve(LATEST_DIR))) {
            for (Path latestFile : stream.toList()) {
                latestFiles.add(readLatestFile(latestFile));
            }
        }
        return latestFiles;
    }

    private String findAddedTransportVersionName(Set<String> referencedNames, List<String> changedDefinitionNames) throws IOException {

        // First check for unreferenced names. There should only be at most one. If there is more than
        // one reference the remaining reference will still be unreferenced when validation runs later
        // and the developer will remove the unreferenced name.
        for (String referencedName : referencedNames) {
            Path definitionFile = TransportVersionUtils.definitionFilePath(getResourcesDirectory().get(), referencedName);
            if (Files.exists(definitionFile) == false) {
                return referencedName;
            }
        }

        if (changedDefinitionNames.isEmpty()) {
            return "";
        } else {
            return changedDefinitionNames.getFirst();
        }
    }

    private List<String> findTargetReleaseBranches() {
        // look for env var indicating github PR link from CI
        // use github api to find current labels, filter down to version labels
        // map version labels to branches
        return List.of();
    }

    // TODO duplicated
    private Set<String> getChangedResources(String subPath) {
        String output = gitCommand("ls-tree", "--name-only", "-r", "main", getResourcesProjectDir().get() + "/" + subPath);
        return Set.of(output.split(System.lineSeparator()));
    }

    // TODO duplicated
    private TransportVersionLatest readLatestFromMain(String branch) {
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
