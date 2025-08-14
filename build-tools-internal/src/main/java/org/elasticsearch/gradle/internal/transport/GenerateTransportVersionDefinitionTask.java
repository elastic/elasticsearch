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
import java.util.stream.Collectors;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.transport.TransportVersionReference.listFromFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.latestFilePath;
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
     * The release branch names the generated transport version should target.
     */
    @Optional // In CI we find these from the github PR labels
    @Input
    @Option(option = "branches", description = "The branches for which to generate IDs, e.g. --branches=\"main,9.1\"")
    public abstract ListProperty<String> getBranches();

    @Input
    @Optional
    @Option(option = "increment", description = "TBD")
    public abstract Property<Integer> getPrimaryIncrement();

    @Input
    public abstract Property<String> getMainReleaseBranch();

    @Input
    public abstract Property<String> getResourcesProjectDir();

    private final Path rootPath;
    private final ExecOperations execOperations;

    @Inject
    public GenerateTransportVersionDefinitionTask(Project project, ExecOperations execOperations) {
        this.execOperations = execOperations;
        this.rootPath = getProject().getRootProject().getLayout().getProjectDirectory().getAsFile().toPath();
    }

    @TaskAction
    public void run() throws IOException {
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

        removeUnusedNamedDefinitions(referencedNames, changedDefinitionNames);
    }

    private Set<String> getReferencedNames() throws IOException {
        Set<String> referencedNames = new HashSet<>();
        for (var referencesFile : getReferencesFiles()) {
            listFromFile(referencesFile.toPath()).stream().map(TransportVersionReference::name).forEach(referencedNames::add);
        }
        return referencedNames;
    }

    private List<TransportVersionId> updateLatestFiles(Path resourcesDir, String name) throws IOException {
        Set<String> targetReleaseBranches = getTargetReleaseBranches();
        String mainReleaseBranch = getMainReleaseBranch().get();
        int primaryIncrement = getPrimaryIncrement().get();
        List<TransportVersionId> ids = new ArrayList<>();

        for (TransportVersionLatest mainLatest : getMainLatestFiles()) {

            if (name.equals(mainLatest.name())) {
                if (targetReleaseBranches.contains(mainLatest.releaseBranch()) == false) {
                    // we don't want to target this latest file but we already changed it
                    // Regenerate to make this operation idempotent. Need to undo prior updates to the latest files if the list of minor
                    // versions has changed.
                    writeLatestFile(resourcesDir, mainLatest);
                }
            } else {
                if (targetReleaseBranches.contains(mainLatest.releaseBranch())) {
                    int increment = mainLatest.releaseBranch().equals(mainReleaseBranch) ? primaryIncrement : 1;
                    TransportVersionId id = TransportVersionId.fromInt(mainLatest.id().complete() + increment);
                    ids.add(id);
                    writeLatestFile(resourcesDir, new TransportVersionLatest(mainLatest.releaseBranch(), name, id));
                }
            }
        }

        return ids;
    }

    private Set<String> getTargetReleaseBranches() {
        if (getBranches().isPresent()) {
            return getBranches().get()
                .stream()
                .map(branch -> branch.equals("main") ? getMainReleaseBranch().get() : branch)
                .collect(Collectors.toSet());
        } else {
            // look for env var indicating github PR link from CI
            // use github api to find current labels, filter down to version labels
            // map version labels to branches
            String prUrl = System.getenv("BUILDKITE_PULL_REQUEST");
            if (prUrl == null) {
                throw new RuntimeException("When running outside CI, --branches must be specified");
            }

            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ExecResult result = execOperations.exec(spec -> {
                spec.setCommandLine(List.of("gh", "pr", "view", prUrl, "--json", "labels", "--jq", ".labels[].name"));
                spec.setErrorOutput(output);
                spec.setStandardOutput(output);
                spec.setIgnoreExitValue(true);
            });
            if (result.getExitValue() != 0) {
                throw new RuntimeException("Failed to get labels from github API:\n" + output);
            }
            Set<String> targetReleaseBranches = new HashSet<>();
            for (String label : output.toString().split(System.lineSeparator())) {
                if (label.startsWith("v") == false) {
                    continue;
                }
                int firstDot = label.indexOf('.');
                targetReleaseBranches.add(label.substring(1, label.indexOf('.', firstDot + 1)));
            }
            // if we didn't find any version labels we must be on serverless, so just use the main release branch
            if (targetReleaseBranches.isEmpty()) {
                targetReleaseBranches.add(getMainReleaseBranch().get());
            }
            return targetReleaseBranches;
        }
    }

    private void resetAllLatestFiles(Path resourcesDir) throws IOException {
        for (TransportVersionLatest latest : getMainLatestFiles()) {
            writeLatestFile(resourcesDir, latest);
        }
    }

    private List<String> getChangedDefinitionNames() throws IOException {
        List<String> changedDefinitionNames = new ArrayList<>();
        String namedDefinitionsBasePath = TransportVersionUtils.getResourcePath(getResourcesProjectDir().get(), "definitions/named/");
        for (String mainResource : getMainResources(namedDefinitionsBasePath)) {
            String definitionName = mainResource.substring(
                mainResource.lastIndexOf(File.pathSeparator) + 1,
                mainResource.length() - 4 /* .csv */
            );
            changedDefinitionNames.add(definitionName);
        }
        return changedDefinitionNames;
    }

    private void removeUnusedNamedDefinitions(Set<String> referencedNames, List<String> changedDefinitionNames) throws IOException {
        for (String definitionName : changedDefinitionNames) {
            if (referencedNames.contains(definitionName) == false) {
                // we added this definition file, but it's now unreferenced, so delete it
                getLogger().lifecycle("Deleting unreferenced named transport version definition [" + definitionName + "]");
                Path definitionPath = TransportVersionUtils.definitionFilePath(getResourcesDirectory().get(), definitionName);
                Files.delete(definitionPath);
            }
        }
    }

    private List<TransportVersionLatest> getMainLatestFiles() throws IOException {
        List<TransportVersionLatest> latestFiles = new ArrayList<>();
        String latestBasePath = TransportVersionUtils.getResourcePath(getResourcesProjectDir().get(), "latest/");
        for (String latestPath : getMainResources(latestBasePath)) {
            TransportVersionLatest latest = readExistingFile(latestPath, this::latestRelativePath, TransportVersionLatest::fromString);
            latestFiles.add(latest);
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

    // TODO duplicated
    private Set<String> getMainResources(String subPath) {
        String output = gitCommand("ls-tree", "--name-only", "-r", "main", getResourcesProjectDir().get() + "/" + subPath);
        return Set.of(output.split(System.lineSeparator()));
    }

    // TODO duplicated
    private TransportVersionLatest readLatestFromMain(String releaseBranch) {
        return readExistingFile(releaseBranch, this::latestRelativePath, TransportVersionLatest::fromString);
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
