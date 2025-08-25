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
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
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

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This task generates transport version definition files. These files
 * are runtime resources that TransportVersion loads statically.
 * They contain a comma separated list of integer ids. Each file is named the same
 * as the transport version name itself (with the .csv suffix).
 */
public abstract class GenerateTransportVersionDefinitionTask extends DefaultTask {

    @InputDirectory
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public Path getResourcesDir() {
        return getResources().get().getTransportResourcesDir();
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getReferencesFiles();

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getResources();

    // assumption: this task is always run on main, so we can determine the name by diffing with main and looking for new files added in the
    // definition directory. (not true: once we generate the file, this will no longer hold true if we then need to update it)

    /**
     * Used to set the name of the TransportVersionSet for which a data file will be generated.
     */
    @Input
    @Optional
    @Option(option = "name", description = "TBD") // TODO add description
    public abstract Property<String> getTransportVersionName(); // The plugin should always set this, not optional

    /**
     * The release branch names the generated transport version should target.
     */
    @Optional // In CI we find these from the github PR labels
    @Input
    @Option(option = "branches", description = "The branches for which to generate IDs, e.g. --branches=\"main,9.1\"")
    public abstract Property<String> getBranches();

    @Input
    @Optional
    @Option(option = "increment", description = "The amount to increment the primary id for the main branch")
    public abstract Property<Integer> getPrimaryIncrement();

    @Input
    public abstract Property<String> getMainReleaseBranch();

    @Input
    public abstract Property<String> getResourcesProjectDir();

    private final ExecOperations execOperations;

    @Inject
    public GenerateTransportVersionDefinitionTask(ExecOperations execOperations) {
        this.execOperations = execOperations;
    }

    @TaskAction
    public void run() throws IOException {
        TransportVersionResourcesService resources = getResources().get();
        Set<String> referencedNames = TransportVersionReference.collectNames(getReferencesFiles());
        List<String> changedDefinitionNames = resources.getChangedNamedDefinitionNames();
        String name = getTransportVersionName().isPresent()
            ? getTransportVersionName().get()
            : findAddedTransportVersionName(resources, referencedNames, changedDefinitionNames);

        if (name.isEmpty()) {
            // Todo this should reset all the changed latest files regardless of if there's a name or not
            resetAllLatestFiles(resources);
        } else {
            List<TransportVersionId> ids = updateLatestFiles(resources, name);
            // (Re)write the definition file.
            resources.writeNamedDefinition(new TransportVersionDefinition(name, ids));
        }

        removeUnusedNamedDefinitions(resources, referencedNames, changedDefinitionNames);
    }

    private List<TransportVersionId> updateLatestFiles(TransportVersionResourcesService resources, String name) throws IOException {
        Set<String> targetReleaseBranches = getTargetReleaseBranches();
        String mainReleaseBranch = getMainReleaseBranch().get();
        int primaryIncrement = getPrimaryIncrement().get();
        List<TransportVersionId> ids = new ArrayList<>();

        for (TransportVersionLatest mainLatest : resources.getMainLatests()) {

            if (name.equals(mainLatest.name())) {
                if (targetReleaseBranches.contains(mainLatest.releaseBranch()) == false) {
                    // we don't want to target this latest file but we already changed it
                    // Regenerate to make this operation idempotent. Need to undo prior updates to the latest files if the list of minor
                    // versions has changed.
                    resources.writeLatestFile(mainLatest);
                }
            } else {
                if (targetReleaseBranches.contains(mainLatest.releaseBranch())) {
                    int increment = mainLatest.releaseBranch().equals(mainReleaseBranch) ? primaryIncrement : 1;
                    TransportVersionId id = TransportVersionId.fromInt(mainLatest.id().complete() + increment);
                    ids.add(id);
                    resources.writeLatestFile(new TransportVersionLatest(mainLatest.releaseBranch(), name, id));
                } else {
                    resources.writeLatestFile(mainLatest);
                }
            }
        }

        Collections.sort(ids);
        return ids;
    }

    private Set<String> getTargetReleaseBranches() {
        if (getBranches().get().isEmpty() == false) {
            return Arrays.stream(getBranches().get().split(","))
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

    private void resetAllLatestFiles(TransportVersionResourcesService resources) throws IOException {
        // TODO: this should also _delete_ extraneous files from latest?
        for (TransportVersionLatest latest : resources.getMainLatests()) {
            resources.writeLatestFile(latest);
        }
    }

    private void removeUnusedNamedDefinitions(
        TransportVersionResourcesService resources,
        Set<String> referencedNames,
        List<String> changedDefinitions
    ) throws IOException {
        for (String definitionName : changedDefinitions) {
            if (referencedNames.contains(definitionName) == false) {
                // we added this definition file, but it's now unreferenced, so delete it
                getLogger().lifecycle("Deleting unreferenced named transport version definition [" + definitionName + "]");
                resources.deleteNamedDefinition(definitionName);
            }
        }
    }

    private String findAddedTransportVersionName(
        TransportVersionResourcesService resources,
        Set<String> referencedNames,
        List<String> changedDefinitions
    ) throws IOException {
        // First check for unreferenced names. We only care about the first one. If there is more than one
        // validation will fail later and the developer will have to remove one. When that happens, generation
        // will re-run and we will fixup the state to use whatever new name remains.
        for (String referencedName : referencedNames) {
            if (resources.namedDefinitionExists(referencedName) == false) {
                return referencedName;
            }
        }

        // Since we didn't find any missing names, we use the first changed name. If there is more than
        // one changed name, validation will fail later, just as above.
        if (changedDefinitions.isEmpty()) {
            return "";
        } else {
            return changedDefinitions.getFirst();
        }
    }
}
