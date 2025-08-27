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

import javax.inject.Inject;

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

    @Input
    @Optional
    @Option(option = "name", description = "The name of the Transport Version reference")
    public abstract Property<String> getTransportVersionName();

    @Optional
    @Input
    @Option(option = "branches", description = "The release branches for which to generate IDs, e.g. --branches=main,9.1")
    public abstract Property<String> getBranches();

    @Input
    @Optional
    @Option(option = "increment", description = "The amount to increment the primary id for the main releaseBranch")
    public abstract Property<Integer> getPrimaryIncrement();

    /**
     * The version number currently associated with the `main` branch, (e.g. as returned by
     * VersionProperties.getElasticsearchVersion()).
     */
    @Input
    public abstract Property<String> getMainReleaseBranch();

    private final ExecOperations execOperations;

    @Inject
    public GenerateTransportVersionDefinitionTask(ExecOperations execOperations) {
        this.execOperations = execOperations;
    }

    @TaskAction
    public void run() throws IOException {
        TransportVersionResourcesService resources = getResources().get();
        Set<String> referencedNames = TransportVersionReference.collectNames(getReferencesFiles());
        List<String> changedDefinitionNames = resources.getChangedReferableDefinitionNames();
        String name = getTransportVersionName().isPresent()
            ? getTransportVersionName().get()
            : findAddedTransportVersionName(resources, referencedNames, changedDefinitionNames);

        if (name.isEmpty()) {
            resetAllLatestFiles(resources);
        } else {
            List<TransportVersionId> ids = updateLatestFiles(resources, name);
            // (Re)write the definition file.
            resources.writeReferableDefinition(new TransportVersionDefinition(name, ids));
        }

        removeUnusedNamedDefinitions(resources, referencedNames, changedDefinitionNames);
    }

    private List<TransportVersionId> updateLatestFiles(TransportVersionResourcesService resources, String name) throws IOException {
        Set<String> targetReleaseBranches = getTargetReleaseBranches();
        String mainReleaseBranch = getMainReleaseBranch().get();
        int primaryIncrement = getPrimaryIncrement().get();
        if (primaryIncrement <= 0) {
            throw new IllegalArgumentException("Invalid increment: must be a positive integer > 0");
        }
        List<TransportVersionId> ids = new ArrayList<>();

        TransportVersionDefinition existingDefinition = resources.getReferableDefinitionFromMain(name);

        for (TransportVersionUpperBound upperBound : resources.getUpperBoundsFromMain()) {
            if (name.equals(upperBound.name())) {
                if (targetReleaseBranches.contains(upperBound.releaseBranch()) == false) {
                    // Here, we aren't targeting this latest file, but need to undo prior updates if the list of minor
                    // versions has changed. We must regenerate this latest file to make this operation idempotent.
                    resources.writeUpperBound(upperBound);
                } else {
                    ids.add(upperBound.id());
                }
            } else {
                if (targetReleaseBranches.contains(upperBound.releaseBranch())) {
                    TransportVersionId targetId = null;
                    if (existingDefinition != null) {
                        for (TransportVersionId id : existingDefinition.ids()) {
                            if (id.base() == upperBound.id().base()) {
                                targetId = id;
                                break;
                            }
                        }
                    }
                    if (targetId == null) {
                        int increment = upperBound.releaseBranch().equals(mainReleaseBranch) ? primaryIncrement : 1;
                        TransportVersionId id = TransportVersionId.fromInt(upperBound.id().complete() + increment);
                        ids.add(id);
                        resources.writeUpperBound(new TransportVersionUpperBound(upperBound.releaseBranch(), name, id));
                    }
                } else {
                    resources.writeUpperBound(upperBound);
                }
            }
        }

        Collections.sort(ids);
        return ids;
    }

    private Set<String> getTargetReleaseBranches() {
        if (getBranches().isPresent()) {
            return Arrays.stream(getBranches().get().split(","))
                .map(branch -> branch.equals("main") ? getMainReleaseBranch().get() : branch)
                .collect(Collectors.toSet());
        } else {
            // Look for env var indicating github PR link from CI.
            // Use github api to find current labels, filter down to version labels.
            // Map version labels to branches.
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
            // if we didn't find any version labels we must be on serverless, so just use the main release releaseBranch
            if (targetReleaseBranches.isEmpty()) {
                targetReleaseBranches.add(getMainReleaseBranch().get());
            }
            return targetReleaseBranches;
        }
    }

    private void resetAllLatestFiles(TransportVersionResourcesService resources) throws IOException {
        // TODO: this should also _delete_ extraneous files from latest?
        for (TransportVersionUpperBound upperBound : resources.getUpperBoundsFromMain()) {
            resources.writeUpperBound(upperBound);
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
                resources.deleteReferableDefinition(definitionName);
            }
        }
    }

    private String findAddedTransportVersionName(
        TransportVersionResourcesService resources,
        Set<String> referencedNames,
        List<String> changedDefinitions
    ) {
        // First check for unreferenced names. We only care about the first one. If there is more than one
        // validation will fail later and the developer will have to remove one. When that happens, generation
        // will re-run and we will fixup the state to use whatever new name remains.
        for (String referencedName : referencedNames) {
            if (resources.referableDefinitionExists(referencedName) == false) {
                return referencedName;
            }
        }

        // Since we didn't find any missing names, we use the first changed name. If there is more than
        // one changed name, validation will fail later, just as above.
        if (changedDefinitions.isEmpty()) {
            return "";
        } else {
            String changedDefinitionName = changedDefinitions.getFirst();
            if (referencedNames.contains(changedDefinitionName)) {
                return changedDefinitionName;
            } else {
                return ""; // the changed name is unreferenced, so go into "reset mode"
            }
        }
    }
}
