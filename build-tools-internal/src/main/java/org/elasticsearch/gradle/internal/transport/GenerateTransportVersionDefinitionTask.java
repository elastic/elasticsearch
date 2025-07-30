/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.transport.TransportVersionUtils.MajorMinor;
import org.elasticsearch.gradle.internal.transport.TransportVersionUtils.TransportVersionDefinition;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.IdIncrement.PATCH;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.IdIncrement.SERVER;

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
    public abstract DirectoryProperty getTransportResourcesDirectory(); // The plugin should always set this, not optional

    // assumption: this task is always run on main, so we can determine the name by diffing with main and looking for new files added in the
    // definition directory
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
    @Optional
    @Input
    @Option(option = "versions", description = "The minor version(s) for which to generate IDs, e.g. --versions=\"9.2,9.1\"")
    public abstract ListProperty<String> getMinorVersions();

    // @Optional
    // @Input
    // public abstract Property<Function<String, IdIncrement>> getIdIncrementSupplier();

    @TaskAction
    public void generateTransportVersionData() throws IOException {
        getLogger().lifecycle("Name: " + getTransportVersionName().get());
        getLogger().lifecycle("Versions: " + getMinorVersions().get());
        Path resourcesDir = Objects.requireNonNull(getTransportResourcesDirectory().getAsFile().get()).toPath();
        String name = getTransportVersionName().isPresent() ? getTransportVersionName().get() : findLocalTransportVersionName();
        Set<String> targetMinorVersions = new HashSet<>(
            getMinorVersions().isPresent() ? getMinorVersions().get() : findTargetMinorVersions()
        );

        List<Integer> ids = new ArrayList<>();
        for (String minorVersion : getKnownMinorVersions(resourcesDir)) {
            TransportVersionDefinition latest = TransportVersionUtils.getLatestFile(resourcesDir, minorVersion);
            TransportVersionDefinition newLatest = null;

            if (name.equals(latest.name())) {
                if (targetMinorVersions.contains(minorVersion) == false) {
                    // regenerate
                }
            } else {
                if (targetMinorVersions.contains(minorVersion)) {
                    // increment
                }
            }

            if (newLatest != null) {
                assert newLatest.ids().size() == 1;
                TransportVersionUtils.updateLatestFile(resourcesDir, minorVersion, newLatest.name(), newLatest.ids().getFirst());
            }
        }

        /*
        final String tvName = Objects.requireNonNull(getTransportVersionName().get());
        List<String> minorVersions = getMinorVersions().get();
        //        final var idIncrementSupplier = Objects.requireNonNull(getIdIncrementSupplier().get());

        // TODO
        //  - [x] do we need to also validate that the minorVersions don't contain duplicates here? How do we enforce idempotency if we don't?
        //  - is there an order we need to apply? ( I don't think so)
        //  - Do we need to run this iteratively for backport construction, rather than accepting a list like this? (I don't think so)
        //  - [x] parse args if run alone
        //  - check that duplicate versions don't come in?
        //  - Check that we don't have duplicate names (elsewhere, not here)
        //  - Do we need to allow creating only patch versions?
        //  - Must also keep data in sync for removal.
        //      - We could remove any TVs not associated with a version arg. We then either generate or keep any tvs
        //        for each version arg, and discard the rest
        //      - How will this work for follow-up backport PRs that will not have all the version labels?
        //          - The follow up PR somehow needs to know original IDs. Look at git?  Need a new task?
        //  -

        // Load the tvSetData for the specified name, if it exists
        final var tvDefinition = getDefinedFile(tvDataDir, tvName);
        boolean tvDefinitionExists = tvDefinition != null;
        final List<Integer> preexistingIds = tvDefinitionExists ? Collections.unmodifiableList(tvDefinition.ids()) : List.of();

        List<Integer> ids = new ArrayList<>();
        for (var forVersion : forMinorVersions.stream().map(MajorMinor::of).toList()) {
            // Get the latest transport version data for the specified minor version.
            final int latestTV = getLatestId(tvDataDir, forVersion.toString());

            // Create the new version id
            // final int newID = idIncrementSupplier.apply(forVersion).bumpTransportVersion(latestTV);
            final int newID = incrementTVId(latestTV, forVersion);

            // Check that if we already have a TV ID for this minor version
            Integer preexistingTVId = retrieveValueInRange(
                getPriorLatestId(tvDataDir, forVersion.toString()),
                newID, preexistingIds
            );
            if (preexistingTVId != null) {
                ids.add(preexistingTVId);
                // TODO: Should we log something here?
            } else {
                ids.add(newID);
                // Update the LATEST file.
                // TODO need to revert the latest files for anything that has been removed.
                updateLatestFile(tvDataDir, forVersion.toString(), tvName, newID);
            }
        }

        writeDefinitionFile(tvDataDir, tvName, ids.stream().sorted(Comparator.reverseOrder()).toList());
        */
    }

    private int incrementTVId(int tvID, MajorMinor version) {
        // We can only run this task on main, so the ElasticsearchVersion will be for main.
        final var mainVersion = MajorMinor.of(VersionProperties.getElasticsearchVersion());
        final var isMain = version.equals(mainVersion);
        if (isMain) {
            return SERVER.bumpTransportVersion(tvID);
        } else {
            return PATCH.bumpTransportVersion(tvID);
        }
        // TODO add serverless check
    }

    private boolean containsValueInRange(int lowerExclusive, int upperInclusive, List<Integer> ids) {
        for (var id : ids) {
            if (lowerExclusive < id && id <= upperInclusive) {
                return true;
            }
        }
        return false;
    }

    private Integer retrieveValueInRange(int lowerExclusive, int upperInclusive, List<Integer> ids) {
        for (var id : ids) {
            if (lowerExclusive < id && id <= upperInclusive) {
                return id;
            }
        }
        return null;
    }

    private List<String> getKnownMinorVersions(Path resourcesDir) {
        // list files under latest
        return List.of();
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
}
