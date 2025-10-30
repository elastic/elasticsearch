/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.internal.info.BranchesFileParser;
import org.elasticsearch.gradle.internal.info.DevelopmentBranch;
import org.gradle.api.DefaultTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;

import javax.inject.Inject;

/**
 * Updates the branches.json file in the root of the repository
 */
public class UpdateBranchesJsonTask extends DefaultTask {

    private static final Logger LOGGER = Logging.getLogger(UpdateBranchesJsonTask.class);

    private final ObjectMapper objectMapper;
    private final BranchesFileParser branchesFileParser;

    @OutputFile
    private File branchesFile;

    @Input
    @Optional
    private DevelopmentBranch addBranch;
    @Input
    @Optional
    private String removeBranch;
    @Input
    @Optional
    private DevelopmentBranch updateBranch;

    @Inject
    public UpdateBranchesJsonTask(ProjectLayout projectLayout) {
        this.objectMapper = new ObjectMapper();
        this.branchesFileParser = new BranchesFileParser(objectMapper);
        this.branchesFile = projectLayout.getSettingsDirectory().file("branches.json").getAsFile();
    }

    public File getBranchesFile() {
        return branchesFile;
    }

    public void setBranchesFile(File branchesFile) {
        this.branchesFile = branchesFile;
    }

    public DevelopmentBranch getAddBranch() {
        return addBranch;
    }

    public String getRemoveBranch() {
        return removeBranch;
    }

    public DevelopmentBranch getUpdateBranch() {
        return updateBranch;
    }

    @Option(option = "add-branch", description = "Specifies the branch and corresponding version to add in format <branch>:<version>")
    public void addBranch(String branchAndVersion) {
        this.addBranch = toDevelopmentBranch(branchAndVersion);
    }

    @Option(option = "remove-branch", description = "Specifies the branch to remove")
    public void removeBranch(String branch) {
        this.removeBranch = branch;
    }

    @Option(option = "update-branch", description = "Specifies the branch and corresponding version to update in format <branch>:<version>")
    public void updateBranch(String branchAndVersion) {
        this.updateBranch = toDevelopmentBranch(branchAndVersion);
    }

    private DevelopmentBranch toDevelopmentBranch(String branchAndVersion) {
        String[] parts = branchAndVersion.split(":");
        if (parts.length != 2) {
            throw new InvalidUserDataException("Expected branch and version in format <branch>:<version>");
        }
        return new DevelopmentBranch(parts[0], Version.fromString(parts[1]));
    }

    @TaskAction
    public void executeTask() throws IOException {
        List<DevelopmentBranch> developmentBranches = readBranches(branchesFile);

        if (addBranch == null && removeBranch == null && updateBranch == null) {
            throw new InvalidUserDataException("At least one of add-branch, remove-branch or update-branch must be specified");
        }

        if (addBranch != null) {
            LOGGER.info("Adding branch {} with version {}", addBranch.name(), addBranch.version());
            if (developmentBranches.stream().anyMatch(developmentBranch -> developmentBranch.name().equals(addBranch.name()))) {
                throw new InvalidUserDataException("Branch " + addBranch.name() + " already exists");
            }
            developmentBranches.add(addBranch);
        }
        if (removeBranch != null) {
            LOGGER.info("Removing branch {}", removeBranch);
            if (developmentBranches.stream().noneMatch(developmentBranch -> developmentBranch.name().equals(removeBranch))) {
                throw new InvalidUserDataException("Branch " + removeBranch + " does not exist");
            }
            developmentBranches.removeIf(developmentBranch -> developmentBranch.name().equals(removeBranch));
        }
        if (updateBranch != null) {
            LOGGER.info("Updating branch {} with version {}", updateBranch.name(), updateBranch.version());
            if (developmentBranches.stream().noneMatch(developmentBranch -> developmentBranch.name().equals(updateBranch.name()))) {
                throw new InvalidUserDataException("Branch " + updateBranch.name() + " does not exist");
            }
            developmentBranches.removeIf(developmentBranch -> developmentBranch.name().equals(updateBranch.name()));
            developmentBranches.add(updateBranch);
        }

        developmentBranches.sort(Comparator.comparing(DevelopmentBranch::version).reversed());

        JsonNode jsonNode = objectMapper.readTree(new FileInputStream(branchesFile));
        ArrayNode updatedBranches = objectMapper.createArrayNode();
        for (DevelopmentBranch branch : developmentBranches) {
            ObjectNode objectNode = objectMapper.createObjectNode();
            objectNode.put("branch", branch.name());
            objectNode.put("version", branch.version().toString());
            updatedBranches.add(objectNode);
        }
        ((ObjectNode) jsonNode).replace("branches", updatedBranches);

        DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
        prettyPrinter.indentArraysWith(new DefaultIndenter("  ", DefaultIndenter.SYS_LF));
        objectMapper.writer(prettyPrinter).writeValue(branchesFile, jsonNode);
    }

    private List<DevelopmentBranch> readBranches(File branchesFile) {
        if (branchesFile.isFile() == false) {
            throw new InvalidUserDataException("File branches.json has not been found in " + branchesFile.getAbsolutePath());
        }

        try {
            byte[] branchesBytes = Files.readAllBytes(branchesFile.toPath());
            return branchesFileParser.parse(branchesBytes);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read branches.json from " + branchesFile.getPath(), e);
        }
    }
}
