/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.info.DevelopmentBranch;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.initialization.layout.BuildLayout;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class UpdateBranchesJsonTask extends DefaultTask {

    private static final Logger LOGGER = Logging.getLogger(UpdateBranchesJsonTask.class);

    private final Project project;
    private final ObjectMapper objectMapper;

    @Nullable
    private DevelopmentBranch addBranch;
    @Nullable
    private String removeBranch;
    @Nullable
    private DevelopmentBranch updateBranch;

    @Inject
    public UpdateBranchesJsonTask(Project project) {
        this.project = project;
        // TODO jozala: is there a better way to pass ObjectMapper?
        this.objectMapper = new ObjectMapper();
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
    public void setCurrent(String branchAndVersion) {
        this.updateBranch = toDevelopmentBranch(branchAndVersion);
    }

    private DevelopmentBranch toDevelopmentBranch(String branchAndVersion) {
        String[] parts = branchAndVersion.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Expected branch and version in format <branch>:<version>");
        }
        return new DevelopmentBranch(parts[0], Version.fromString(parts[1]));
    }

    @TaskAction
    public void executeTask() throws IOException {
        File branchesFile = new File(Util.locateElasticsearchWorkspace(project.getGradle()), "branches.json");
        List<DevelopmentBranch> developmentBranches = readBranches(branchesFile);

        if (addBranch != null) {
            LOGGER.info("Adding branch {} with version {}", addBranch.name(), addBranch.version());
            developmentBranches.add(addBranch);
        }
        if (removeBranch != null) {
            LOGGER.info("Removing branch {}", removeBranch);
            developmentBranches.removeIf(developmentBranch -> developmentBranch.name().equals(removeBranch));
        }
        if (updateBranch != null) {
            LOGGER.info("Updating branch {} with version {}", updateBranch.name(), updateBranch.version());
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
        objectMapper.writeValue(branchesFile, jsonNode);

        // TODO jozala: where to actually push this change to Git?
    }

    private List<DevelopmentBranch> readBranches(File branchesFile) throws IOException {
        // TODO jozala: move to some common space and reuse with GlobalBuildInfoPlugin
        List<DevelopmentBranch> branches = new ArrayList<>();
        try (InputStream is = new FileInputStream(branchesFile)) {
            JsonNode json = objectMapper.readTree(is);
            for (JsonNode node : json.get("branches")) {
                branches.add(
                        new DevelopmentBranch(
                                node.get("branch").asText(),
                                Version.fromString(node.get("version").asText())
                        )
                );
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return branches;
    }


}
