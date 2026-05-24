/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.gradle.Version;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A parser for the branches.json file
 */
public class BranchesFileParser {

    private final ObjectMapper objectMapper;

    public BranchesFileParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<DevelopmentBranch> parse(byte[] bytes) {
        List<DevelopmentBranch> branches = new ArrayList<>();
        try {
            JsonNode json = objectMapper.readTree(bytes);
            for (JsonNode node : json.get("branches")) {
                branches.add(new DevelopmentBranch(node.get("branch").asText(), Version.fromString(node.get("version").asText())));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse content of branches.json", e);
        }

        return branches;
    }
}
