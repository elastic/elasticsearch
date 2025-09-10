/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.InvalidIndexNameException;

import java.util.Locale;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Generates valid Elasticsearch index names.
 */
public final class IndexNameGenerator {

    public static final Pattern ILLEGAL_INDEXNAME_CHARS_REGEX = Pattern.compile("[/:\"*?<>|# ,\\\\]+");
    public static final int MAX_GENERATED_UUID_LENGTH = 4;

    private IndexNameGenerator() {}

    /**
     * This generates a valid unique index name by using the provided prefix, appended with a generated UUID, and the index name.
     */
    public static String generateValidIndexName(String prefix, String indexName) {
        String randomUUID = generateValidIndexSuffix(UUIDs::randomBase64UUID);
        randomUUID = randomUUID.substring(0, Math.min(randomUUID.length(), MAX_GENERATED_UUID_LENGTH));
        return prefix + randomUUID + "-" + indexName;
    }

    /**
     * Generates a valid index suffix, removing any illegal characters.
     * @param randomGenerator supplier of the initial (random) suffix
     * @return a valid index suffix without any illegal characters
     * @throws IllegalArgumentException if removing illegal characters resulted in an empty string
     */
    public static String generateValidIndexSuffix(Supplier<String> randomGenerator) {
        String randomSuffix = randomGenerator.get().toLowerCase(Locale.ROOT);
        randomSuffix = ILLEGAL_INDEXNAME_CHARS_REGEX.matcher(randomSuffix).replaceAll("");
        if (randomSuffix.isEmpty()) {
            throw new IllegalArgumentException("unable to generate random index name suffix");
        }

        return randomSuffix;
    }

    /**
     * Validates the provided index name against the provided cluster state. This checks the index name for invalid characters
     * and that it doesn't clash with existing indices or aliases.
     * Returns null for valid indices.
     */
    @Nullable
    public static ActionRequestValidationException validateGeneratedIndexName(String generatedIndexName, ProjectState projectState) {
        ActionRequestValidationException err = new ActionRequestValidationException();
        try {
            MetadataCreateIndexService.validateIndexOrAliasName(generatedIndexName, InvalidIndexNameException::new);
        } catch (InvalidIndexNameException e) {
            err.addValidationError(e.getMessage());
        }
        if (projectState.routingTable().hasIndex(generatedIndexName) || projectState.metadata().hasIndex(generatedIndexName)) {
            err.addValidationError("the index name we generated [" + generatedIndexName + "] already exists");
        }
        if (projectState.metadata().hasAlias(generatedIndexName)) {
            err.addValidationError("the index name we generated [" + generatedIndexName + "] already exists as alias");
        }

        if (err.validationErrors().size() > 0) {
            return err;
        } else {
            return null;
        }
    }
}
