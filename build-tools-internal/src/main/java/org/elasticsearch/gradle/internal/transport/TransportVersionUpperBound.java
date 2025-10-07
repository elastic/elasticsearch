/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import java.nio.file.Path;

/**
 * An object to represent the loaded version of a transport version upper bound.
 *
 * An upper bound is the maximum transport version definitionId that should be loaded for a given release branch.
 */
record TransportVersionUpperBound(String name, String definitionName, TransportVersionId definitionId) {
    public static TransportVersionUpperBound fromString(Path file, String contents) {
        String filename = file.getFileName().toString();
        assert filename.endsWith(".csv");
        int slashIndex = filename.lastIndexOf('/');
        String branch = filename.substring(slashIndex == -1 ? 0 : (slashIndex + 1), filename.length() - 4);

        String idsLine = null;
        // Regardless of whether windows newlines exist (they could be added by git), we split on line feed.
        // All we care about skipping lines with the comment character, so the remaining \r won't matter
        String[] lines = contents.split("\n");
        for (String line : lines) {
            line = line.strip();
            if (line.startsWith("#") == false) {
                idsLine = line;
                break;
            }
        }
        String[] parts = idsLine.split(",");
        if (parts.length != 2) {
            throw new IllegalStateException("Invalid transport version upper bound file [" + file + "]: " + contents);
        }

        return new TransportVersionUpperBound(branch, parts[0], TransportVersionId.fromString(parts[1]));
    }
}
