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
 * An upper bound is the maximum transport version id that should be loaded for a given release branch.
 */
record TransportVersionUpperBound(String branch, String name, TransportVersionId id) {
    public static TransportVersionUpperBound fromString(Path file, String contents) {
        String filename = file.getFileName().toString();
        assert filename.endsWith(".csv");
        String branch = filename.substring(0, filename.length() - 4);

        String[] parts = contents.split(",");
        if (parts.length != 2) {
            throw new IllegalStateException("Invalid transport version upper bound file [" + file + "]: " + contents);
        }

        return new TransportVersionUpperBound(branch, parts[0], TransportVersionId.fromString(parts[1]));
    }
}
