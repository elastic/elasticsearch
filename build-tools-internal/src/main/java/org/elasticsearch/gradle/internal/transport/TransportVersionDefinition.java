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
import java.util.ArrayList;
import java.util.List;

record TransportVersionDefinition(String name, List<TransportVersionId> ids, boolean isReferable) {
    public static TransportVersionDefinition fromString(Path file, String contents, boolean isReferable) {
        String filename = file.getFileName().toString();
        assert filename.endsWith(".csv");
        String name = filename.substring(0, filename.length() - 4);
        List<TransportVersionId> ids = new ArrayList<>();

        String idsLine = null;
        if (contents.isEmpty() == false) {
            String[] lines = contents.split(System.lineSeparator());
            for (String line : lines) {
                line = line.replaceAll("\\s+", "");
                if (line.startsWith("#") == false) {
                    idsLine = line;
                    break;
                }
            }
        }
        if (idsLine != null) {
            for (String rawId : idsLine.split(",")) {
                try {
                    ids.add(TransportVersionId.fromString(rawId));
                } catch (NumberFormatException e) {
                    throw new IllegalStateException("Failed to parse id " + rawId + " in " + file, e);
                }
            }
        }

        return new TransportVersionDefinition(name, ids, isReferable);
    }
}
