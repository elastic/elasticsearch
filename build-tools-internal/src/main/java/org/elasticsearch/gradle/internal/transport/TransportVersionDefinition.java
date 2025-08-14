/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import java.util.ArrayList;
import java.util.List;

record TransportVersionDefinition(String name, List<TransportVersionId> ids) {
    public static TransportVersionDefinition fromString(String filename, String contents) {
        assert filename.endsWith(".csv");
        String name = filename.substring(0, filename.length() - 4);
        List<TransportVersionId> ids = new ArrayList<>();

        if (contents.isEmpty() == false) {
            for (String rawId : contents.split(",")) {
                try {
                    ids.add(TransportVersionId.fromString(rawId));
                } catch (NumberFormatException e) {
                    throw new IllegalStateException("Failed to parse id " + rawId + " in " + filename, e);
                }
            }
        }

        return new TransportVersionDefinition(name, ids);
    }
}
