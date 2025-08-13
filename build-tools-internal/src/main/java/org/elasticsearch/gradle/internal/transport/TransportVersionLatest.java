/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

record TransportVersionLatest(String branch, String name, TransportVersionId id) {
    public static TransportVersionLatest fromString(String filename, String contents) {
        assert filename.endsWith(".csv");
        String branch = filename.substring(0, filename.length() - 4);

        String[] parts = contents.split(",");
        if (parts.length != 2) {
            throw new IllegalStateException("Invalid transport version latest file [" + filename + "]: " + contents);
        }

        return new TransportVersionLatest(branch, parts[0], TransportVersionId.fromString(parts[1]));
    }
}
