/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.Action;
import org.gradle.api.file.FileCopyDetails;

/**
 * Shared file-permission actions for distribution copy specs.
 */
public final class DistributionPermissions {

    private DistributionPermissions() {}

    public static Action<FileCopyDetails> modules(String os) {
        return details -> {
            String penultimate = penultimateSegment(details);
            if ("bin".equals(penultimate) || ("darwin".equals(os) && "MacOS".equals(penultimate))) {
                unix(details, 0755);
            } else {
                unix(details, 0644);
            }
        };
    }

    public static Action<FileCopyDetails> jdk() {
        return details -> {
            String penultimate = penultimateSegment(details);
            String last = lastSegment(details);
            if ("bin".equals(penultimate) || "jspawnhelper".equals(last)) {
                unix(details, 0755);
            } else {
                unix(details, 0644);
            }
            if ("src.zip".equals(details.getName())) {
                details.exclude();
            }
        };
    }

    public static Action<FileCopyDetails> serverLauncher() {
        return details -> {
            if ("server-launcher".equals(details.getName()) || "server-launcher.exe".equals(details.getName())) {
                unix(details, 0755);
            }
        };
    }

    private static String penultimateSegment(FileCopyDetails details) {
        String[] segments = details.getRelativePath().getSegments();
        return segments.length < 2 ? null : segments[segments.length - 2];
    }

    private static String lastSegment(FileCopyDetails details) {
        String[] segments = details.getRelativePath().getSegments();
        return segments.length < 1 ? null : segments[segments.length - 1];
    }

    private static void unix(FileCopyDetails details, int mode) {
        details.permissions(permissions -> permissions.unix(mode));
    }
}
