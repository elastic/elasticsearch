/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import java.util.Comparator;
import java.util.Objects;

/**
 * Encapsulates comparison and printing logic for an x.y.
 */
public record MinorVersion(int major, int minor) implements Comparable<MinorVersion> {
    /**
     * Converts a QualifiedVersion into a MinorVersion by deleting all but the major and minor components.
     */
    public static MinorVersion of(final QualifiedVersion v) {
        Objects.requireNonNull(v);
        return new MinorVersion(v.major(), v.minor());
    }

    @Override
    public String toString() {
        return major + "." + minor;
    }

    /** Generate version string with underscore instead of dot */
    public String underscore() {
        return major + "_" + minor;
    }

    private static final Comparator<MinorVersion> COMPARATOR = Comparator.comparing((MinorVersion v) -> v.major)
        .thenComparing(v -> v.minor);

    @Override
    public int compareTo(MinorVersion other) {
        return COMPARATOR.compare(this, other);
    }

    public boolean isBefore(MinorVersion other) {
        return this.compareTo(other) < 0;
    }
}
