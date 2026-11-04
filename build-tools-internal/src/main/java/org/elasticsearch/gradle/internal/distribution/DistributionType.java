/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import java.util.Arrays;
import java.util.Locale;

/**
 * The distribution formats whose packaging files (scripts, config) are assembled with distribution specific
 * {@link TokenExpansions}. Modelled as an enum so that adding a format forces every format specific rule to be
 * revisited rather than silently falling back to the behaviour of another format.
 */
public enum DistributionType {
    TAR,
    ZIP,
    DEB,
    RPM;

    /**
     * The lower case identifier of the format as used in build scripts and file names, e.g. {@code deb}.
     */
    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Whether this is an OS package installed by a package manager, as opposed to an archive extracted by the user.
     */
    public boolean isPackage() {
        return switch (this) {
            case TAR, ZIP -> false;
            case DEB, RPM -> true;
        };
    }

    /**
     * Parses the lower case identifier of a format, e.g. {@code deb}.
     */
    public static DistributionType fromId(String id) {
        return Arrays.stream(values())
            .filter(t -> t.id().equals(id))
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException(
                    "Unknown distribution type [" + id + "], expected one of " + Arrays.stream(values()).map(DistributionType::id).toList()
                )
            );
    }
}
