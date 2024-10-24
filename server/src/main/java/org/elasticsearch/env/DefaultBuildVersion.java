/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.env;

import org.elasticsearch.Version;

import java.util.Objects;

/**
 * A {@link BuildVersion} that uses the same identifiers and compatibility constraints
 * as {@link Version}.
 *
 * <p>This default implementation of BuildVersion delegates to the {@link Version} class.
 * It's intended to let us check wither a version identifier is "too old" or "too new."
 * "Too old" is determined by {@code Version.CURRENT.minimumCompatibilityVersion()},
 * and "too new" is anything that comes after {@code Version.CURRENT}. This lets us
 * give users simple rules in terms of public-facing release versions for Elasticsearch
 * compatibility when upgrading nodes and prevents downgrades in place.</p>
 */
final class DefaultBuildVersion extends BuildVersion {

    public static BuildVersion CURRENT = new DefaultBuildVersion(Version.CURRENT.id());

    private final Version version;

    DefaultBuildVersion(int versionId) {
        assert versionId >= 0 : "Release version IDs must be non-negative integers";
        this.version = Version.fromId(versionId);
    }

    DefaultBuildVersion(String version) {
        this.version = Version.fromString(Objects.requireNonNull(version));
    }

    @Override
    public boolean onOrAfterMinimumCompatible() {
        return Version.CURRENT.minimumCompatibilityVersion().onOrBefore(version);
    }

    @Override
    public boolean isFutureVersion() {
        return Version.CURRENT.before(version);
    }

    @Override
    public int id() {
        return version.id();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultBuildVersion that = (DefaultBuildVersion) o;
        return version.equals(that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version.id());
    }

    @Override
    public String toString() {
        return version.toString();
    }
}
