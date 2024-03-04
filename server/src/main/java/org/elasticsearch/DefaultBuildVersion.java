/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import java.util.Objects;

// TODO[wrb]: make package-private once default implementations are removed in BuildExtension
public class DefaultBuildVersion extends BuildVersion {

    public static BuildVersion CURRENT = new org.elasticsearch.DefaultBuildVersion(Version.CURRENT.id());
    public static BuildVersion EMPTY = new org.elasticsearch.DefaultBuildVersion(0);

    private final int versionId;
    private final Version version;

    public DefaultBuildVersion(int versionId) {
        this.versionId = versionId;
        this.version = Version.fromId(versionId);
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
        return versionId;
    }

    @Override
    public Version toVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        org.elasticsearch.DefaultBuildVersion that = (org.elasticsearch.DefaultBuildVersion) o;
        return versionId == that.versionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(versionId);
    }

    @Override
    public String toString() {
        return Version.fromId(versionId).toString();
    }
}
