/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.internal;

import org.elasticsearch.BuildVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

// TODO[wrb]: make package-private
public class DefaultBuildVersion implements BuildVersion {

    public static BuildVersion CURRENT = new DefaultBuildVersion(Version.CURRENT.id());

    // TODO[wrb]: hold on to a Version field
    private final int versionId;

    public DefaultBuildVersion(int versionId) {
        this.versionId = versionId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(this.versionId);
    }

    @Override
    public boolean isCompatibleWithCurrent() {
        return Version.CURRENT.minimumCompatibilityVersion().onOrBefore(Version.fromId(versionId));
    }

    @Override
    public boolean isFutureVersion() {
        return Version.CURRENT.before(Version.fromId(versionId));
    }

    @Override
    public Version toVersion() {
        return Version.fromId(this.versionId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        org.elasticsearch.internal.DefaultBuildVersion that = (org.elasticsearch.internal.DefaultBuildVersion) o;
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(versionId);
    }
}
