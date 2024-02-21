/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.internal.BuildExtension;
import org.elasticsearch.plugins.ExtensionLoader;

import java.io.IOException;
import java.util.Objects;
import java.util.ServiceLoader;

public interface BuildVersion extends Writeable {
    // TODO[wrb]: rename to isBeforeMinimumCompatible or something
    boolean isCompatibleWithCurrent();

    boolean isFutureVersion();

    // temporary
    @Deprecated
    default Version toVersion() {
        return null;
    }

    // temporary
    @Deprecated
    static BuildVersion fromVersion(Version version) {
        return new DefaultBuildVersion(version.id());
    }

    static BuildVersion current() {
        return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class))
            .map(BuildExtension::currentBuildVersion)
            .orElse(new DefaultBuildVersion(Version.CURRENT.id()));
    }

    class DefaultBuildVersion implements BuildVersion {

        // TODO[wrb]: hold on to a Version field
        private final int versionId;

        DefaultBuildVersion(int versionId) {
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
            DefaultBuildVersion that = (DefaultBuildVersion) o;
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
}
