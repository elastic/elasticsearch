/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.internal;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.plugins.ExtensionLoader;

import java.io.IOException;
import java.util.ServiceLoader;

public interface BuildVersion extends Writeable {
    boolean isCompatibleWithCurrent();

    boolean isFutureVersion();

    static BuildVersion current() {
        return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class))
            .map(BuildExtension::currentBuildVersion)
            .orElse(new DefaultBuildVersion(Version.CURRENT.id()));
    }

    class DefaultBuildVersion implements BuildVersion {

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
            return Version.CURRENT.isCompatible(Version.fromId(versionId));
        }

        @Override
        public boolean isFutureVersion() {
            return Version.CURRENT.before(Version.fromId(versionId));
        }
    }
}
