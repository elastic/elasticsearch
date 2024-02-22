/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.internal.BuildExtension;
import org.elasticsearch.plugins.ExtensionLoader;

import java.util.ServiceLoader;

public interface BuildVersion extends Writeable {
    boolean onOrAfterMinimumCompatible();

    boolean isFutureVersion();

    // temporary
    // TODO[wrb]: remove from PersistedClusterStateService
    // TODO[wrb]: remove from security bootstrap checks
    @Deprecated
    default Version toVersion() {
        return null;
    }

    static BuildVersion fromVersionId(int versionId) {
        return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class))
            .map(ext -> ext.fromVersionId(versionId))
            .orElse(new DefaultBuildVersion(versionId));
    }

    static BuildVersion current() {
        return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class))
            .map(BuildExtension::currentBuildVersion)
            .orElse(DefaultBuildVersion.CURRENT);
    }

    static BuildVersion empty() {
        return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class))
            .map(ext -> ext.fromVersionId(0))
            .orElse(DefaultBuildVersion.EMPTY);
    }

    // only exists for NodeMetadata#toXContent
    default int id() {
        return -1;
    }
}
