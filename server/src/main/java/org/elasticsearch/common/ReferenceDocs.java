/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.Build;
import org.elasticsearch.Version;

import java.util.Arrays;
import java.util.List;

/**
 * Encapsulates links to pages in the reference docs, so that for example we can include URLs in logs and API outputs.
 */
public enum ReferenceDocs {
    INITIAL_MASTER_NODES("important-settings.html#initial_master_nodes"),
    DISCOVERY_TROUBLESHOOTING("discovery-troubleshooting.html"),
    UNSTABLE_CLUSTER_TROUBLESHOOTING("cluster-fault-detection.html#cluster-fault-detection-troubleshooting");

    private final String relativePath;

    ReferenceDocs(String relativePath) {
        this.relativePath = relativePath;
    }

    static final String UNRELEASED_VERSION_COMPONENT = "master";
    static final String VERSION_COMPONENT = getVersionComponent(Version.CURRENT, Build.CURRENT.isSnapshot());

    static String getVersionComponent(Version currentVersion, boolean isSnapshot) {
        return isSnapshot && currentVersion.revision == 0
            ? UNRELEASED_VERSION_COMPONENT
            : currentVersion.major + "." + currentVersion.minor;
    }

    @Override
    public String toString() {
        return "https://www.elastic.co/guide/en/elasticsearch/reference/" + VERSION_COMPONENT + "/" + relativePath;
    }

    public static List<ReferenceDocs> linksToVerify(Version currentVersion, boolean isSnapshot) {
        if (currentVersion.revision == 0 && isSnapshot == false) {
            return List.of();
        }
        return Arrays.stream(values()).toList();
    }
}
