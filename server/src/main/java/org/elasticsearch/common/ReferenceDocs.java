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
 * Encapsulates links to pages in the reference docs, so that for example we can include URLs in logs and API outputs. Each instance's
 * {@link #toString()} yields (a string representation of) a URL for the relevant docs.
 */
public enum ReferenceDocs {
    INITIAL_MASTER_NODES("important-settings.html#initial_master_nodes"),
    DISCOVERY_TROUBLESHOOTING("discovery-troubleshooting.html"),
    UNSTABLE_CLUSTER_TROUBLESHOOTING("cluster-fault-detection.html#cluster-fault-detection-troubleshooting"),
    LAGGING_NODE_TROUBLESHOOTING("cluster-fault-detection.html#_diagnosing_lagging_nodes");

    private final String relativePath;

    ReferenceDocs(String relativePath) {
        this.relativePath = relativePath;
    }

    static final String UNRELEASED_VERSION_COMPONENT = "master";
    static final String VERSION_COMPONENT = getVersionComponent(Version.CURRENT, Build.CURRENT.isSnapshot());

    /**
     * Compute the version component of the URL path (e.g. {@code 8.5} or {@code master}) for a particular version of Elasticsearch. Exposed
     * for testing, but all items use {@link #VERSION_COMPONENT} ({@code getVersionComponent(Version.CURRENT, Build.CURRENT.isSnapshot())})
     * which relates to the current version and build.
     */
    static String getVersionComponent(Version version, boolean isSnapshot) {
        return isSnapshot && version.revision == 0 ? UNRELEASED_VERSION_COMPONENT : version.major + "." + version.minor;
    }

    @Override
    public String toString() {
        return "https://www.elastic.co/guide/en/elasticsearch/reference/" + VERSION_COMPONENT + "/" + relativePath;
    }

    /**
     * @return a list of links which are expected to exist for a particular version of Elasticsearch, which is either all links (if the docs
     * are released) or no links (otherwise). Exposed for testing the behaviour of different versions, but for the current version use
     * {@link #linksToVerify()}.
     */
    static List<ReferenceDocs> linksToVerify(Version version, boolean isSnapshot) {
        if (version.revision == 0 && isSnapshot == false) {
            return List.of();
        }
        return Arrays.stream(values()).toList();
    }

    /**
     * @return a list of links which are expected to exist for the current version of Elasticsearch, which is either all links (if the docs
     * are released) or no links (otherwise).
     */
    public static List<ReferenceDocs> linksToVerify() {
        return linksToVerify(Version.CURRENT, Build.CURRENT.isSnapshot());
    }
}
