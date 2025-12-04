/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.rules.TemporaryFolder;

import java.util.function.Supplier;

public class DefaultClusters {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";
    private static final int NODE_NUM = 3;

    private static String getOldClusterVersion() {
        return System.getProperty("tests.old_cluster_version");
    }

    public static ElasticsearchCluster buildCluster(TemporaryFolder repoDirectory) {
        // String oldVersionString = System.getProperty("tests.old_cluster_version");
        // oldVersionString = System.getProperty("tests.bwc.main.version", oldVersionString);
        // Version oldVersion = Version.fromString(oldVersionString);
        System.out.println("hey from Stateless");

        boolean isDetachedVersion = System.getProperty("tests.bwc.refspec.main") != null;

        var cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(getOldClusterVersion(), isDetachedVersion)
            .nodes(NODE_NUM)
            .user(USER, PASS)
            .setting("xpack.security.autoconfiguration.enabled", "false")
            .setting("path.repo", new Supplier<>() {
                @Override
                @SuppressForbidden(reason = "TemporaryFolder only has io.File methods, not nio.File")
                public String get() {
                    return repoDirectory.getRoot().getPath();
                }
            });

        // Avoid triggering bogus assertion when serialized parsed mappings don't match with original mappings, because _source key is
        // inconsistent. Assume non-parseable versions (serverless) do not need this.
        if (Version.tryParse(getOldClusterVersion()).map(v -> v.before(Version.fromString("8.18.0"))).orElse(false)) {
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }
        return cluster.build();
    }
}
