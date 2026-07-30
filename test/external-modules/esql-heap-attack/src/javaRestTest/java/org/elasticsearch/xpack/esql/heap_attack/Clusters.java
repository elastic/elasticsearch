/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

public class Clusters {
    public static final int HEAP_SIZE_IN_MB = 512;

    static ElasticsearchCluster buildCluster() {
        return buildClusterSpec().build();
    }

    static LocalClusterSpecBuilder<ElasticsearchCluster> buildClusterSpec() {
        var spec = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .module("test-esql-heap-attack")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("esql.query.allow_partial_results", "false")
            // Allow setup to index 16MB source documents; tests lower the request breaker around the queries under test.
            .setting("indexing_pressure.memory.primary.limit", "20%")
            .setting("logger.org.elasticsearch.compute.lucene.read", "DEBUG")
            .jvmArg("-Xmx" + HEAP_SIZE_IN_MB + "m");
        String javaVersion = JvmInfo.jvmInfo().version();
        if (javaVersion.equals("20") || javaVersion.equals("21")) {
            // see https://github.com/elastic/elasticsearch/issues/99592
            spec.jvmArg("-XX:+UnlockDiagnosticVMOptions -XX:+G1UsePreventiveGC");
        }
        return spec;
    }
}
