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

    /**
     * The addresses the suite's REST client sends requests to. Any node can coordinate here; serverless replaces this class
     * and sends everything to the search node, which is where its proxy routes {@code _query}.
     */
    static String testRestCluster(ElasticsearchCluster cluster) {
        return cluster.getHttpAddresses();
    }

    static LocalClusterSpecBuilder<ElasticsearchCluster> buildClusterSpec() {
        var spec = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .module("test-esql-heap-attack")
            .module("test-pausable-field")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("esql.query.allow_partial_results", "false")
            // Allow setup to index source documents of about 15MB of CBOR, which the tests of the values that a read of _source holds
            // need: one that is large enough to exhaust the heap of a node if what it holds is not charged to the request breaker,
            // which they then query at its default limit. Indexing pressure counts about 4 times the size of a document for a write,
            // more than the default limit of this heap. This limit is for the coordinating, the primary and, 1.5 times as much, the
            // replica.
            .setting("indexing_pressure.memory.limit", "40%")
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
