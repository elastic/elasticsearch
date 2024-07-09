/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

public class Clusters {
    static ElasticsearchCluster buildCluster() {
        var spec = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .module("test-esql-heap-attack")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .jvmArg("-Xmx512m");
        String javaVersion = JvmInfo.jvmInfo().version();
        if (javaVersion.equals("20") || javaVersion.equals("21")) {
            // see https://github.com/elastic/elasticsearch/issues/99592
            spec.jvmArg("-XX:+UnlockDiagnosticVMOptions -XX:+G1UsePreventiveGC");
        }
        return spec.build();
    }
}
