/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.gpu;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.BeforeClass;
import org.junit.ClassRule;

public class GPUClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    @BeforeClass
    public static void setup() {
        assumeTrue("cuvs not supported", GPUSupport.isSupported());
    }

    @ClassRule
    public static ElasticsearchCluster cluster = createCluster();

    private static ElasticsearchCluster createCluster() {
        var builder = ElasticsearchCluster.local()
            .nodes(1)
            .module("gpu")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "false")
            // Needed to get access to raw vectors from Lucene scorers
            .jvmArg("--add-opens=org.apache.lucene.core/org.apache.lucene.codecs.lucene99=org.elasticsearch.server")
            .jvmArg("--add-opens=org.apache.lucene.core/org.apache.lucene.internal.vectorization=org.elasticsearch.server");

        var libraryPath = System.getenv("LD_LIBRARY_PATH");
        if (libraryPath != null) {
            builder.environment("LD_LIBRARY_PATH", libraryPath);
        }
        return builder.build();
    }

    public GPUClientYamlTestSuiteIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
