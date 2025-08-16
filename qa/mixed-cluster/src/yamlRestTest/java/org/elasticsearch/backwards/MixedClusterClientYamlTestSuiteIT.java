/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.backwards;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

@TimeoutSuite(millis = 60 * TimeUnits.MINUTE) // some of the windows test VMs are slow as hell
public class MixedClusterClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .withNode(n -> n.version(System.getProperty("tests.old_cluster_version")))
        .withNode(n -> n.version(System.getProperty("tests.old_cluster_version")))
        .withNode(n -> n.version(Version.CURRENT.toString()))
        .withNode(n -> n.version(Version.CURRENT.toString()))
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        // There is a chance we have more master changes than "normal", so to avoid this test from failing, we increase the
        // threshold (as this purpose of this test isn't to test that specific indicator).
        .setting("health.master_history.no_master_transitions_threshold", () -> "10", s -> s.getVersion().onOrAfter("8.4.0"))
        .apply(c -> {
            if (Version.fromString(System.getProperty("tests.old_cluster_version")).before(Version.fromString("8.18.0"))) {
                c.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper").jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
            }
        })
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .feature(FeatureFlag.SUB_OBJECTS_AUTO_ENABLED)
        .build();

    public MixedClusterClientYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected boolean randomizeContentType() {
        return false;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
