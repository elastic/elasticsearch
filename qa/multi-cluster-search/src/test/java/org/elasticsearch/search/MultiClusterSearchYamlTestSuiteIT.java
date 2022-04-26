/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.BeforeClass;

@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class MultiClusterSearchYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static Version remoteEsVersion = null;

    @BeforeClass
    public static void determineRemoteClusterMinimumVersion() {
        String remoteClusterVersion = System.getProperty("tests.rest.remote_cluster_version");
        if (remoteClusterVersion != null) {
            remoteEsVersion = Version.fromString(remoteClusterVersion);
        }
    }

    protected ClientYamlTestExecutionContext createRestTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient
    ) {
        return new ClientYamlTestExecutionContext(clientYamlTestCandidate, clientYamlTestClient, randomizeContentType()) {

            /**
             * Since the esVersion is used to skip tests in ESClientYamlSuiteTestCase, we also take into account the
             * remote cluster version here and return it if it is lower than the local client version. This is used to
             * skip tests if some feature isn't available on the remote cluster yet.
             */
            @Override
            public Version esVersion() {
                Version clientEsVersion = clientYamlTestClient.getEsVersion();
                if (remoteEsVersion == null) {
                    return clientEsVersion;
                } else {
                    return remoteEsVersion.before(clientEsVersion) ? remoteEsVersion : clientEsVersion;
                }
            }
        };
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    public MultiClusterSearchYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }
}
