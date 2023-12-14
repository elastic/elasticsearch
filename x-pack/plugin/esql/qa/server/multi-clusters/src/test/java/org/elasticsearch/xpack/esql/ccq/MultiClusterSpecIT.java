/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.elasticsearch.xpack.ql.CsvSpecReader;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiClusterSpecIT extends EsqlSpecTestCase {
    static final Version bwcVersion = Version.fromString(System.getProperty("tests.bwc_nodes_version"));
    static final Version newVersion = Version.CURRENT;

    public MultiClusterSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvSpecReader.CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, convertToRemoteIndices(testCase));
    }

    static CsvSpecReader.CsvTestCase convertToRemoteIndices(CsvSpecReader.CsvTestCase testCase) {
        testCase.query = Fixtures.convertQueryToRemoteIndex(testCase.query);
        return testCase;
    }

    @Override
    protected void shouldSkipTest(CsvSpecReader.CsvTestCase testCase, String testName) {
        assumeFalse("only run spec tests with two clusters", Fixtures.testAgainstRemoteClusterOnly());
        assumeFalse("CCQ doesn't support enrich yet", Fixtures.hasEnrich(testCase.query));
        assumeFalse("can't test with _index metadata", Fixtures.hasIndexMetadata(testCase.query));
        assumeTrue("Test " + testName + " is skipped on " + bwcVersion, isEnabled(testName, bwcVersion));
        assumeTrue("Test " + testName + " is skipped on " + newVersion, isEnabled(testName, newVersion));
    }

    @Override
    protected void loadTestDataIfNeeded(RestClient localClient) throws IOException {
        if (Fixtures.testAgainstRemoteClusterOnly()) {
            super.loadTestDataIfNeeded(localClient);
        } else {
            try (RestClient remoteClient = remoteClusterClient()) {
                super.loadTestDataIfNeeded(twoClustersClient(localClient, remoteClient));
            }
        }
    }

    @Override
    protected void wipeTestData(RestClient localClient) throws IOException {
        super.wipeTestData(localClient);
        if (Fixtures.testAgainstRemoteClusterOnly() == false) {
            try (RestClient remoteClient = remoteClusterClient()) {
                super.wipeTestData(twoClustersClient(localClient, remoteClient));
            }
        }
    }

    RestClient twoClustersClient(RestClient localClient, RestClient remoteClient) throws IOException {
        RestClient client = mock(RestClient.class);
        when(client.performRequest(any())).then(invocation -> {
            Request request = invocation.getArgument(0);
            if (request.getEndpoint().contains("_bulk")) {
                if (randomBoolean()) {
                    return remoteClient.performRequest(request);
                } else {
                    return localClient.performRequest(request);
                }
            } else {
                localClient.performRequest(request);
                return remoteClient.performRequest(request);
            }
        });
        return client;
    }

    private RestClient remoteClusterClient() throws IOException {
        String remoteCluster = System.getProperty("tests.rest.remote_cluster");
        if (remoteCluster == null) {
            throw new RuntimeException("remote_cluster wasn't specified");
        }
        var clusterHosts = parseClusterHosts(remoteCluster);
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }
}
