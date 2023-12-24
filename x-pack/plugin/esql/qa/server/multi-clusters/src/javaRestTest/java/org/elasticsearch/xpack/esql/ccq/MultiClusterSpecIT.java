/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.elasticsearch.xpack.ql.CsvSpecReader;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiClusterSpecIT extends EsqlSpecTestCase {

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);
    private static boolean upgraded = false;

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Before
    public void upgradeLocalCluster() throws Exception {
        if (upgraded == false) {
            upgraded = true;
            closeClients();
            localCluster.upgradeToVersion(Version.CURRENT);
            initClient();
        }
    }

    public MultiClusterSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvSpecReader.CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, convertToRemoteIndices(testCase));
    }

    static CsvSpecReader.CsvTestCase convertToRemoteIndices(CsvSpecReader.CsvTestCase testCase) {
        String oldQuery = testCase.query;
        testCase.query = Fixtures.convertQueryToRemoteIndex(oldQuery);
        int offset = testCase.query.length() - oldQuery.length();
        if (offset != 0) {
            final String pattern = "Line (\\d+):(\\d+):";
            final Pattern regex = Pattern.compile(pattern);
            testCase.adjustExpectedWarnings(warning -> {
                Matcher matcher = regex.matcher(warning);
                if (matcher.find()) {
                    int line = Integer.parseInt(matcher.group(1));
                    if (line == 1) {
                        int position = Integer.parseInt(matcher.group(2));
                        int newPosition = position + offset;
                        return warning.replaceFirst(pattern, "Line " + line + ":" + newPosition + ":");
                    }
                }
                return warning;
            });
        }
        return testCase;
    }

    @Override
    protected void shouldSkipTest(String testName) {
        super.shouldSkipTest(testName);
        assumeFalse("CCQ doesn't support enrich yet", Fixtures.hasEnrich(testCase.query));
        assumeFalse("can't test with _index metadata", Fixtures.hasIndexMetadata(testCase.query));
        assumeTrue("Test " + testName + " is skipped on " + Clusters.oldVersion(), isEnabled(testName, Clusters.oldVersion()));
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] localHosts) throws IOException {
        RestClient localClient = super.buildClient(settings, localHosts);
        HttpHost[] remoteHosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
        RestClient remoteClient = super.buildClient(settings, remoteHosts);
        return twoClients(localClient, remoteClient);
    }

    static RestClient twoClients(RestClient localClient, RestClient remoteClient) throws IOException {
        RestClient twoClients = mock(RestClient.class);
        when(twoClients.performRequest(any())).then(invocation -> {
            Request request = invocation.getArgument(0);
            if (request.getEndpoint().contains("_query")) {
                return localClient.performRequest(request);
            } else if (request.getEndpoint().contains("_bulk")) {
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
        doAnswer(invocation -> {
            IOUtils.close(localClient, remoteClient);
            return null;
        }).when(twoClients).close();
        return twoClients;
    }
}
