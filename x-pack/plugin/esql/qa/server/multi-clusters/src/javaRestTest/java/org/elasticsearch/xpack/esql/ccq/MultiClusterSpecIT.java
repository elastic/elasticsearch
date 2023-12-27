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
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.elasticsearch.xpack.ql.CsvSpecReader;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This suite loads the data into either the local cluster or the remote cluster, then run spec tests with CCQ.
 * TODO: Some spec tests prevents us from splitting data across multiple shards/indices/clusters
 */
public class MultiClusterSpecIT extends EsqlSpecTestCase {

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    public MultiClusterSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvSpecReader.CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, convertToRemoteIndices(testCase));
    }

    @Override
    protected void shouldSkipTest(String testName) {
        super.shouldSkipTest(testName);
        assumeFalse("CCQ doesn't support enrich yet", hasEnrich(testCase.query));
        assumeFalse("can't test with _index metadata", hasIndexMetadata(testCase.query));
        assumeTrue("Test " + testName + " is skipped on " + Clusters.oldVersion(), isEnabled(testName, Clusters.oldVersion()));
    }

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] localHosts) throws IOException {
        RestClient localClient = super.buildClient(settings, localHosts);
        HttpHost[] remoteHosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
        RestClient remoteClient = super.buildClient(settings, remoteHosts);
        return twoClients(localClient, remoteClient);
    }

    /**
     * Creates a new mock client that dispatches every request to both the local and remote clusters, excluding _bulk and _query requests.
     * - '_bulk' requests are randomly sent to either the local or remote cluster to populate data. Some spec tests, such as AVG,
     *   prevent the splitting of bulk requests.
     * - '_query' requests are dispatched to the local cluster only, as we are testing cross-cluster queries.
     */
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

    static CsvSpecReader.CsvTestCase convertToRemoteIndices(CsvSpecReader.CsvTestCase testCase) {
        String query = testCase.query;
        String[] commands = query.split("\\|");
        String first = commands[0].trim();
        if (commands[0].toLowerCase(Locale.ROOT).startsWith("from")) {
            String[] parts = commands[0].split("\\[");
            assert parts.length >= 1 : parts;
            String fromStatement = parts[0];
            String[] localIndices = fromStatement.substring("FROM ".length()).split(",");
            String remoteIndices = Arrays.stream(localIndices)
                .map(index -> "*:" + index.trim() + "," + index.trim())
                .collect(Collectors.joining(","));
            var newFrom = "FROM " + remoteIndices + commands[0].substring(fromStatement.length());
            testCase.query = newFrom + " " + query.substring(first.length());
        }
        int offset = testCase.query.length() - query.length();
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

    static boolean hasEnrich(String query) {
        String[] commands = query.split("\\|");
        for (int i = 0; i < commands.length; i++) {
            commands[i] = commands[i].trim();
            if (commands[i].toLowerCase(Locale.ROOT).startsWith("enrich")) {
                return true;
            }
        }
        return false;
    }

    static boolean hasIndexMetadata(String query) {
        String[] commands = query.split("\\|");
        if (commands[0].trim().toLowerCase(Locale.ROOT).startsWith("from")) {
            String[] parts = commands[0].split("\\[");
            return parts.length > 1 && parts[1].contains("_index");
        }
        return false;
    }
}
