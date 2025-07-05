/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class TopStatsIT extends ESRestTestCase {
    private static final int DOCS_PER_CLUSTER = 400_000;
    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private static TestFeatureService remoteFeaturesService;

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    record Doc(long id, String highCard, String lowCard) {}

    final String localIndex = "test-local-index";
    List<Doc> localDocs = List.of();
    final String remoteIndex = "test-remote-index";
    List<Doc> remoteDocs = List.of();

    @Before
    public void setUpIndices() throws Exception {

        List<String> highCard = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            highCard.add(randomAlphaOfLength(12));
        }
        for (int i = 0; i < 40; i++) {
            highCard.add(highCard.get(i));
        }
        for (int i = 0; i < 30; i++) {
            highCard.add(highCard.get(i));
        }
        for (int i = 0; i < 20; i++) {
            highCard.add(highCard.get(i));
        }
        for (int i = 0; i < 10; i++) {
            highCard.add(highCard.get(i));
        }
        for (int i = 0; i < 5; i++) {
            highCard.add(highCard.get(i));
        }

        List<String> lowCard = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            lowCard.add(randomFrom(List.of("internal:", "")) + randomAlphaOfLength(20));
        }

        final String mapping = """
             "properties": {
               "highCard": { "type": "keyword" },
               "lowCard": { "type": "keyword" }
             }
            """;

        RestClient localClient = client();
        localDocs = createDocs(DOCS_PER_CLUSTER, highCard, lowCard);

        createIndex(
            localClient,
            localIndex,
            Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)).build(),
            mapping,
            null
        );
        indexDocs(localClient, localIndex, localDocs);

        remoteDocs = createDocs(DOCS_PER_CLUSTER, highCard, lowCard);

        try (RestClient remoteClient = remoteClusterClient()) {
            createIndex(
                remoteClient,
                remoteIndex,
                Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)).build(),
                mapping,
                null
            );
            indexDocs(remoteClient, remoteIndex, remoteDocs);
        }
    }

    private List<Doc> createDocs(int nDocs, List<String> highCard, List<String> lowCard) {
        List<Doc> result = new ArrayList<>();
        for (int i = 0; i < nDocs; i++) {
            result.add(new Doc(i, randomFrom(highCard), randomInt(50) < 3 ? null : randomFrom(lowCard)));
        }
        return result;
    }

    @After
    public void wipeIndices() throws Exception {
        try (RestClient remoteClient = remoteClusterClient()) {
            deleteIndex(remoteClient, remoteIndex);
        }
    }

    void indexDocs(RestClient client, String index, List<Doc> docs) throws IOException {
        logger.info("--> indexing {} docs to index {}", docs.size(), index);
        int counter = 0;
        while (counter < docs.size()) {
            Request req = new Request("POST", "/" + index + "/_bulk");
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < 1000 && counter < docs.size(); j++) {
                Doc doc = docs.get(counter++);
                builder.append("{ \"index\" : { \"_index\" : \"" + index + "\"} }\n" + "{\"highCard\" : \"" + doc.highCard + "\"");
                if (doc.lowCard != null) {
                    builder.append(", \"lowCard\" : \"" + doc.lowCard + "\"");
                }
                builder.append("}\n");
            }
            if (builder.isEmpty() == false) {
                req.setJsonEntity(builder.toString());
                assertOK(client.performRequest(req));
            }
        }
        logger.info("--> index={}", index);
        refresh(client, index);
    }

    private Map<String, Object> run(String query, boolean includeCCSMetadata) throws IOException {
        var queryBuilder = new RestEsqlTestCase.RequestObjectBuilder().query(query).profile(true);
        if (includeCCSMetadata) {
            queryBuilder.includeCCSMetadata(true);
        }
        Map<String, Object> resp = runEsql(queryBuilder.build());
        logger.info("--> query {} response {}", queryBuilder, resp);
        return resp;
    }

    protected boolean supportsAsync() {
        return false; // TODO: Version.CURRENT.onOrAfter(Version.V_8_13_0); ?? // the Async API was introduced in 8.13.0
    }

    private Map<String, Object> runEsql(RestEsqlTestCase.RequestObjectBuilder requestObject) throws IOException {
        if (supportsAsync()) {
            return RestEsqlTestCase.runEsqlAsync(requestObject);
        } else {
            return RestEsqlTestCase.runEsqlSync(requestObject);
        }
    }

    @SuppressWarnings("unchecked")
    public void testCountSort() throws Exception {

        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
                FROM test-local-index,*:test-remote-index
                | WHERE STARTS_WITH(lowCard, "internal:")
                | STATS spans = COUNT() BY highCard, lowCard
                | SORT spans DESC
                | LIMIT 100
            """, includeCCSMetadata);
        List<List<?>> values = (List<List<?>>) result.get("values");
        for (List<?> value : values) {
            int count = (int) value.get(0);
            String highC = (String) value.get(1);
            String lowC = (String) value.get(2);

            result = run(
                "FROM test-local-index,*:test-remote-index "
                    + "| WHERE highCard == \""
                    + highC
                    + "\" AND "
                    + (lowC == null ? "lowCard is null" : "lowCard == \"" + lowC + "\"")
                    + "| stats count(*)",
                includeCCSMetadata
            );

            int newCount = (int) ((List<List<?>>) result.get("values")).get(0).get(0);
            assertEquals(count, newCount);
        }
    }

    private RestClient remoteClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }

    private static boolean ccsMetadataAvailable() {
        return Clusters.localClusterVersion().onOrAfter(Version.V_8_16_0);
    }

    private static boolean capabilitiesEndpointAvailable() {
        return Clusters.localClusterVersion().onOrAfter(Version.V_8_15_0);
    }

    private static boolean includeCCSMetadata() {
        return ccsMetadataAvailable() && randomBoolean();
    }
}
