/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos7x;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OldMappingsIT extends ESRestTestCase {

    public static TemporaryFolder repoDirectory = new TemporaryFolder();

    public static ElasticsearchCluster currentCluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("path.repo", () -> repoDirectory.getRoot().getPath())
        .build();

    public static ElasticsearchCluster oldCluster = ElasticsearchCluster.local()
        .version(Version.fromString("7.17.25"))
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("path.repo", () -> repoDirectory.getRoot().getPath())
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(oldCluster).around(currentCluster);

    @Override
    protected String getTestRestCluster() {
        return oldCluster.getHttpAddresses();
    }

    private Request createIndex(String indexName, String file) throws IOException {
        Request createIndex = new Request("PUT", "/" + indexName);
        int numberOfShards = randomIntBetween(1, 3);

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("settings")
            .field("index.number_of_shards", numberOfShards)
            .endObject()
            .startObject("mappings");
        builder.rawValue(getClass().getResourceAsStream(file), XContentType.JSON);
        builder.endObject().endObject();

        createIndex.setJsonEntity(Strings.toString(builder));
        return createIndex;
    }

    @Before
    public void setupIndex() throws IOException {
        // String repoLocation = PathUtils.get(System.getProperty("tests.repo.location"))
        // .resolve(RandomizedTest.getContext().getTargetClass().getName())
        // .toString();

        String repoLocation = repoDirectory.getRoot().getPath();

        String repoName = "old_mappings_repo";
        String snapshotName = "snap";
        List<String> indices;
        indices = Arrays.asList("filebeat", "custom", "nested");

        List<HttpHost> oldClusterHosts = parseClusterHosts(oldCluster.getHttpAddresses());
        List<HttpHost> clusterHosts = parseClusterHosts(currentCluster.getHttpAddresses());
        // try (RestClient oldEsClient = client()) {
        try (
            RestClient oldEsClient = RestClient.builder(oldClusterHosts.toArray(new HttpHost[oldClusterHosts.size()])).build();
            RestClient esClient = RestClient.builder(clusterHosts.toArray(new HttpHost[clusterHosts.size()])).build();
        ) {
            assertOK(oldEsClient.performRequest(createIndex("filebeat", "filebeat.json")));

            assertOK(oldEsClient.performRequest(createIndex("custom", "custom.json")));
            assertOK(oldEsClient.performRequest(createIndex("nested", "nested.json")));

            Request doc1 = new Request("PUT", "/" + "custom" + "/" + "_doc" + "/" + "1");
            doc1.addParameter("refresh", "true");
            XContentBuilder bodyDoc1 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("apache2")
                .startObject("access")
                .field("url", "myurl1")
                .field("agent", "agent1")
                .endObject()
                .endObject()
                .endObject();
            doc1.setJsonEntity(Strings.toString(bodyDoc1));
            assertOK(oldEsClient.performRequest(doc1));

            Request doc2 = new Request("PUT", "/" + "custom" + "/" + "_doc" + "/" + "2");
            doc2.addParameter("refresh", "true");
            XContentBuilder bodyDoc2 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("apache2")
                .startObject("access")
                .field("url", "myurl2")
                .field("agent", "agent2 agent2")
                .endObject()
                .endObject()
                .field("completion", "some_value")
                .endObject();
            doc2.setJsonEntity(Strings.toString(bodyDoc2));
            assertOK(oldEsClient.performRequest(doc2));

            Request doc3 = new Request("PUT", "/" + "nested" + "/" + "_doc" + "/" + "1");
            doc3.addParameter("refresh", "true");
            XContentBuilder bodyDoc3 = XContentFactory.jsonBuilder()
                .startObject()
                .field("group", "fans")
                .startArray("user")
                .startObject()
                .field("first", "John")
                .field("last", "Smith")
                .endObject()
                .startObject()
                .field("first", "Alice")
                .field("last", "White")
                .endObject()
                .endArray()
                .endObject();
            doc3.setJsonEntity(Strings.toString(bodyDoc3));
            assertOK(oldEsClient.performRequest(doc3));

            Request getSettingsRequest = new Request("GET", "/_cluster/settings?include_defaults=true");
            Map<String, Object> response = entityAsMap(oldEsClient.performRequest(getSettingsRequest));
            assertEquals(repoLocation, ((List<?>) (XContentMapValues.extractValue("defaults.path.repo", response))).get(0));

            // register repo on old ES and take snapshot
            Request createRepoRequest = new Request("PUT", "/_snapshot/" + repoName);
            createRepoRequest.setJsonEntity(Strings.format("""
                {"type":"fs","settings":{"location":"%s"}}
                """, repoLocation));
            assertOK(oldEsClient.performRequest(createRepoRequest));

            Request createSnapshotRequest = new Request("PUT", "/_snapshot/" + repoName + "/" + snapshotName);
            createSnapshotRequest.addParameter("wait_for_completion", "true");
            createSnapshotRequest.setJsonEntity("{\"indices\":\"" + indices.stream().collect(Collectors.joining(",")) + "\"}");
            assertOK(oldEsClient.performRequest(createSnapshotRequest));
            // }

            // register repo on new ES and restore snapshot
            Request createRepoRequest2 = new Request("PUT", "/_snapshot/" + repoName);
            createRepoRequest2.setJsonEntity(Strings.format("""
                {"type":"fs","settings":{"location":"%s"}}
                """, repoLocation));
            assertOK(esClient.performRequest(createRepoRequest2));

            final Request createRestoreRequest = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_restore");
            createRestoreRequest.addParameter("wait_for_completion", "true");
            createRestoreRequest.setJsonEntity("{\"indices\":\"" + indices.stream().collect(Collectors.joining(",")) + "\"}");
            createRestoreRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            assertOK(esClient.performRequest(createRestoreRequest));
        }
    }

    public void testMappingOk() throws IOException {
        // Request mappingRequest = new Request("GET", "/" + "filebeat" + "/_mapping");
        // Map<String, Object> mapping = entityAsMap(client().performRequest(mappingRequest));
        // assertNotNull(XContentMapValues.extractValue(mapping, "filebeat", "mappings", "properties", "apache2"));
    }

}
