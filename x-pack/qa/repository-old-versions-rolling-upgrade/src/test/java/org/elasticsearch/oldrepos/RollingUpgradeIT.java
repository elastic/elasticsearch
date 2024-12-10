/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import static org.hamcrest.Matchers.lessThan;

public class RollingUpgradeIT extends ESRestTestCase {

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testRollingUpgrade() throws IOException {
        String repoLocation = System.getProperty("tests.repo.location.first.cluster");

        String indexName = "test_index";
        int portFirstCluster = Integer.parseInt(System.getProperty("tests.es.port.first.cluster"));
        try (RestClient client = RestClient.builder(new HttpHost("127.0.0.1", portFirstCluster)).build()) {
            Version version5 = Version.fromString(System.getProperty("tests.es.version5"));
            getVersion(client);
            createIndex(version5, indexName, client);
            addDocuments(version5, indexName,  client);
            registerRepository(indexName, repoLocation, client);
            createSnapshot(indexName, repoLocation, client);
        }

        int portSecondCluster = Integer.parseInt(System.getProperty("tests.es.port.second.cluster"));
        try (RestClient client = RestClient.builder(new HttpHost("127.0.0.1", portSecondCluster)).build()) {
            getVersion(client);
            registerRepository(indexName, repoLocation, client);
            restoreSnapshot(indexName, client);
        }
        int x = 10;
    }

    private void createIndex(Version version, String indexName, RestClient client) throws IOException {
        Request createIndex = new Request("PUT", "/" + indexName);
        int numberOfShards = randomIntBetween(1, 3);

        XContentBuilder settingsBuilder = XContentFactory.jsonBuilder().startObject().startObject("settings");
        settingsBuilder.field("index.number_of_shards", numberOfShards);

        // 6.5.0 started using soft-deletes, but it was only enabled by default on 7.0
        if (version.onOrAfter(Version.fromString("6.5.0")) && version.before(Version.fromString("7.0.0")) && randomBoolean()) {
            settingsBuilder.field("index.soft_deletes.enabled", true);
        }

        settingsBuilder.endObject().endObject();
        createIndex.setJsonEntity(Strings.toString(settingsBuilder));
        assertOK(client.performRequest(createIndex));
    }

    private void addDocuments(Version version, String indexName, RestClient client) throws IOException {
        int numDocs = 10;
        int extraDocs = 1;
        final Set<String> expectedIds = new HashSet<>();
        for (int i = 0; i < numDocs + extraDocs; i++) {
            String id = "testdoc" + i;
            expectedIds.add(id);
            // use multiple types for ES versions < 6.0.0
            String type = getType(version, id);
            Request doc = new Request("PUT", "/" + indexName + "/" + type + "/" + id);
            doc.addParameter("refresh", "true");
            doc.setJsonEntity(sourceForDoc(i));
            assertOK(client.performRequest(doc));
        }

        for (int i = 0; i < extraDocs; i++) {
            String id = randomFrom(expectedIds);
            expectedIds.remove(id);
            String type = getType(version, id);
            Request doc = new Request("DELETE", "/" + indexName + "/" + type + "/" + id);
            doc.addParameter("refresh", "true");
            client.performRequest(doc);
        }
    }

    private void registerRepository(String indexName, String repoLocation, RestClient client) throws IOException {
        String repoName = "repo_" + indexName;
        Request request = new Request("PUT", "/_snapshot/" + repoName);
        request.setJsonEntity(Strings.format("""
            {"type":"fs","settings":{"location":"%s"}}
            """, repoLocation));
        assertOK(client.performRequest(request));
    }

    private void createSnapshot(String indexName, String repoLocation, RestClient client)throws IOException  {
        String repoName = "repo_" + indexName;
        String snapshotName = "snap_" + indexName;

        Request request = new Request("PUT", "/_snapshot/" + repoName + "/" + snapshotName);
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity("{\"indices\":\"" + indexName + "\"}");
        assertOK(client.performRequest(request));
    }

    private void restoreSnapshot(String indexName, RestClient client) throws IOException {
        String repoName = "repo_" + indexName;
        String snapshotName = "snap_" + indexName;

        final Request request = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_restore");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity("{\"indices\":\"" + indexName + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(client.performRequest(request));
    }

    private void getVersion(RestClient client) throws IOException {
        final Request request = new Request("GET", "/");
        Response response = client.performRequest(request);
        String fd = EntityUtils.toString(response.getEntity());
        int x = 10;
    }

    private String getType(Version oldVersion, String id) {
        return "doc" + (oldVersion.before(Version.fromString("6.0.0")) ? Math.abs(Murmur3HashFunction.hash(id) % 2) : 0);
    }

    private static String sourceForDoc(int i) {
        return "{\"test\":\"test" + i + "\",\"val\":" + i + ",\"create_date\":\"2020-01-" + Strings.format("%02d", i + 1) + "\"}";
    }
}
