/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;

/**
 * Tests doc-value-based searches against archive indices, imported from clusters older than N-2.
 * We reuse the YAML tests in search/390_doc_values_search.yml but have to do the setup
 * manually here as the setup is done on the old cluster for which we have to use the
 * low-level REST client instead of the YAML set up that only knows how to talk to
 * newer ES versions.
 *
 * We mimic the setup in search/390_doc_values_search.yml here, but adapt it to work
 * against older version clusters.
 */
public class DocValueOnlyFieldsIT extends ESClientYamlSuiteTestCase {

    final Version oldVersion = Version.fromString(System.getProperty("tests.es.version"));
    static boolean setupDone;

    public DocValueOnlyFieldsIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected boolean skipSetupSections() {
        // setup in the YAML file is replaced by the method below
        return true;
    }

    @Before
    public void setupIndex() throws IOException {
        final boolean afterRestart = Booleans.parseBoolean(System.getProperty("tests.after_restart"));
        if (afterRestart) {
            return;
        }

        // The following is bit of a hack. While we wish we could make this an @BeforeClass, it does not work because the client() is only
        // initialized later, so we do it when running the first test
        if (setupDone) {
            return;
        }

        setupDone = true;

        String repoLocation = PathUtils.get(System.getProperty("tests.repo.location"))
            .resolve(RandomizedTest.getContext().getTargetClass().getName())
            .toString();

        String indexName = "test";
        String repoName = "doc_values_repo";
        String snapshotName = "snap";
        String[] basicTypes = new String[] {
            "byte",
            "double",
            "float",
            "half_float",
            "integer",
            "long",
            "short",
            "boolean",
            "keyword",
            "ip",
            "geo_point" }; // date is manually added as it need further configuration

        int oldEsPort = Integer.parseInt(System.getProperty("tests.es.port"));
        try (RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()) {
            Request createIndex = new Request("PUT", "/" + indexName);
            int numberOfShards = randomIntBetween(1, 3);

            boolean multiTypes = oldVersion.before(Version.V_7_0_0);

            XContentBuilder settingsBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("settings")
                .field("index.number_of_shards", numberOfShards)
                .endObject()
                .startObject("mappings");
            if (multiTypes) {
                settingsBuilder.startObject("doc");
            }
            settingsBuilder.field("dynamic", false).startObject("properties");
            for (String type : basicTypes) {
                settingsBuilder.startObject(type).field("type", type).endObject();
            }
            settingsBuilder.startObject("date").field("type", "date").field("format", "yyyy/MM/dd").endObject();
            if (multiTypes) {
                settingsBuilder.endObject();
            }
            settingsBuilder.endObject().endObject().endObject();

            createIndex.setJsonEntity(Strings.toString(settingsBuilder));
            assertOK(oldEs.performRequest(createIndex));

            Request doc1 = new Request("PUT", "/" + indexName + "/" + "doc" + "/" + "1");
            doc1.addParameter("refresh", "true");
            XContentBuilder bodyDoc1 = XContentFactory.jsonBuilder()
                .startObject()
                .field("byte", 1)
                .field("double", 1.0)
                .field("float", 1.0)
                .field("half_float", 1.0)
                .field("integer", 1)
                .field("long", 1)
                .field("short", 1)
                .field("date", "2017/01/01")
                .field("keyword", "key1")
                .field("boolean", false)
                .field("ip", "192.168.0.1")
                .array("geo_point", 13.5, 34.89)
                .endObject();
            doc1.setJsonEntity(Strings.toString(bodyDoc1));
            assertOK(oldEs.performRequest(doc1));

            Request doc2 = new Request("PUT", "/" + indexName + "/" + "doc" + "/" + "2");
            doc2.addParameter("refresh", "true");
            XContentBuilder bodyDoc2 = XContentFactory.jsonBuilder()
                .startObject()
                .field("byte", 2)
                .field("double", 2.0)
                .field("float", 2.0)
                .field("half_float", 2.0)
                .field("integer", 2)
                .field("long", 2)
                .field("short", 2)
                .field("date", "2017/01/02")
                .field("keyword", "key2")
                .field("boolean", true)
                .field("ip", "192.168.0.2")
                .array("geo_point", -63.24, 31.0)
                .endObject();
            doc2.setJsonEntity(Strings.toString(bodyDoc2));
            assertOK(oldEs.performRequest(doc2));

            // register repo on old ES and take snapshot
            Request createRepoRequest = new Request("PUT", "/_snapshot/" + repoName);
            createRepoRequest.setJsonEntity(Strings.format("""
                {"type":"fs","settings":{"location":"%s"}}
                """, repoLocation));
            assertOK(oldEs.performRequest(createRepoRequest));

            Request createSnapshotRequest = new Request("PUT", "/_snapshot/" + repoName + "/" + snapshotName);
            createSnapshotRequest.addParameter("wait_for_completion", "true");
            createSnapshotRequest.setJsonEntity("{\"indices\":\"" + indexName + "\"}");
            assertOK(oldEs.performRequest(createSnapshotRequest));
        }

        // register repo on new ES and restore snapshot
        Request createRepoRequest2 = new Request("PUT", "/_snapshot/" + repoName);
        createRepoRequest2.setJsonEntity(Strings.format("""
            {"type":"fs","settings":{"location":"%s"}}
            """, repoLocation));
        assertOK(client().performRequest(createRepoRequest2));

        final Request createRestoreRequest = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_restore");
        createRestoreRequest.addParameter("wait_for_completion", "true");
        createRestoreRequest.setJsonEntity("{\"indices\":\"" + indexName + "\"}");
        assertOK(client().performRequest(createRestoreRequest));
    }
}
