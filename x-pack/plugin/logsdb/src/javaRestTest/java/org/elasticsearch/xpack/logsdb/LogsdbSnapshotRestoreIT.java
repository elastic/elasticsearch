/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;

public class LogsdbSnapshotRestoreIT extends ESRestTestCase {

    private static TemporaryFolder repoDirectory = new TemporaryFolder();

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("path.repo", () -> getRepoPath())
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    static final String LOGS_TEMPLATE = """
        {
          "index_patterns": [ "logs-*-*" ],
          "data_stream": {},
          "priority": 1000,
          "template": {
            "settings": {
              "index": {
                "mapping": {
                  "source":{
                    "mode": "{{source_mode}}"
                  }
                }
              }
            },
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "host": {
                  "properties": {
                     "name": {
                        "type": "keyword"
                     }
                  }
                },
                "pid": {
                  "type": "integer"
                },
                "method": {
                  "type": "keyword"
                },
                "message": {
                  "type": "text"
                },
                "ip_address": {
                  "type": "ip"
                },
                "my_object_array": {
                    "type": "{{array_type}}"
                }
              }
            }
          }
        }""";

    static final String DOC_TEMPLATE = """
        {
            "@timestamp": "%s",
            "host": { "name": "%s"},
            "pid": %d,
            "method": "%s",
            "message": "%s",
            "ip_address": "%s",
            "memory_usage_bytes": "%d",
            "my_object_array": [
                {
                    "field_1": "a",
                    "field_2": "b"
                },
                {
                    "field_1": "c",
                    "field_2": "d"
                }
            ]
        }
        """;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testSnapshotRestore() throws Exception {
        snapshotAndRestore("synthetic", "object", false);
    }

    public void testSnapshotRestoreWithSourceOnlyRepository() throws Exception {
        snapshotAndFail("object");
    }

    public void testSnapshotRestoreNested() throws Exception {
        snapshotAndRestore("synthetic", "nested", false);
    }

    public void testSnapshotRestoreNestedWithSourceOnlyRepository() throws Exception {
        snapshotAndFail("nested");
    }

    public void testSnapshotRestoreStoredSource() throws Exception {
        snapshotAndRestore("stored", "object", false);
    }

    public void testSnapshotRestoreStoredSourceWithSourceOnlyRepository() throws Exception {
        snapshotAndRestore("stored", "object", true);
    }

    public void testSnapshotRestoreStoredSourceNested() throws Exception {
        snapshotAndRestore("stored", "nested", false);
    }

    public void testSnapshotRestoreStoredSourceNestedWithSourceOnlyRepository() throws Exception {
        snapshotAndRestore("stored", "nested", true);
    }

    @After
    public void cleanup() throws Exception {
        deleteSnapshot("my-repository", "my-snapshot", true);
        deleteRepository("my-repository");
        deleteDataStream("logs-my-test");
    }

    static void snapshotAndRestore(String sourceMode, String arrayType, boolean sourceOnly) throws IOException {
        String dataStreamName = "logs-my-test";
        String repositoryName = "my-repository";
        if (sourceOnly) {
            var repositorySettings = Settings.builder().put("delegate_type", "fs").put("location", getRepoPath()).build();
            registerRepository(repositoryName, "source", true, repositorySettings);
        } else {
            var repositorySettings = Settings.builder().put("location", getRepoPath()).build();
            registerRepository(repositoryName, FsRepository.TYPE, true, repositorySettings);
        }

        putTemplate("my-template", LOGS_TEMPLATE.replace("{{source_mode}}", sourceMode).replace("{{array_type}}", arrayType));
        String[] docs = new String[100];
        for (int i = 0; i < 100; i++) {
            docs[i] = document(
                Instant.now(),
                String.format(Locale.ROOT, "host-%03d", i),
                randomNonNegativeInt(),
                randomFrom("PUT", "POST", "GET"),
                randomAlphaOfLength(32),
                randomIp(randomBoolean()),
                randomLongBetween(1_000_000L, 2_000_000L)
            );
            indexDocument(dataStreamName, docs[i]);
        }
        refresh(dataStreamName);
        assertDocCount(client(), dataStreamName, 100);
        assertSource(dataStreamName, docs);
        assertDataStream(dataStreamName, sourceMode);

        String snapshotName = "my-snapshot";
        var snapshotResponse = performSnapshot(repositoryName, dataStreamName, snapshotName, true);
        assertOK(snapshotResponse);
        var snapshotResponseBody = entityAsMap(snapshotResponse);
        Map<?, ?> snapshotItem = (Map<?, ?>) snapshotResponseBody.get("snapshot");
        List<?> failures = (List<?>) snapshotItem.get("failures");
        assertThat(failures, empty());
        deleteDataStream(dataStreamName);
        assertDocCount(dataStreamName, 0);

        restoreSnapshot(repositoryName, snapshotName, true);
        assertDataStream(dataStreamName, sourceMode);
        assertDocCount(dataStreamName, 100);
        assertSource(dataStreamName, docs);
    }

    static void snapshotAndFail(String arrayType) throws IOException {
        String dataStreamName = "logs-my-test";
        String repositoryName = "my-repository";
        var repositorySettings = Settings.builder().put("delegate_type", "fs").put("location", getRepoPath()).build();
        registerRepository(repositoryName, "source", true, repositorySettings);

        putTemplate("my-template", LOGS_TEMPLATE.replace("{{source_mode}}", "synthetic").replace("{{array_type}}", arrayType));
        for (int i = 0; i < 100; i++) {
            indexDocument(
                dataStreamName,
                document(
                    Instant.now(),
                    randomAlphaOfLength(10),
                    randomNonNegativeLong(),
                    randomFrom("PUT", "POST", "GET"),
                    randomAlphaOfLength(32),
                    randomIp(randomBoolean()),
                    randomIntBetween(1_000_000, 2_000_000)
                )
            );
        }
        refresh(dataStreamName);
        assertDocCount(client(), dataStreamName, 100);
        assertDataStream(dataStreamName, "synthetic");

        String snapshotName = "my-snapshot";
        var snapshotResponse = performSnapshot(repositoryName, dataStreamName, snapshotName, true);
        assertOK(snapshotResponse);
        var snapshotResponseBody = entityAsMap(snapshotResponse);
        Map<?, ?> snapshotItem = (Map<?, ?>) snapshotResponseBody.get("snapshot");
        List<?> failures = (List<?>) snapshotItem.get("failures");
        assertThat(failures, hasSize(1));
        Map<?, ?> failure = (Map<?, ?>) failures.get(0);
        assertThat(
            (String) failure.get("reason"),
            containsString(
                "Can't snapshot _source only on an index that has incomplete source ie. has _source disabled or filters the source"
            )
        );
    }

    static void deleteDataStream(String dataStreamName) throws IOException {
        assertOK(client().performRequest(new Request("DELETE", "/_data_stream/" + dataStreamName)));
    }

    static void putTemplate(String templateName, String template) throws IOException {
        final Request request = new Request("PUT", "/_index_template/" + templateName);
        request.setJsonEntity(template);
        assertOK(client().performRequest(request));
    }

    static void indexDocument(String indexOrtDataStream, String doc) throws IOException {
        final Request request = new Request("POST", "/" + indexOrtDataStream + "/_doc?refresh=true");
        request.setJsonEntity(doc);
        final Response response = client().performRequest(request);
        assertOK(response);
        assertThat(entityAsMap(response).get("result"), equalTo("created"));
    }

    static String document(
        final Instant timestamp,
        final String hostname,
        long pid,
        final String method,
        final String message,
        final InetAddress ipAddress,
        long memoryUsageBytes
    ) {
        return String.format(
            Locale.ROOT,
            DOC_TEMPLATE,
            DateFormatter.forPattern(FormatNames.DATE_TIME.getName()).format(timestamp),
            hostname,
            pid,
            method,
            message,
            InetAddresses.toAddrString(ipAddress),
            memoryUsageBytes
        );
    }

    static Response performSnapshot(String repository, String dataStreamName, String snapshot, boolean waitForCompletion)
        throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        request.setJsonEntity("""
            {
                "indices": "{{dataStreamName}}"
            }
            """.replace("{{dataStreamName}}", dataStreamName));
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));

        return client().performRequest(request);
    }

    static void assertDataStream(String dataStreamName, final String sourceMode) throws IOException {
        String indexName = getWriteBackingIndex(dataStreamName, 0);
        var flatSettings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(indexName).get(indexName)).get("settings");
        assertThat(flatSettings, hasEntry("index.mode", "logsdb"));
        assertThat(flatSettings, hasEntry("index.mapping.source.mode", sourceMode));
    }

    static String getWriteBackingIndex(String dataStreamName, int backingIndex) throws IOException {
        final Request request = new Request("GET", "_data_stream/" + dataStreamName);
        final List<?> dataStreams = (List<?>) entityAsMap(client().performRequest(request)).get("data_streams");
        final Map<?, ?> dataStream = (Map<?, ?>) dataStreams.get(0);
        final List<?> backingIndices = (List<?>) dataStream.get("indices");
        return (String) ((Map<?, ?>) backingIndices.get(backingIndex)).get("index_name");
    }

    static void assertDocCount(String indexName, long docCount) throws IOException {
        Request countReq = new Request("GET", "/" + indexName + "/_count");
        countReq.addParameter("ignore_unavailable", "true");
        ObjectPath resp = ObjectPath.createFromResponse(client().performRequest(countReq));
        assertEquals(
            "expected " + docCount + " documents but it was a different number",
            docCount,
            Long.parseLong(resp.evaluate("count").toString())
        );
    }

    static void assertSource(String indexName, String[] docs) throws IOException {
        Request searchReq = new Request("GET", "/" + indexName + "/_search");
        searchReq.addParameter("size", String.valueOf(docs.length));
        var response = client().performRequest(searchReq);
        assertOK(response);
        var responseBody = entityAsMap(response);
        List<?> hits = (List<?>) ((Map<?, ?>) responseBody.get("hits")).get("hits");
        assertThat(hits, hasSize(docs.length));
        for (Object hit : hits) {
            Map<?, ?> actualSource = (Map<?, ?>) ((Map<?, ?>) hit).get("_source");
            String actualHost = (String) ((Map<?, ?>) actualSource.get("host")).get("name");
            Map<?, ?> expectedSource = null;
            for (String doc : docs) {
                expectedSource = XContentHelper.convertToMap(XContentType.JSON.xContent(), doc, false);
                String expectedHost = (String) ((Map<?, ?>) expectedSource.get("host")).get("name");
                if (expectedHost.equals(actualHost)) {
                    break;
                }
            }

            assertMap(actualSource, matchesMap(expectedSource));
        }
    }

    @SuppressForbidden(reason = "TemporaryFolder only has io.File methods, not nio.File")
    private static String getRepoPath() {
        return repoDirectory.getRoot().getPath();
    }

}
