/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.github.nik9000.mapmatcher.MapMatcher.assertMap;
import static io.github.nik9000.mapmatcher.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class FollowIndexIT extends ESCCRRestTestCase {

    public void testFollowIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "test_index1";
        if ("leader".equals(targetCluster)) {
            logger.info("Running against leader cluster");
            String mapping = "";
            if (randomBoolean()) { // randomly do source filtering on indexing
                mapping =
                    "\"_source\": {" +
                    "  \"includes\": [\"field\"]," +
                    "  \"excludes\": [\"filtered_field\"]" +
                    "}";
            }
            createIndex(leaderIndexName, Settings.EMPTY, mapping);
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(client(), leaderIndexName, Integer.toString(i), "field", i, "filtered_field", "true");
            }
            refresh(leaderIndexName);
            verifyDocuments(leaderIndexName, numDocs, "filtered_field:true");
        } else if ("follow".equals(targetCluster)) {
            logger.info("Running against follow cluster");
            final String followIndexName = "test_index2";
            final boolean overrideNumberOfReplicas = randomBoolean();
            if (overrideNumberOfReplicas) {
                followIndex(
                    client(),
                    "leader_cluster",
                    leaderIndexName,
                    followIndexName,
                    Settings.builder().put("index.number_of_replicas", 0).build()
                );
            } else {
                followIndex(leaderIndexName, followIndexName);
            }
            assertBusy(() -> {
                verifyDocuments(followIndexName, numDocs, "filtered_field:true");
                if (overrideNumberOfReplicas) {
                    assertThat(getIndexSettingsAsMap("test_index2"), hasEntry("index.number_of_replicas", "0"));
                } else {
                    assertThat(getIndexSettingsAsMap("test_index2"), hasEntry("index.number_of_replicas", "1"));
                }
            });
            // unfollow and then follow and then index a few docs in leader index:
            pauseFollow(followIndexName);
            resumeFollow(followIndexName);
            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
            }
            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 3, "filtered_field:true"));
            assertBusy(() -> verifyCcrMonitoring(leaderIndexName, followIndexName), 30, TimeUnit.SECONDS);

            pauseFollow(followIndexName);
            closeIndex(followIndexName);
            assertOK(client().performRequest(new Request("POST", "/" + followIndexName + "/_ccr/unfollow")));
            Exception e = expectThrows(ResponseException.class, () -> resumeFollow(followIndexName));
            assertThat(e.getMessage(), containsString("follow index [" + followIndexName + "] does not have ccr metadata"));
        }
    }

    public void testFollowThatOverridesRequiredLeaderSetting() throws IOException {
        if ("leader".equals(targetCluster)) {
            createIndex("override_leader_index", Settings.EMPTY);
        } else {
            final Settings settings = Settings.builder().put("index.number_of_shards", 5).build();
            final ResponseException responseException = expectThrows(
                ResponseException.class,
                () -> followIndex(client(), "leader_cluster", "override_leader_index", "override_follow_index", settings)
            );
            final Response response = responseException.getResponse();
            assertThat(response.getStatusLine().getStatusCode(), equalTo(400));
            final Map<String, Object> responseAsMap = entityAsMap(response);
            assertThat(responseAsMap, hasKey("error"));
            assertThat(responseAsMap.get("error"), instanceOf(Map.class));
            @SuppressWarnings("unchecked") final Map<Object, Object> error = (Map<Object, Object>) responseAsMap.get("error");
            assertThat(error, hasEntry("type", "illegal_argument_exception"));
            assertThat(
                error,
                hasEntry("reason", "can not put follower index that could override leader settings {\"index.number_of_shards\":\"5\"}")
            );
        }
    }

    public void testFollowThatOverridesNonExistentSetting() throws IOException {
        if ("leader".equals(targetCluster)) {
            createIndex("override_leader_index_non_existent_setting", Settings.EMPTY);
        } else {
            final Settings settings = Settings.builder().put("index.non_existent_setting", randomAlphaOfLength(3)).build();
            final ResponseException responseException = expectThrows(
                ResponseException.class,
                () -> followIndex(
                    client(),
                    "leader_cluster",
                    "override_leader_index_non_existent_setting",
                    "override_follow_index_non_existent_setting",
                    settings
                )
            );
            final Response response = responseException.getResponse();
            assertThat(response.getStatusLine().getStatusCode(), equalTo(400));
            final Map<String, Object> responseAsMap = entityAsMap(response);
            assertThat(responseAsMap, hasKey("error"));
            assertThat(responseAsMap.get("error"), instanceOf(Map.class));
            @SuppressWarnings("unchecked") final Map<Object, Object> error = (Map<Object, Object>) responseAsMap.get("error");
            assertThat(error, hasEntry("type", "illegal_argument_exception"));
            assertThat(
                error,
                hasEntry("reason", "unknown setting [index.non_existent_setting]")
            );
        }
    }

    public void testFollowNonExistingLeaderIndex() {
        if ("follow".equals(targetCluster) == false) {
            logger.info("skipping test, waiting for target cluster [follow]" );
            return;
        }
        ResponseException e = expectThrows(ResponseException.class, () -> resumeFollow("non-existing-index"));
        assertThat(e.getMessage(), containsString("no such index [non-existing-index]"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        e = expectThrows(ResponseException.class, () -> followIndex("non-existing-index", "non-existing-index"));
        assertThat(e.getMessage(), containsString("no such index [non-existing-index]"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    public void testFollowDataStreamFails() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final String dataStreamName = "logs-syslog-prod";
        try (RestClient leaderClient = buildLeaderClient()) {
            Request request = new Request("PUT", "/_data_stream/" + dataStreamName);
            assertOK(leaderClient.performRequest(request));
            verifyDataStream(leaderClient, dataStreamName, DataStream.getDefaultBackingIndexName("logs-syslog-prod", 1));
        }

        ResponseException failure = expectThrows(ResponseException.class, () -> followIndex(dataStreamName, dataStreamName));
        assertThat(failure.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(failure.getMessage(), containsString("cannot follow [logs-syslog-prod], because it is a DATA_STREAM"));
    }

    public void testChangeBackingIndexNameFails() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final String dataStreamName = "logs-foobar-prod";
        try (RestClient leaderClient = buildLeaderClient()) {
            Request request = new Request("PUT", "/_data_stream/" + dataStreamName);
            assertOK(leaderClient.performRequest(request));
            verifyDataStream(leaderClient, dataStreamName, DataStream.getDefaultBackingIndexName("logs-foobar-prod", 1));
        }

        ResponseException failure = expectThrows(ResponseException.class,
            () -> followIndex(DataStream.getDefaultBackingIndexName("logs-foobar-prod", 1), ".ds-logs-barbaz-prod-000001"));
        assertThat(failure.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(failure.getMessage(), containsString("a backing index name in the local and remote cluster must remain the same"));
    }

    public void testFollowSearchableSnapshotsFails() throws Exception {
        final String testPrefix = getTestName().toLowerCase(Locale.ROOT);

        final String mountedIndex = "mounted-" + testPrefix;
        if ("leader".equals(targetCluster)) {
            final String systemPropertyRepoPath = System.getProperty("tests.leader_cluster_repository_path");
            assertThat("Missing system property [tests.leader_cluster_repository_path]", systemPropertyRepoPath, not(emptyOrNullString()));
            final String repositoryPath = systemPropertyRepoPath + '/' + testPrefix;

            final String repository = "repository-" + testPrefix;
            registerRepository(repository, FsRepository.TYPE, true, Settings.builder().put("location", repositoryPath).build());

            final String indexName = "index-" + testPrefix;
            createIndex(indexName, Settings.EMPTY);

            final String snapshot = "snapshot-" + testPrefix;
            deleteSnapshot(repository, snapshot, true);
            createSnapshot(repository, snapshot, true);
            deleteIndex(indexName);

            final Request mountRequest = new Request(HttpPost.METHOD_NAME, "/_snapshot/" + repository + '/' + snapshot + "/_mount");
            mountRequest.setJsonEntity("{\"index\": \"" + indexName + "\",\"renamed_index\": \"" + mountedIndex + "\"}");
            final Response mountResponse = client().performRequest(mountRequest);
            assertThat(
                "Failed to mount snapshot [" + snapshot + "] from repository [" + repository + "]: " + mountResponse,
                mountResponse.getStatusLine().getStatusCode(),
                equalTo(RestStatus.OK.getStatus())
            );
            ensureGreen(mountedIndex);

        } else {
            final ResponseException e = expectThrows(ResponseException.class, () -> followIndex(mountedIndex, mountedIndex + "-follower"));
            assertThat(e.getMessage(), containsString("is a searchable snapshot index and cannot be used as a leader index for " +
                "cross-cluster replication purpose"));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    public void testFollowTsdbIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "tsdb_leader";
        long basetime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2021-01-01T00:00:00Z");
        if ("leader".equals(targetCluster)) {
            logger.info("Running against leader cluster");
            createIndex(
                leaderIndexName,
                Settings.builder().put(IndexSettings.MODE.getKey(), "time_series").build(),
                "\"properties\": {\"@timestamp\": {\"type\": \"date\"}, \"dim\": {\"type\": \"keyword\", \"dimension\": true}}"
            );
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(
                    client(),
                    leaderIndexName,
                    Integer.toString(i),
                    "@timestamp",
                    basetime + TimeUnit.SECONDS.toMillis(i * 10),
                    "dim",
                    "foobar"
                );
            }
            refresh(leaderIndexName);
            verifyDocuments(client(), leaderIndexName, numDocs);
        } else if ("follow".equals(targetCluster)) {
            logger.info("Running against follow cluster");
            final String followIndexName = "tsdb_follower";
            final boolean overrideNumberOfReplicas = randomBoolean();
            if (overrideNumberOfReplicas) {
                followIndex(
                    client(),
                    "leader_cluster",
                    leaderIndexName,
                    followIndexName,
                    Settings.builder().put("index.number_of_replicas", 0).build()
                );
            } else {
                followIndex(leaderIndexName, followIndexName);
            }
            assertBusy(() -> {
                verifyDocuments(client(), followIndexName, numDocs);
                if (overrideNumberOfReplicas) {
                    assertMap(
                        getIndexSettingsAsMap(followIndexName),
                        matchesMap().extraOk().entry("index.mode", "time_series").entry("index.number_of_replicas", "0")
                    );
                } else {
                    assertMap(
                        getIndexSettingsAsMap(followIndexName),
                        matchesMap().extraOk().entry("index.mode", "time_series").entry("index.number_of_replicas", "1")
                    );
                }
            });
            // unfollow and then follow and then index a few docs in leader index:
            pauseFollow(followIndexName);
            resumeFollow(followIndexName);
            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs;
                index(
                    leaderClient,
                    leaderIndexName,
                    Integer.toString(id),
                    "@timestamp",
                    basetime + TimeUnit.SECONDS.toMillis(id * 10),
                    "dim",
                    "foobar"
                );
                index(
                    leaderClient,
                    leaderIndexName,
                    Integer.toString(id + 1),
                    "@timestamp",
                    basetime + TimeUnit.SECONDS.toMillis(id * 10 + 10),
                    "dim",
                    "foobar"
                );
                index(
                    leaderClient,
                    leaderIndexName,
                    Integer.toString(id + 2),
                    "@timestamp",
                    basetime + TimeUnit.SECONDS.toMillis(id * 10 + 20),
                    "dim",
                    "foobar"
                );
            }
            assertBusy(() -> verifyDocuments(client(), followIndexName, numDocs + 3));
            assertBusy(() -> verifyCcrMonitoring(leaderIndexName, followIndexName), 30, TimeUnit.SECONDS);

            pauseFollow(followIndexName);
            closeIndex(followIndexName);
            assertOK(client().performRequest(new Request("POST", "/" + followIndexName + "/_ccr/unfollow")));
            Exception e = expectThrows(ResponseException.class, () -> resumeFollow(followIndexName));
            assertThat(e.getMessage(), containsString("follow index [" + followIndexName + "] does not have ccr metadata"));
        }
    }

    public void testFollowTsdbIndexCanNotOverrideMode() throws Exception {
        if (false == "follow".equals(targetCluster)) {
            return;
        }
        logger.info("Running against follow cluster");
        Exception e = expectThrows(ResponseException.class, () -> followIndex(
            client(),
            "leader_cluster",
            "tsdb_leader",
            "tsdb_follower_bad",
            Settings.builder().put("index.mode", "standard").build()
        ));
        assertThat(
            e.getMessage(),
            containsString("can not put follower index that could override leader settings {\\\"index.mode\\\":\\\"standard\\\"}")
        );
    }

    public void testFollowStandardIndexCanNotOverrideMode() throws Exception {
        if (false == "follow".equals(targetCluster)) {
            return;
        }
        logger.info("Running against follow cluster");
        Exception e = expectThrows(ResponseException.class, () -> followIndex(
            client(),
            "leader_cluster",
            "test_index1",
            "tsdb_follower_bad",
            Settings.builder().put("index.mode", "time_series").build()
        ));
        assertThat(
            e.getMessage(),
            containsString("can not put follower index that could override leader settings {\\\"index.mode\\\":\\\"time_series\\\"}")
        );
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }
}
