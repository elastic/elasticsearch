/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.SuppressForbidden;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;

@SuppressForbidden("temp folder uses file api")
public class FollowIndexIT extends AbstractCCRRestTestCase {

    public static TemporaryFolder leaderRepoDir = new TemporaryFolder();

    public static LocalClusterConfigProvider commonConfig = c -> c.module("x-pack-ccr")
        .module("analysis-common")
        .module("searchable-snapshots")
        .module("data-streams")
        .module("ingest-common")
        .module("mapper-extras")
        .module("x-pack-stack")
        .module("x-pack-ilm")
        .module("x-pack-monitoring")
        .module("constant-keyword")
        .module("wildcard")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("path.repo", () -> leaderRepoDir.getRoot().getAbsolutePath())
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .user("admin", "admin-password", "superuser", false);

    public static ElasticsearchCluster leaderCluster = ElasticsearchCluster.local().name("leader-cluster").apply(commonConfig).build();

    public static ElasticsearchCluster followerCluster = ElasticsearchCluster.local()
        .name("follow-cluster")
        .apply(commonConfig)
        .setting("xpack.monitoring.collection.enabled", "true")
        .setting("cluster.remote.leader_cluster.seeds", () -> "\"" + leaderCluster.getTransportEndpoints() + "\"")
        .build();

    @ClassRule
    public static RuleChain ruleChain = RuleChain.outerRule(leaderRepoDir).around(leaderCluster).around(followerCluster);

    public FollowIndexIT(@Name("targetCluster") TargetCluster targetCluster) {
        super(targetCluster);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return leaderFollower();
    }

    @Override
    protected ElasticsearchCluster getLeaderCluster() {
        return leaderCluster;
    }

    @Override
    protected ElasticsearchCluster getFollowerCluster() {
        return followerCluster;
    }

    public void testFollowIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "test_index1";
        if (targetCluster == TargetCluster.LEADER) {
            logger.info("Running against leader cluster");
            String mapping = "";
            if (randomBoolean()) { // randomly do source filtering on indexing
                mapping = """
                    "_source": {  "includes": ["field"],  "excludes": ["filtered_field"]}""";
            }
            createIndex(adminClient(), leaderIndexName, Settings.EMPTY, mapping, null);
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(client(), leaderIndexName, Integer.toString(i), "field", i, "filtered_field", "true");
            }
            refresh(adminClient(), leaderIndexName);
            verifyDocuments(leaderIndexName, numDocs, "filtered_field:true");
        } else if (targetCluster == TargetCluster.FOLLOWER) {
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
        if (targetCluster == TargetCluster.LEADER) {
            createIndex(adminClient(), "override_leader_index", Settings.EMPTY);
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
            @SuppressWarnings("unchecked")
            final Map<Object, Object> error = (Map<Object, Object>) responseAsMap.get("error");
            assertThat(error, hasEntry("type", "illegal_argument_exception"));
            assertThat(
                error,
                hasEntry("reason", "can not put follower index that could override leader settings {\"index.number_of_shards\":\"5\"}")
            );
        }
    }

    public void testFollowThatOverridesNonExistentSetting() throws IOException {
        if (targetCluster == TargetCluster.LEADER) {
            createIndex(adminClient(), "override_leader_index_non_existent_setting", Settings.EMPTY);
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
            @SuppressWarnings("unchecked")
            final Map<Object, Object> error = (Map<Object, Object>) responseAsMap.get("error");
            assertThat(error, hasEntry("type", "illegal_argument_exception"));
            assertThat(error, hasEntry("reason", "unknown setting [index.non_existent_setting]"));
        }
    }

    public void testFollowNonExistingLeaderIndex() {
        if (targetCluster == TargetCluster.FOLLOWER == false) {
            logger.info("skipping test, waiting for target cluster [follow]");
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
        if (targetCluster == TargetCluster.FOLLOWER == false) {
            return;
        }

        final String dataStreamName = "logs-syslog-prod";
        try (RestClient leaderClient = buildLeaderClient()) {
            Request request = new Request("PUT", "/_data_stream/" + dataStreamName);
            assertOK(leaderClient.performRequest(request));
            verifyDataStream(leaderClient, dataStreamName, 1);
        }

        ResponseException failure = expectThrows(ResponseException.class, () -> followIndex(dataStreamName, dataStreamName));
        assertThat(failure.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(failure.getMessage(), containsString("cannot follow [logs-syslog-prod], because it is a DATA_STREAM"));
    }

    public void testFollowSearchableSnapshotsFails() throws Exception {
        final String testPrefix = "test_follow_searchable_snapshots_fails";

        final String mountedIndex = "mounted-" + testPrefix;
        if (targetCluster == TargetCluster.LEADER) {
            final String repositoryPath = leaderRepoDir.newFolder(testPrefix).getAbsolutePath();

            final String repository = "repository-" + testPrefix;
            registerRepository(repository, FsRepository.TYPE, true, Settings.builder().put("location", repositoryPath).build());

            final String indexName = "index-" + testPrefix;
            createIndex(adminClient(), indexName, Settings.EMPTY);

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
            assertThat(
                e.getMessage(),
                containsString(
                    "is a searchable snapshot index and cannot be used as a leader index for " + "cross-cluster replication purpose"
                )
            );
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    public void testFollowTsdbIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "tsdb_leader";
        long basetime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2021-04-28T18:35:24.467Z");
        if (targetCluster == TargetCluster.LEADER) {
            logger.info("Running against leader cluster");
            createIndex(
                adminClient(),
                leaderIndexName,
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), "time_series")
                    .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                    .build(),
                """
                    "properties": {"@timestamp": {"type": "date"}, "dim": {"type": "keyword", "time_series_dimension": true}}""",
                null
            );
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(client(), leaderIndexName, null, "@timestamp", basetime + TimeUnit.SECONDS.toMillis(i * 10), "dim", "foobar");
            }
            refresh(adminClient(), leaderIndexName);
            verifyDocuments(client(), leaderIndexName, numDocs);
        } else if (targetCluster == TargetCluster.FOLLOWER) {
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
                index(
                    leaderClient,
                    leaderIndexName,
                    null,
                    "@timestamp",
                    basetime + TimeUnit.SECONDS.toMillis(numDocs * 10),
                    "dim",
                    "foobar"
                );
                index(
                    leaderClient,
                    leaderIndexName,
                    null,
                    "@timestamp",
                    basetime + TimeUnit.SECONDS.toMillis(numDocs * 10 + 10),
                    "dim",
                    "foobar"
                );
                index(
                    leaderClient,
                    leaderIndexName,
                    null,
                    "@timestamp",
                    basetime + TimeUnit.SECONDS.toMillis(numDocs * 10 + 20),
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
        if (targetCluster != TargetCluster.FOLLOWER) {
            return;
        }
        logger.info("Running against follow cluster");
        Exception e = expectThrows(
            ResponseException.class,
            () -> followIndex(
                client(),
                "leader_cluster",
                "tsdb_leader",
                "tsdb_follower_bad",
                Settings.builder().put("index.mode", "standard").build()
            )
        );
        assertThat(
            e.getMessage(),
            containsString("can not put follower index that could override leader settings {\\\"index.mode\\\":\\\"standard\\\"}")
        );
    }

    public void testFollowStandardIndexCanNotOverrideMode() throws Exception {
        if (targetCluster != TargetCluster.FOLLOWER) {
            return;
        }
        logger.info("Running against follow cluster");
        Exception e = expectThrows(
            ResponseException.class,
            () -> followIndex(
                client(),
                "leader_cluster",
                "test_index1",
                "tsdb_follower_bad",
                Settings.builder().put("index.mode", "time_series").build()
            )
        );
        assertThat(
            e.getMessage(),
            containsString("can not put follower index that could override leader settings {\\\"index.mode\\\":\\\"time_series\\\"}")
        );
    }

    public void testSyntheticSource() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "synthetic_leader";
        if (targetCluster == TargetCluster.LEADER) {
            logger.info("Running against leader cluster");
            Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC)
                .build();
            createIndex(adminClient(), leaderIndexName, settings, """
                "properties": {"kwd": {"type": "keyword"}}}""", null);
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(client(), leaderIndexName, null, "kwd", "foo", "i", i);
            }
            refresh(adminClient(), leaderIndexName);
            verifyDocuments(client(), leaderIndexName, numDocs);
        } else if (targetCluster == TargetCluster.FOLLOWER) {
            logger.info("Running against follow cluster");
            final String followIndexName = "synthetic_follower";
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
                    assertMap(getIndexSettingsAsMap(followIndexName), matchesMap().extraOk().entry("index.number_of_replicas", "0"));
                } else {
                    assertMap(getIndexSettingsAsMap(followIndexName), matchesMap().extraOk().entry("index.number_of_replicas", "1"));
                }
            });
            // unfollow and then follow and then index a few docs in leader index:
            pauseFollow(followIndexName);
            resumeFollow(followIndexName);
            try (RestClient leaderClient = buildLeaderClient()) {
                index(leaderClient, leaderIndexName, null, "kwd", "foo", "i", -1);
                index(leaderClient, leaderIndexName, null, "kwd", "foo", "i", -2);
                index(leaderClient, leaderIndexName, null, "kwd", "foo", "i", -3);
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

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
