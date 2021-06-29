/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.repositories.RepositoryInfo;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.RepositoryStatsSnapshot;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractRepositoriesMeteringAPIRestTestCase extends ESRestTestCase {
    protected abstract String repositoryType();

    protected abstract Map<String, String> repositoryLocation();

    protected abstract Settings repositorySettings();

    /**
     * New settings to force a new repository creation
     */
    protected abstract Settings updatedRepositorySettings();

    protected abstract List<String> readCounterKeys();

    protected abstract List<String> writeCounterKeys();

    @Before
    public void clearArchive() throws Exception {
        clearRepositoriesStats(Long.MAX_VALUE);
    }

    public void testStatsAreTracked() throws Exception {
        snapshotAndRestoreIndex((repository, index) -> {
            List<RepositoryStatsSnapshot> repoStats = getRepositoriesStats();
            assertThat(repoStats.size(), equalTo(1));

            RepositoryStatsSnapshot repositoryStats = repoStats.get(0);
            assertRepositoryStatsBelongToRepository(repositoryStats, repository);
            assertRequestCountersAccountedForReads(repositoryStats);
            assertRequestCountersAccountedForWrites(repositoryStats);
        });
    }

    public void testStatsAreUpdatedAfterRepositoryOperations() throws Exception {
        String snapshot = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        snapshotAndRestoreIndex(snapshot, (repository, index) -> {
            List<RepositoryStatsSnapshot> repoStatsBeforeRestore = getRepositoriesStats();
            assertThat(repoStatsBeforeRestore.size(), equalTo(1));

            RepositoryStatsSnapshot repositoryStatsBeforeRestore = repoStatsBeforeRestore.get(0);
            Map<String, Long> requestCountsBeforeRestore = repositoryStatsBeforeRestore.getRepositoryStats().requestCounts;
            assertRepositoryStatsBelongToRepository(repositoryStatsBeforeRestore, repository);
            assertRequestCountersAccountedForReads(repositoryStatsBeforeRestore);
            assertRequestCountersAccountedForWrites(repositoryStatsBeforeRestore);

            deleteIndex(index);

            restoreSnapshot(repository, snapshot, true);

            List<RepositoryStatsSnapshot> updatedRepoStats = getRepositoriesStats();
            assertThat(updatedRepoStats.size(), equalTo(1));
            RepositoryStatsSnapshot repoStatsAfterRestore = updatedRepoStats.get(0);
            Map<String, Long> requestCountsAfterRestore = repoStatsAfterRestore.getRepositoryStats().requestCounts;

            for (String readCounterKey : readCounterKeys()) {
                assertThat(
                    requestCountsAfterRestore.get(readCounterKey),
                    greaterThanOrEqualTo(requestCountsBeforeRestore.get(readCounterKey))
                );
            }
        });
    }

    public void testClearRepositoriesStats() throws Exception {
        snapshotAndRestoreIndex((repository, index) -> {
            deleteRepository(repository);

            List<RepositoryStatsSnapshot> repositoriesStatsBeforeClearing = getRepositoriesStats();
            assertThat(repositoriesStatsBeforeClearing.size(), equalTo(1));
            RepositoryStatsSnapshot repositoryStatsSnapshot = repositoriesStatsBeforeClearing.get(0);

            assertThat(clearRepositoriesStats(-1).size(), equalTo(0));

            List<RepositoryStatsSnapshot> removedRepositoriesStats = clearRepositoriesStats(repositoryStatsSnapshot.getClusterVersion());

            assertThat(repositoriesStatsBeforeClearing, equalTo(removedRepositoriesStats));

            assertThat(getRepositoriesStats().size(), equalTo(0));
        });
    }

    public void testRegisterMultipleRepositoriesAndGetStats() throws Exception {
        List<String> repositoryNames = List.of("repo-a", "repo-b", "repo-c");
        for (String repositoryName : repositoryNames) {
            registerRepository(repositoryName, repositoryType(), false, repositorySettings());
        }

        List<RepositoryStatsSnapshot> repositoriesStats = getRepositoriesStats();
        Map<String, List<RepositoryStatsSnapshot>> repositoryStatsByName = repositoriesStats.stream()
            .collect(Collectors.groupingBy(r -> r.getRepositoryInfo().name));

        for (String repositoryName : repositoryNames) {
            List<RepositoryStatsSnapshot> repositoryStats = repositoryStatsByName.get(repositoryName);
            assertThat(repositoryStats, is(notNullValue()));
            assertThat(repositoryStats.size(), equalTo(1));

            RepositoryStatsSnapshot stats = repositoryStats.get(0);
            assertRepositoryStatsBelongToRepository(stats, repositoryName);
            assertAllRequestCountsAreZero(stats);
        }
    }

    public void testStatsAreArchivedAfterRepositoryDeletion() throws Exception {
        snapshotAndRestoreIndex((repository, index) -> {
            List<RepositoryStatsSnapshot> repositoriesStats = getRepositoriesStats();
            assertThat(repositoriesStats.size(), equalTo(1));
            RepositoryStatsSnapshot statsBeforeRepoDeletion = repositoriesStats.get(0);
            assertRepositoryStatsBelongToRepository(statsBeforeRepoDeletion, repository);

            deleteRepository(repository);

            List<RepositoryStatsSnapshot> repoStatsAfterDeletion = getRepositoriesStats();
            assertThat(repoStatsAfterDeletion.size(), equalTo(1));
            RepositoryStatsSnapshot statsAfterRepoDeletion = repoStatsAfterDeletion.get(0);
            assertStatsAreEqualsIgnoringStoppedAt(statsBeforeRepoDeletion, statsAfterRepoDeletion);
        });
    }

    public void testStatsAreStoredIntoANewCounterInstanceAfterRepoConfigUpdate() throws Exception {
        final String snapshot = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        snapshotAndRestoreIndex(snapshot, (repository, index) -> {
            List<RepositoryStatsSnapshot> repositoriesStatsBeforeUpdate = getRepositoriesStats();
            assertThat(repositoriesStatsBeforeUpdate.size(), equalTo(1));
            assertRepositoryStatsBelongToRepository(repositoriesStatsBeforeUpdate.get(0), repository);
            assertRequestCountersAccountedForReads(repositoriesStatsBeforeUpdate.get(0));
            assertRequestCountersAccountedForWrites(repositoriesStatsBeforeUpdate.get(0));

            // Update repository
            registerRepository(repository, repositoryType(), false, updatedRepositorySettings());

            List<RepositoryStatsSnapshot> repositoriesStatsAfterUpdate = getRepositoriesStats();

            assertThat(repositoriesStatsAfterUpdate.size(), equalTo(2));
            assertStatsAreEqualsIgnoringStoppedAt(repositoriesStatsBeforeUpdate.get(0), repositoriesStatsAfterUpdate.get(0));

            // The counters for the new repository instance are zero
            assertAllRequestCountsAreZero(repositoriesStatsAfterUpdate.get(1));

            deleteIndex(index);

            restoreSnapshot(repository, snapshot, true);

            List<RepositoryStatsSnapshot> repoStatsAfterRestore = getRepositoriesStats();

            assertThat(repoStatsAfterRestore.size(), equalTo(2));
            assertStatsAreEqualsIgnoringStoppedAt(repositoriesStatsAfterUpdate.get(0), repoStatsAfterRestore.get(0));

            assertRequestCountersAccountedForReads(repoStatsAfterRestore.get(1));
        });
    }

    public void testDeleteThenAddRepositoryWithTheSameName() throws Exception {
        snapshotAndRestoreIndex((repository, index) -> {
            List<RepositoryStatsSnapshot> repoStatsBeforeDeletion = getRepositoriesStats();
            assertThat(repoStatsBeforeDeletion.size(), equalTo(1));

            deleteRepository(repository);

            List<RepositoryStatsSnapshot> repoStatsAfterDeletion = getRepositoriesStats();
            assertThat(repoStatsAfterDeletion.size(), equalTo(1));
            assertStatsAreEqualsIgnoringStoppedAt(repoStatsBeforeDeletion.get(0), repoStatsAfterDeletion.get(0));

            registerRepository(repository, repositoryType(), false, repositorySettings());

            List<RepositoryStatsSnapshot> repositoriesStatsAfterRegisteringTheSameRepo = getRepositoriesStats();
            assertThat(repositoriesStatsAfterRegisteringTheSameRepo.size(), equalTo(2));
            assertStatsAreEqualsIgnoringStoppedAt(repoStatsBeforeDeletion.get(0), repositoriesStatsAfterRegisteringTheSameRepo.get(0));
            assertAllRequestCountsAreZero(repositoriesStatsAfterRegisteringTheSameRepo.get(1));
        });
    }

    private void snapshotAndRestoreIndex(CheckedBiConsumer<String, String, Exception> biConsumer) throws Exception {
        final String snapshot = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        snapshotAndRestoreIndex(snapshot, biConsumer);
    }

    private void snapshotAndRestoreIndex(String snapshot, CheckedBiConsumer<String, String, Exception> biConsumer) throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String repository = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);

        final String repositoryType = repositoryType();
        final Settings repositorySettings = repositorySettings();

        registerRepository(repository, repositoryType, true, repositorySettings);

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        final int numDocs = randomIntBetween(1, 500);
        final StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulkBody.append("{\"index\":{\"_id\":\"").append(i).append("\"}}\n");
            bulkBody.append("{\"field\":").append(i).append(",\"text\":\"Document number ").append(i).append("\"}\n");
        }

        final Request documents = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
        documents.addParameter("refresh", Boolean.TRUE.toString());
        documents.setJsonEntity(bulkBody.toString());
        assertOK(client().performRequest(documents));

        createSnapshot(repository, snapshot, true);

        deleteIndex(indexName);

        restoreSnapshot(repository, snapshot, true);

        biConsumer.accept(repository, indexName);
    }

    private void assertRequestCountersAccountedForReads(RepositoryStatsSnapshot statsSnapshot) {
        RepositoryStats repositoryStats = statsSnapshot.getRepositoryStats();
        Map<String, Long> requestCounts = repositoryStats.requestCounts;
        for (String readCounterKey : readCounterKeys()) {
            assertThat(requestCounts.get(readCounterKey), is(notNullValue()));
            assertThat(requestCounts.get(readCounterKey), is(greaterThan(0L)));
        }
    }

    private void assertRequestCountersAccountedForWrites(RepositoryStatsSnapshot statsSnapshot) {
        RepositoryStats repositoryStats = statsSnapshot.getRepositoryStats();
        Map<String, Long> requestCounts = repositoryStats.requestCounts;
        for (String writeCounterKey : writeCounterKeys()) {
            assertThat(requestCounts.get(writeCounterKey), is(notNullValue()));
            assertThat(requestCounts.get(writeCounterKey), is(greaterThan(0L)));
        }
    }

    private void assertStatsAreEqualsIgnoringStoppedAt(RepositoryStatsSnapshot stats, RepositoryStatsSnapshot otherStats) {
        assertRepositoryInfoIsEqualIgnoringStoppedAt(stats.getRepositoryInfo(), otherStats.getRepositoryInfo());
        assertThat(stats.getRepositoryStats(), equalTo(otherStats.getRepositoryStats()));
    }

    private void assertRepositoryInfoIsEqualIgnoringStoppedAt(RepositoryInfo repositoryInfo, RepositoryInfo otherRepositoryInfo) {
        assertThat(repositoryInfo.ephemeralId, equalTo(otherRepositoryInfo.ephemeralId));
        assertThat(repositoryInfo.name, equalTo(otherRepositoryInfo.name));
        assertThat(repositoryInfo.type, equalTo(otherRepositoryInfo.type));
        assertThat(repositoryInfo.location, equalTo(otherRepositoryInfo.location));
        assertThat(repositoryInfo.startedAt, equalTo(otherRepositoryInfo.startedAt));
    }

    private void assertRepositoryStatsBelongToRepository(RepositoryStatsSnapshot stats, String repositoryName) {
        RepositoryInfo repositoryInfo = stats.getRepositoryInfo();
        assertThat(repositoryInfo.name, equalTo(repositoryName));
        assertThat(repositoryInfo.type, equalTo(repositoryType()));
        assertThat(repositoryInfo.location, equalTo(repositoryLocation()));
    }

    private void assertAllRequestCountsAreZero(RepositoryStatsSnapshot statsSnapshot) {
        RepositoryStats stats = statsSnapshot.getRepositoryStats();
        for (long requestCount : stats.requestCounts.values()) {
            assertThat(requestCount, equalTo(0));
        }
    }

    private List<RepositoryStatsSnapshot> getRepositoriesStats() throws IOException {
        Map<String, Object> response = getAsMap("/_nodes/_all/_repositories_metering");
        return parseRepositoriesStatsResponse(response);
    }

    private List<RepositoryStatsSnapshot> parseRepositoriesStatsResponse(Map<String, Object> response) throws IOException {
        Map<String, List<Map<String, Object>>> nodesRepoStats = extractValue(response, "nodes");
        assertThat(response.size(), greaterThan(0));
        List<RepositoryStatsSnapshot> repositoriesStats = new ArrayList<>();
        for (String nodeId : getNodeIds()) {
            List<Map<String, Object>> nodeStats = nodesRepoStats.get(nodeId);
            assertThat(nodeStats, is(notNullValue()));

            for (Map<String, Object> nodeStatSnapshot : nodeStats) {
                RepositoryInfo repositoryInfo = parseRepositoryInfo(nodeStatSnapshot);
                Map<String, Integer> intRequestCounters = extractValue(nodeStatSnapshot, "request_counts");
                boolean archived = extractValue(nodeStatSnapshot, "archived");
                int clusterVersion = (int) RepositoryStatsSnapshot.UNKNOWN_CLUSTER_VERSION;
                if (archived) {
                    clusterVersion = extractValue(nodeStatSnapshot, "cluster_version");
                }
                Map<String, Long> requestCounters = intRequestCounters.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().longValue()));
                RepositoryStats repositoryStats = new RepositoryStats(requestCounters);
                RepositoryStatsSnapshot statsSnapshot = new RepositoryStatsSnapshot(
                    repositoryInfo,
                    repositoryStats,
                    clusterVersion,
                    archived
                );
                repositoriesStats.add(statsSnapshot);
            }
        }
        return repositoriesStats;
    }

    private RepositoryInfo parseRepositoryInfo(Map<String, Object> nodeStatSnapshot) {
        String id = extractValue(nodeStatSnapshot, "repository_ephemeral_id");
        String name = extractValue(nodeStatSnapshot, "repository_name");
        String type = extractValue(nodeStatSnapshot, "repository_type");
        Map<String, String> location = extractValue(nodeStatSnapshot, "repository_location");
        Long startedAt = extractValue(nodeStatSnapshot, "repository_started_at");
        Long stoppedAt = extractValue(nodeStatSnapshot, "repository_stopped_at");
        return new RepositoryInfo(id, name, type, location, startedAt, stoppedAt);
    }

    private Set<String> getNodeIds() throws IOException {
        Map<String, Object> nodes = extractValue(getAsMap("_nodes/"), "nodes");
        return nodes.keySet();
    }

    private List<RepositoryStatsSnapshot> clearRepositoriesStats(long maxVersionToClear) throws IOException {
        final Request request = new Request(HttpDelete.METHOD_NAME, "/_nodes/_all/_repositories_metering/" + maxVersionToClear);
        final Response response = client().performRequest(request);
        assertThat(
            "Failed to clear repositories stats: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
        return parseRepositoriesStatsResponse(responseAsMap(response));
    }

    @SuppressWarnings("unchecked")
    protected static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }

    protected String getProperty(String propertyName) {
        final String property = System.getProperty(propertyName);
        assertThat(property, not(blankOrNullString()));
        return property;
    }
}
