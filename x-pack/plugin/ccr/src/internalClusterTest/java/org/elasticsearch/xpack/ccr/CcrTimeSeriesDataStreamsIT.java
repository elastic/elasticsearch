/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.elasticsearch.common.time.FormatNames.STRICT_DATE_OPTIONAL_TIME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertCheckedResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CcrTimeSeriesDataStreamsIT extends CcrIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    public void testCrossClusterReplicationForTSDB() throws Exception {
        executeTest(false);
    }

    public void testCrossClusterReplicationForTSDBWithSyntheticId() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        executeTest(true);
    }

    private void executeTest(final boolean useSyntheticId) throws Exception {
        final var dataStreamName = randomIdentifier();
        final int nbPrimaries = randomIntBetween(1, 5);
        final int nbReplicas = between(0, 1);
        putDataStreamTemplate(leaderClient(), dataStreamName, nbPrimaries, nbReplicas, useSyntheticId);

        // Randomize between Auto Follow and Put Follow
        final var useAutoFollow = randomBoolean();
        logger.info("--> using auto-follow is {}", useAutoFollow);
        if (useAutoFollow) {
            putAutoFollowPatterns(
                getTestName().toLowerCase(Locale.ROOT),
                new String[] { dataStreamName },
                List.of(),
                AutoFollowCoordinator.AUTO_FOLLOW_PATTERN_REPLACEMENT
            );
        }

        enum Operation {
            FLUSH,
            REFRESH,
            NONE
        }

        final var docsIndicesById = new HashMap<String, String>();

        var timestamp = Instant.now();
        // Use `timestamp = Instant.ofEpochMilli(epoch)` to set the timestamp back to a specific value when reproducing a test failure
        logger.info("--> timestamp is {} (epoch: {})", timestamp, timestamp.toEpochMilli());

        final int nbDocs = switch (randomInt(2)) {
            case 0 -> randomIntBetween(1, 9);
            case 1 -> randomIntBetween(10, 999);
            case 2 -> randomIntBetween(1_000, 10_000);
            default -> throw new AssertionError("Unexpected value");
        };

        int remainingDocs = nbDocs;
        while (remainingDocs > 0) {
            int bulkDocs = randomIntBetween(1, remainingDocs);
            var bulkResponse = indexRandomDocs(leaderClient(), dataStreamName, timestamp, bulkDocs);

            for (var result : bulkResponse.getItems()) {
                assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
                assertThat(result.getVersion(), equalTo(1L));
                assertThat(result.getResponse().getPrimaryTerm(), equalTo(1L));
                var previous = docsIndicesById.put(result.getId(), result.getIndex());
                assertThat(previous, nullValue());
            }

            timestamp = timestamp.plusMillis(bulkDocs + 1);
            remainingDocs -= bulkDocs;
            assert remainingDocs >= 0;

            // Randomly executes a flush, refresh or nothing between bulk requests
            switch (randomFrom(Operation.values())) {
                case FLUSH:
                    flush(leaderClient(), dataStreamName);
                    break;
                case REFRESH:
                    refresh(leaderClient(), dataStreamName);
                    break;
                case NONE:
                default:
                    break;
            }
        }

        // Delete some random docs
        final List<String> deletedDocs = randomBoolean() ? randomNonEmptySubsetOf(docsIndicesById.keySet()) : new ArrayList<>();
        for (var deletedDocId : deletedDocs) {
            var deletedDocIndex = docsIndicesById.get(deletedDocId);
            assertThat(deletedDocIndex, notNullValue());

            var deleteResponse = leaderClient().prepareDelete(deletedDocIndex, deletedDocId).get();
            assertThat(deleteResponse.getId(), equalTo(deletedDocId));
            assertThat(deleteResponse.getIndex(), equalTo(deletedDocIndex));
            assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));
            assertThat(deleteResponse.getVersion(), equalTo(2L));
            assertThat(deleteResponse.getPrimaryTerm(), equalTo(1L));
        }

        // Randomly executes a flush, refresh or nothing before checking the follower index
        final var operation = randomFrom(Operation.values());
        switch (operation) {
            case FLUSH:
                flush(leaderClient(), dataStreamName);
                break;
            case REFRESH:
                refresh(leaderClient(), dataStreamName);
                break;
            case NONE:
            default:
                break;
        }

        // Backing indices of the datastream
        var backingIndices = docsIndicesById.values().stream().distinct().toList();

        // Install a cluster state listener on the follow cluster to be completed once the datastream exists and backing indices are started
        final var waitForFollowerReady = ClusterServiceUtils.addTemporaryStateListener(
            getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class),
            state -> {
                if (state.projectState(ProjectId.DEFAULT).metadata().indexIsADataStream(dataStreamName) == false) {
                    return false;
                }
                for (var backingIndex : backingIndices) {
                    var backingRoutingTable = state.routingTable(ProjectId.DEFAULT).index(backingIndex);
                    if (backingRoutingTable == null || backingRoutingTable.allShardsActive() == false) {
                        return false;
                    }
                }
                return true;
            }
        );

        if (useAutoFollow == false) {
            for (var backingIndex : backingIndices) {
                var followRequest = putFollow(backingIndex, backingIndex);
                var followResponse = followerClient().execute(PutFollowAction.INSTANCE, followRequest).actionGet();
                assertTrue(followResponse.isFollowIndexCreated());
                assertTrue(followResponse.isFollowIndexShardsAcked());
                assertTrue(followResponse.isIndexFollowingStarted());
            }
        }

        safeAwait(waitForFollowerReady);

        refresh(leaderClient(), dataStreamName);
        var nonDeletedDocs = Sets.difference(docsIndicesById.keySet(), Set.copyOf(deletedDocs));
        assertHitCount(leaderClient().prepareSearch(dataStreamName).setTrackTotalHits(true).setSize(0), nonDeletedDocs.size());

        assertBusy(() -> {
            refresh(followerClient(), dataStreamName);
            assertHitCount(followerClient().prepareSearch(dataStreamName).setTrackTotalHits(true).setSize(0), nonDeletedDocs.size());
        });

        // Verify (a random subset of) documents
        assertBusyDocumentsExist(leaderClient(), randomSubsetOf(nonDeletedDocs), docsIndicesById);
        assertBusyDocumentsExist(followerClient(), randomSubsetOf(nonDeletedDocs), docsIndicesById);

        // Verify (all) deleted documents
        assertBusyDocumentsDeleted(leaderClient(), deletedDocs, docsIndicesById);
        assertBusyDocumentsDeleted(followerClient(), deletedDocs, docsIndicesById);

        // Index more documents
        final int moreDocs = randomIntBetween(1, 1_000);
        var bulkResponse = indexRandomDocs(leaderClient(), dataStreamName, timestamp, moreDocs);
        for (var result : bulkResponse.getItems()) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(result.getVersion(), equalTo(1L));
            assertThat(result.getResponse().getPrimaryTerm(), equalTo(1L));
            var previous = docsIndicesById.put(result.getId(), result.getIndex());
            assertThat(previous, nullValue());
            var added = nonDeletedDocs.add(result.getId());
            assertThat(added, equalTo(true));
        }

        // Delete more documents
        final List<String> moreDeletions = randomBoolean() ? randomSubsetOf(nonDeletedDocs) : List.of();
        for (var deletedDocId : moreDeletions) {
            var deletedDocIndex = docsIndicesById.get(deletedDocId);
            assertThat(deletedDocIndex, notNullValue());

            var deleteResponse = leaderClient().prepareDelete(deletedDocIndex, deletedDocId).get();
            assertThat(deleteResponse.getId(), equalTo(deletedDocId));
            assertThat(deleteResponse.getIndex(), equalTo(deletedDocIndex));
            assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));
            assertThat(deleteResponse.getVersion(), equalTo(2L));
            assertThat(deleteResponse.getPrimaryTerm(), equalTo(1L));
            var removed = nonDeletedDocs.remove(deletedDocId);
            assertThat(removed, equalTo(true));
            var added = deletedDocs.add(deletedDocId);
            assertThat(added, equalTo(true));
        }

        // Verify (a random subset of) documents
        assertBusyDocumentsExist(leaderClient(), randomSubsetOf(nonDeletedDocs), docsIndicesById);
        assertBusyDocumentsExist(followerClient(), randomSubsetOf(nonDeletedDocs), docsIndicesById);

        // Verify (all) deleted documents
        assertBusyDocumentsDeleted(leaderClient(), moreDeletions, docsIndicesById);
        assertBusyDocumentsDeleted(followerClient(), moreDeletions, docsIndicesById);

        refresh(leaderClient(), dataStreamName);
        assertHitCount(leaderClient().prepareSearch(dataStreamName).setTrackTotalHits(true).setSize(0), nonDeletedDocs.size());

        assertBusy(() -> {
            refresh(followerClient(), dataStreamName);
            assertHitCount(followerClient().prepareSearch(dataStreamName).setTrackTotalHits(true).setSize(0), nonDeletedDocs.size());
        });

        Thread.sleep(30_000L);
        leaderClient().execute(ForceMergeAction.INSTANCE, new ForceMergeRequest()).actionGet();
    }

    private static void putDataStreamTemplate(Client client, String dataStreamName, int primaries, int replicas, boolean useSyntheticId)
        throws IOException {
        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request(getTestClass().getName().toLowerCase(Locale.ROOT))
            .indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of(dataStreamName))
                    .template(
                        new Template(
                            indexSettings(primaries, replicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                                .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), randomBoolean())
                                .put(IndexSettings.SYNTHETIC_ID.getKey(), useSyntheticId)
                                .build(),
                            new CompressedXContent("""
                                {
                                    "_doc": {
                                        "properties": {
                                            "@timestamp": {
                                                "type": "date"
                                            },
                                            "hostname": {
                                                "type": "keyword",
                                                "time_series_dimension": true
                                            },
                                            "metric": {
                                                "properties": {
                                                    "field": {
                                                        "type": "keyword",
                                                        "time_series_dimension": true
                                                    },
                                                    "value": {
                                                        "type": "integer",
                                                        "time_series_metric": "counter"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }"""),
                            null
                        )
                    )
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                    .build()
            );
        assertAcked(client.execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());
    }

    private static BulkResponse indexRandomDocs(Client client, String dataStreamName, Instant timestamp, int nbDocs) throws IOException {
        var bulkRequest = client.prepareBulk();
        for (int j = 0; j < nbDocs; j++) {
            var doc = document(timestamp, randomFrom("vm-dev01", "vm-dev02", "vm-dev03", "vm-dev04"), "cpu-load", randomInt());
            bulkRequest.add(client.prepareIndex(dataStreamName).setOpType(DocWriteRequest.OpType.CREATE).setSource(doc));
            timestamp = timestamp.plusMillis(1);
        }
        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);
        return bulkResponse;
    }

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern(STRICT_DATE_OPTIONAL_TIME.getName());

    private static XContentBuilder document(Instant timestamp, String hostName, String metricField, Integer metricValue)
        throws IOException {
        var source = XContentFactory.jsonBuilder();
        source.startObject();
        {
            source.field("@timestamp", DATE_FORMATTER.format(timestamp));
            source.field("hostname", hostName);
            source.startObject("metric");
            {
                source.field("field", metricField);
                source.field("value", metricValue);
            }
            source.endObject();
        }
        source.endObject();
        return source;
    }

    private void assertBusyDocuments(
        Client client,
        List<String> docsIds,
        Map<String, String> docsIndicesById,
        CheckedBiConsumer<GetResponse, String, IOException> assertGet,
        CheckedBiConsumer<SearchResponse, String, IOException> assertSearch
    ) throws Exception {
        record TestDoc(String docIndex, String docId, boolean getOrSearch, boolean realtime, boolean fetch) {}

        final var docsQueue = new ConcurrentLinkedQueue<TestDoc>();
        for (var docId : docsIds) {
            docsQueue.add(new TestDoc(docsIndicesById.get(docId), docId, randomBoolean(), randomBoolean(), randomBoolean()));
        }

        final Thread[] threads = new Thread[5];
        for (int t = 0; t < threads.length; t++) {
            threads[t] = new Thread(() -> {
                while (true) {
                    final TestDoc test = docsQueue.poll();
                    if (test == null) {
                        break;
                    }
                    try {
                        assertBusy(() -> {
                            refresh(client, test.docIndex);
                            if (test.getOrSearch) {
                                var getResponse = client.prepareGet(test.docIndex, test.docId)
                                    .setRealtime(test.realtime)
                                    .setFetchSource(test.fetch)
                                    .execute()
                                    .actionGet();
                                assertGet.accept(getResponse, test.docId);

                            } else {
                                assertCheckedResponse(
                                    client.prepareSearch(test.docIndex)
                                        .setSource(new SearchSourceBuilder().query(new TermQueryBuilder(IdFieldMapper.NAME, test.docId))),
                                    searchResponse -> assertSearch.accept(searchResponse, test.docId)
                                );
                            }
                        });
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            });
            threads[t].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
    }

    private void assertBusyDocumentsExist(Client client, List<String> docsIds, Map<String, String> docsIndicesById) throws Exception {
        assertBusyDocuments(client, docsIds, docsIndicesById, (getResponse, docId) -> {
            assertThat("Not found: " + docId + " " + Uid.encodeId(docId), getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getVersion(), equalTo(1L));
        }, (searchResponse, docId) -> {
            assertHitCount(searchResponse, 1L);
            assertThat(searchResponse.getHits().getHits(), arrayWithSize(1));
            assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo(docId));
        });
    }

    private void assertBusyDocumentsDeleted(Client client, List<String> docsIds, HashMap<String, String> docsIndicesById) throws Exception {
        assertBusyDocuments(client, docsIds, docsIndicesById, (getResponse, deletedDocId) -> {
            assertThat("Found deleted doc: " + deletedDocId + " " + Uid.encodeId(deletedDocId), getResponse.isExists(), equalTo(false));
            assertThat(getResponse.getVersion(), equalTo(-1L));
        }, (searchResponse, deletedDocId) -> assertHitCount(searchResponse, 0L));
    }
}
