/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardsAction;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchableSnapshotsCanMatchOnCoordinatorIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    private static final String TIMESTAMP_TEMPLATE_WITHIN_RANGE = "2020-11-28T%02d:%02d:%02d.%09dZ";
    private static final String TIMESTAMP_TEMPLATE_OUTSIDE_RANGE = "2020-11-26T%02d:%02d:%02d.%09dZ";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings initialSettings = super.nodeSettings(nodeOrdinal, otherSettings);
        if (DiscoveryNode.canContainData(otherSettings)) {
            return Settings.builder()
                .put(initialSettings)
                // Have a shared cache of reasonable size available on each node because tests randomize over frozen and cold allocation
                .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(randomLongBetween(1, 10)))
                .build();
        } else {
            return initialSettings;
        }
    }

    /**
     * Can match against searchable snapshots is tested via both the Search API and the SearchShards (transport-only) API.
     * The latter is a way to do only a can-match rather than all search phases.
     */
    public void testSearchableSnapshotShardsAreSkippedBySearchRequestWithoutQueryingAnyNodeWhenTheyAreOutsideOfTheQueryRange()
        throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        final String dataNodeHoldingRegularIndex = internalCluster().startDataOnlyNode();
        final String dataNodeHoldingSearchableSnapshot = internalCluster().startDataOnlyNode();
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, dataNodeHoldingSearchableSnapshot);

        final String indexOutsideSearchRange = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int indexOutsideSearchRangeShardCount = randomIntBetween(1, 3);
        createIndexWithTimestamp(indexOutsideSearchRange, indexOutsideSearchRangeShardCount, Settings.EMPTY);

        final String indexWithinSearchRange = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int indexWithinSearchRangeShardCount = randomIntBetween(1, 3);
        createIndexWithTimestamp(
            indexWithinSearchRange,
            indexWithinSearchRangeShardCount,
            Settings.builder()
                .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNodeHoldingRegularIndex)
                .build()
        );

        final int totalShards = indexOutsideSearchRangeShardCount + indexWithinSearchRangeShardCount;

        // Either add data outside of the range, or documents that don't have timestamp data
        final boolean indexDataWithTimestamp = randomBoolean();
        // Add enough documents to have non-metadata segment files in all shards,
        // otherwise the mount operation might go through as the read won't be
        // blocked
        final int numberOfDocsInIndexOutsideSearchRange = between(350, 1000);
        if (indexDataWithTimestamp) {
            indexDocumentsWithTimestampWithinDate(
                indexOutsideSearchRange,
                numberOfDocsInIndexOutsideSearchRange,
                TIMESTAMP_TEMPLATE_OUTSIDE_RANGE
            );
        } else {
            indexRandomDocs(indexOutsideSearchRange, numberOfDocsInIndexOutsideSearchRange);
        }

        // Index enough documents to ensure that all shards have at least some documents
        int numDocsWithinRange = between(100, 1000);
        indexDocumentsWithTimestampWithinDate(indexWithinSearchRange, numDocsWithinRange, TIMESTAMP_TEMPLATE_WITHIN_RANGE);

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "mock");

        final SnapshotId snapshotId = createSnapshot(repositoryName, "snapshot-1", List.of(indexOutsideSearchRange)).snapshotId();
        assertAcked(indicesAdmin().prepareDelete(indexOutsideSearchRange));

        final String searchableSnapshotIndexOutsideSearchRange = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        // Block the repository for the node holding the searchable snapshot shards
        // to delay its restore
        blockDataNode(repositoryName, dataNodeHoldingSearchableSnapshot);

        // Force the searchable snapshot to be allocated in a particular node
        Settings restoredIndexSettings = Settings.builder()
            .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNodeHoldingSearchableSnapshot)
            .build();

        final MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
            searchableSnapshotIndexOutsideSearchRange,
            repositoryName,
            snapshotId.getName(),
            indexOutsideSearchRange,
            restoredIndexSettings,
            Strings.EMPTY_ARRAY,
            false,
            randomFrom(MountSearchableSnapshotRequest.Storage.values())
        );
        client().execute(MountSearchableSnapshotAction.INSTANCE, mountRequest).actionGet();

        final IndexMetadata indexMetadata = getIndexMetadata(searchableSnapshotIndexOutsideSearchRange);
        assertThat(indexMetadata.getTimestampRange(), equalTo(IndexLongFieldRange.NO_SHARDS));

        DateFieldMapper.DateFieldType timestampFieldType = indicesService.getTimestampFieldType(indexMetadata.getIndex());
        assertThat(timestampFieldType, nullValue());

        final boolean includeIndexCoveringSearchRangeInSearchRequest = randomBoolean();
        List<String> indicesToSearch = new ArrayList<>();
        if (includeIndexCoveringSearchRangeInSearchRequest) {
            indicesToSearch.add(indexWithinSearchRange);
        }
        indicesToSearch.add(searchableSnapshotIndexOutsideSearchRange);
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(DataStream.TIMESTAMP_FIELD_NAME)
            .from("2020-11-28T00:00:00.000000000Z", true)
            .to("2020-11-29T00:00:00.000000000Z");

        SearchRequest request = new SearchRequest().indices(indicesToSearch.toArray(new String[0]))
            .source(new SearchSourceBuilder().query(rangeQuery));

        if (includeIndexCoveringSearchRangeInSearchRequest) {
            SearchResponse searchResponse = client().search(request).actionGet();

            // All the regular index searches succeeded
            assertThat(searchResponse.getSuccessfulShards(), equalTo(indexWithinSearchRangeShardCount));
            // All the searchable snapshots shard search failed
            assertThat(searchResponse.getFailedShards(), equalTo(indexOutsideSearchRangeShardCount));
            assertThat(searchResponse.getSkippedShards(), equalTo(0));
            assertThat(searchResponse.getTotalShards(), equalTo(totalShards));
        } else {
            // All shards failed, since all shards are unassigned and the IndexMetadata min/max timestamp
            // is not available yet
            expectThrows(SearchPhaseExecutionException.class, () -> client().search(request).actionGet());
        }

        // test with SearchShardsAPI
        {
            boolean allowPartialSearchResults = includeIndexCoveringSearchRangeInSearchRequest;
            SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                indicesToSearch.toArray(new String[0]),
                SearchRequest.DEFAULT_INDICES_OPTIONS,
                rangeQuery,
                null,
                null,
                allowPartialSearchResults,
                null
            );

            if (includeIndexCoveringSearchRangeInSearchRequest) {
                SearchShardsResponse searchShardsResponse = client().execute(SearchShardsAction.INSTANCE, searchShardsRequest).actionGet();
                assertThat(searchShardsResponse.getGroups().size(), equalTo(totalShards));
                List<List<SearchShardsGroup>> partitionedBySkipped = searchShardsResponse.getGroups()
                    .stream()
                    .collect(
                        Collectors.teeing(
                            Collectors.filtering(g -> g.skipped(), Collectors.toList()),
                            Collectors.filtering(g -> g.skipped() == false, Collectors.toList()),
                            List::of
                        )
                    );
                List<SearchShardsGroup> skipped = partitionedBySkipped.get(0);
                List<SearchShardsGroup> notSkipped = partitionedBySkipped.get(1);
                assertThat(skipped.size(), equalTo(0));
                assertThat(notSkipped.size(), equalTo(totalShards));
            } else {
                SearchShardsResponse searchShardsResponse = null;
                try {
                    searchShardsResponse = client().execute(SearchShardsAction.INSTANCE, searchShardsRequest).actionGet();
                } catch (SearchPhaseExecutionException e) {
                    // ignore as this is expected to happen
                }

                if (searchShardsResponse != null) {
                    for (SearchShardsGroup group : searchShardsResponse.getGroups()) {
                        assertFalse("no shard should be marked as skipped", group.skipped());
                    }
                }
            }
        }

        // Allow the searchable snapshots to be finally mounted
        unblockNode(repositoryName, dataNodeHoldingSearchableSnapshot);
        waitUntilRecoveryIsDone(searchableSnapshotIndexOutsideSearchRange);
        ensureGreen(searchableSnapshotIndexOutsideSearchRange);

        final IndexMetadata updatedIndexMetadata = getIndexMetadata(searchableSnapshotIndexOutsideSearchRange);
        final IndexLongFieldRange updatedTimestampMillisRange = updatedIndexMetadata.getTimestampRange();
        final DateFieldMapper.DateFieldType dateFieldType = indicesService.getTimestampFieldType(updatedIndexMetadata.getIndex());
        assertThat(dateFieldType, notNullValue());
        final DateFieldMapper.Resolution resolution = dateFieldType.resolution();
        assertThat(updatedTimestampMillisRange.isComplete(), equalTo(true));
        if (indexDataWithTimestamp) {
            assertThat(updatedTimestampMillisRange, not(sameInstance(IndexLongFieldRange.EMPTY)));
            assertThat(
                updatedTimestampMillisRange.getMin(),
                greaterThanOrEqualTo(resolution.convert(Instant.parse("2020-11-26T00:00:00Z")))
            );
            assertThat(updatedTimestampMillisRange.getMax(), lessThanOrEqualTo(resolution.convert(Instant.parse("2020-11-27T00:00:00Z"))));
        } else {
            assertThat(updatedTimestampMillisRange, sameInstance(IndexLongFieldRange.EMPTY));
        }

        // Stop the node holding the searchable snapshots, and since we defined
        // the index allocation criteria to require the searchable snapshot
        // index to be allocated in that node, the shards should remain unassigned
        internalCluster().stopNode(dataNodeHoldingSearchableSnapshot);
        waitUntilAllShardsAreUnassigned(updatedIndexMetadata.getIndex());

        if (includeIndexCoveringSearchRangeInSearchRequest) {
            SearchResponse newSearchResponse = client().search(request).actionGet();

            assertThat(newSearchResponse.getSkippedShards(), equalTo(indexOutsideSearchRangeShardCount));
            assertThat(newSearchResponse.getSuccessfulShards(), equalTo(totalShards));
            assertThat(newSearchResponse.getFailedShards(), equalTo(0));
            assertThat(newSearchResponse.getTotalShards(), equalTo(totalShards));
            assertThat(newSearchResponse.getHits().getTotalHits().value, equalTo((long) numDocsWithinRange));

            // test with SearchShardsAPI
            {
                boolean allowPartialSearchResults = true;
                SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                    indicesToSearch.toArray(new String[0]),
                    SearchRequest.DEFAULT_INDICES_OPTIONS,
                    rangeQuery,
                    null,
                    null,
                    allowPartialSearchResults,
                    null
                );

                SearchShardsResponse searchShardsResponse = client().execute(SearchShardsAction.INSTANCE, searchShardsRequest).actionGet();
                assertThat(searchShardsResponse.getGroups().size(), equalTo(totalShards));
                List<List<SearchShardsGroup>> partitionedBySkipped = searchShardsResponse.getGroups()
                    .stream()
                    .collect(
                        Collectors.teeing(
                            Collectors.filtering(g -> g.skipped(), Collectors.toList()),
                            Collectors.filtering(g -> g.skipped() == false, Collectors.toList()),
                            List::of
                        )
                    );
                List<SearchShardsGroup> skipped = partitionedBySkipped.get(0);
                List<SearchShardsGroup> notSkipped = partitionedBySkipped.get(1);
                assertThat(skipped.size(), equalTo(indexOutsideSearchRangeShardCount));
                assertThat(notSkipped.size(), equalTo(totalShards - indexOutsideSearchRangeShardCount));
            }
        } else {
            if (indexOutsideSearchRangeShardCount == 1) {
                expectThrows(SearchPhaseExecutionException.class, () -> client().search(request).actionGet());
                // test with SearchShardsAPI
                {
                    boolean allowPartialSearchResults = false;
                    SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                        indicesToSearch.toArray(new String[0]),
                        SearchRequest.DEFAULT_INDICES_OPTIONS,
                        rangeQuery,
                        null,
                        null,
                        allowPartialSearchResults,
                        null
                    );

                    SearchShardsResponse searchShardsResponse = null;
                    try {
                        searchShardsResponse = client().execute(SearchShardsAction.INSTANCE, searchShardsRequest).actionGet();
                    } catch (SearchPhaseExecutionException e) {
                        // ignore as this is what should happen
                    }
                    if (searchShardsResponse != null) {
                        for (SearchShardsGroup group : searchShardsResponse.getGroups()) {
                            assertFalse("no shard should be marked as skipped", group.skipped());
                        }
                    }
                }
            } else {
                SearchResponse newSearchResponse = client().search(request).actionGet();
                // When all shards are skipped, at least one of them should be queried in order to
                // provide a proper search response.
                assertThat(newSearchResponse.getSkippedShards(), equalTo(indexOutsideSearchRangeShardCount - 1));
                assertThat(newSearchResponse.getSuccessfulShards(), equalTo(indexOutsideSearchRangeShardCount - 1));
                assertThat(newSearchResponse.getFailedShards(), equalTo(1));
                assertThat(newSearchResponse.getTotalShards(), equalTo(indexOutsideSearchRangeShardCount));

                // test with SearchShardsAPI
                {
                    boolean allowPartialSearchResults = true;
                    SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                        indicesToSearch.toArray(new String[0]),
                        SearchRequest.DEFAULT_INDICES_OPTIONS,
                        rangeQuery,
                        null,
                        null,
                        allowPartialSearchResults,
                        null
                    );

                    SearchShardsResponse searchShardsResponse = client().execute(SearchShardsAction.INSTANCE, searchShardsRequest)
                        .actionGet();
                    assertThat(searchShardsResponse.getGroups().size(), equalTo(indexOutsideSearchRangeShardCount));
                    List<List<SearchShardsGroup>> partitionedBySkipped = searchShardsResponse.getGroups()
                        .stream()
                        .collect(
                            Collectors.teeing(
                                Collectors.filtering(g -> g.skipped(), Collectors.toList()),
                                Collectors.filtering(g -> g.skipped() == false, Collectors.toList()),
                                List::of
                            )
                        );
                    List<SearchShardsGroup> skipped = partitionedBySkipped.get(0);
                    List<SearchShardsGroup> notSkipped = partitionedBySkipped.get(1);
                    assertThat(skipped.size(), equalTo(indexOutsideSearchRangeShardCount));
                    assertThat(notSkipped.size(), equalTo(indexOutsideSearchRangeShardCount - indexOutsideSearchRangeShardCount));
                }
            }
        }
    }

    /**
     * Can match against searchable snapshots is tested via both the Search API and the SearchShards (transport-only) API.
     * The latter is a way to do only a can-match rather than all search phases.
     */
    public void testQueryPhaseIsExecutedInAnAvailableNodeWhenAllShardsCanBeSkipped() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        final String dataNodeHoldingRegularIndex = internalCluster().startDataOnlyNode();
        final String dataNodeHoldingSearchableSnapshot = internalCluster().startDataOnlyNode();
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, dataNodeHoldingSearchableSnapshot);

        final String indexOutsideSearchRange = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int indexOutsideSearchRangeShardCount = randomIntBetween(1, 3);
        createIndexWithTimestamp(
            indexOutsideSearchRange,
            indexOutsideSearchRangeShardCount,
            Settings.builder()
                .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNodeHoldingRegularIndex)
                .build()
        );

        indexDocumentsWithTimestampWithinDate(indexOutsideSearchRange, between(1, 1000), TIMESTAMP_TEMPLATE_OUTSIDE_RANGE);

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "mock");

        final SnapshotId snapshotId = createSnapshot(repositoryName, "snapshot-1", List.of(indexOutsideSearchRange)).snapshotId();

        final String searchableSnapshotIndexOutsideSearchRange = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        // Block the repository for the node holding the searchable snapshot shards
        // to delay its restore
        blockNodeOnAnyFiles(repositoryName, dataNodeHoldingSearchableSnapshot);

        // Force the searchable snapshot to be allocated in a particular node
        Settings restoredIndexSettings = Settings.builder()
            .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNodeHoldingSearchableSnapshot)
            .build();

        final MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
            searchableSnapshotIndexOutsideSearchRange,
            repositoryName,
            snapshotId.getName(),
            indexOutsideSearchRange,
            restoredIndexSettings,
            Strings.EMPTY_ARRAY,
            false,
            randomFrom(MountSearchableSnapshotRequest.Storage.values())
        );
        client().execute(MountSearchableSnapshotAction.INSTANCE, mountRequest).actionGet();
        final int searchableSnapshotShardCount = indexOutsideSearchRangeShardCount;

        final IndexMetadata indexMetadata = getIndexMetadata(searchableSnapshotIndexOutsideSearchRange);
        assertThat(indexMetadata.getTimestampRange(), equalTo(IndexLongFieldRange.NO_SHARDS));

        DateFieldMapper.DateFieldType timestampFieldType = indicesService.getTimestampFieldType(indexMetadata.getIndex());
        assertThat(timestampFieldType, nullValue());

        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(DataStream.TIMESTAMP_FIELD_NAME)
            .from("2020-11-28T00:00:00.000000000Z", true)
            .to("2020-11-29T00:00:00.000000000Z");

        SearchRequest request = new SearchRequest().indices(indexOutsideSearchRange, searchableSnapshotIndexOutsideSearchRange)
            .source(new SearchSourceBuilder().query(rangeQuery));

        final int totalShards = indexOutsideSearchRangeShardCount + searchableSnapshotShardCount;

        // test with Search API
        {
            SearchResponse searchResponse = client().search(request).actionGet();
            // All the regular index searches succeeded
            assertThat(searchResponse.getSuccessfulShards(), equalTo(indexOutsideSearchRangeShardCount));
            // All the searchable snapshots shard search failed
            assertThat(searchResponse.getFailedShards(), equalTo(indexOutsideSearchRangeShardCount));
            assertThat(searchResponse.getSkippedShards(), equalTo(searchableSnapshotShardCount));
            assertThat(searchResponse.getTotalShards(), equalTo(totalShards));
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        }

        // test with SearchShards API
        {
            boolean allowPartialSearchResults = true;
            SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                new String[] { indexOutsideSearchRange, searchableSnapshotIndexOutsideSearchRange },
                SearchRequest.DEFAULT_INDICES_OPTIONS,
                rangeQuery,
                null,
                null,
                allowPartialSearchResults,
                null
            );

            SearchShardsResponse searchShardsResponse = client().execute(SearchShardsAction.INSTANCE, searchShardsRequest).actionGet();
            assertThat(searchShardsResponse.getGroups().size(), equalTo(totalShards));
            List<List<SearchShardsGroup>> partitionedBySkipped = searchShardsResponse.getGroups()
                .stream()
                .collect(
                    Collectors.teeing(
                        Collectors.filtering(g -> g.skipped(), Collectors.toList()),
                        Collectors.filtering(g -> g.skipped() == false, Collectors.toList()),
                        List::of
                    )
                );
            List<SearchShardsGroup> skipped = partitionedBySkipped.get(0);
            List<SearchShardsGroup> notSkipped = partitionedBySkipped.get(1);
            assertThat(skipped.size(), equalTo(searchableSnapshotShardCount));
            assertThat(notSkipped.size(), equalTo(indexOutsideSearchRangeShardCount));
        }

        // Allow the searchable snapshots to be finally mounted
        unblockNode(repositoryName, dataNodeHoldingSearchableSnapshot);
        waitUntilRecoveryIsDone(searchableSnapshotIndexOutsideSearchRange);
        ensureGreen(searchableSnapshotIndexOutsideSearchRange);

        final IndexMetadata updatedIndexMetadata = getIndexMetadata(searchableSnapshotIndexOutsideSearchRange);
        final IndexLongFieldRange updatedTimestampMillisRange = updatedIndexMetadata.getTimestampRange();
        final DateFieldMapper.DateFieldType dateFieldType = indicesService.getTimestampFieldType(updatedIndexMetadata.getIndex());
        assertThat(dateFieldType, notNullValue());
        final DateFieldMapper.Resolution resolution = dateFieldType.resolution();
        assertThat(updatedTimestampMillisRange.isComplete(), equalTo(true));
        assertThat(updatedTimestampMillisRange, not(sameInstance(IndexLongFieldRange.EMPTY)));
        assertThat(updatedTimestampMillisRange.getMin(), greaterThanOrEqualTo(resolution.convert(Instant.parse("2020-11-26T00:00:00Z"))));
        assertThat(updatedTimestampMillisRange.getMax(), lessThanOrEqualTo(resolution.convert(Instant.parse("2020-11-27T00:00:00Z"))));

        // Stop the node holding the searchable snapshots, and since we defined
        // the index allocation criteria to require the searchable snapshot
        // index to be allocated in that node, the shards should remain unassigned
        internalCluster().stopNode(dataNodeHoldingSearchableSnapshot);
        waitUntilAllShardsAreUnassigned(updatedIndexMetadata.getIndex());

        // busy assert since computing the time stamp field from the cluster state happens off of the CS applier thread and thus can be
        // slightly delayed
        assertBusy(() -> {
            SearchResponse newSearchResponse = client().search(request).actionGet();

            // All the regular index searches succeeded
            assertThat(newSearchResponse.getSuccessfulShards(), equalTo(totalShards));
            assertThat(newSearchResponse.getFailedShards(), equalTo(0));
            // We have to query at least one node to construct a valid response, and we pick
            // a shard that's available in order to construct the search response
            assertThat(newSearchResponse.getSkippedShards(), equalTo(totalShards - 1));
            assertThat(newSearchResponse.getTotalShards(), equalTo(totalShards));
            assertThat(newSearchResponse.getHits().getTotalHits().value, equalTo(0L));
        });

        // test with SearchShards API
        {
            boolean allowPartialSearchResults = true;
            SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                new String[] { indexOutsideSearchRange, searchableSnapshotIndexOutsideSearchRange },
                SearchRequest.DEFAULT_INDICES_OPTIONS,
                rangeQuery,
                null,
                null,
                allowPartialSearchResults,
                null
            );

            SearchShardsResponse searchShardsResponse = client().execute(SearchShardsAction.INSTANCE, searchShardsRequest).actionGet();
            assertThat(searchShardsResponse.getGroups().size(), equalTo(totalShards));
            List<List<SearchShardsGroup>> partitionedBySkipped = searchShardsResponse.getGroups()
                .stream()
                .collect(
                    Collectors.teeing(
                        Collectors.filtering(g -> g.skipped(), Collectors.toList()),
                        Collectors.filtering(g -> g.skipped() == false, Collectors.toList()),
                        List::of
                    )
                );
            List<SearchShardsGroup> skipped = partitionedBySkipped.get(0);
            List<SearchShardsGroup> notSkipped = partitionedBySkipped.get(1);
            assertThat(skipped.size(), equalTo(totalShards));
            assertThat(notSkipped.size(), equalTo(0));
        }
    }

    /**
     * Can match against searchable snapshots is tested via both the Search API and the SearchShards (transport-only) API.
     * The latter is a way to do only a can-match rather than all search phases.
     */
    public void testSearchableSnapshotShardsThatHaveMatchingDataAreNotSkippedOnTheCoordinatingNode() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        final String dataNodeHoldingRegularIndex = internalCluster().startDataOnlyNode();
        final String dataNodeHoldingSearchableSnapshot = internalCluster().startDataOnlyNode();
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, dataNodeHoldingSearchableSnapshot);

        final String indexWithinSearchRange = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int indexWithinSearchRangeShardCount = randomIntBetween(1, 3);
        createIndexWithTimestamp(
            indexWithinSearchRange,
            indexWithinSearchRangeShardCount,
            Settings.builder()
                .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNodeHoldingRegularIndex)
                .build()
        );

        indexDocumentsWithTimestampWithinDate(indexWithinSearchRange, between(1, 1000), TIMESTAMP_TEMPLATE_WITHIN_RANGE);

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "mock");

        final SnapshotId snapshotId = createSnapshot(repositoryName, "snapshot-1", List.of(indexWithinSearchRange)).snapshotId();
        assertAcked(indicesAdmin().prepareDelete(indexWithinSearchRange));

        final String searchableSnapshotIndexWithinSearchRange = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        // Block the repository for the node holding the searchable snapshot shards
        // to delay its restore
        blockDataNode(repositoryName, dataNodeHoldingSearchableSnapshot);

        // Force the searchable snapshot to be allocated in a particular node
        Settings restoredIndexSettings = Settings.builder()
            .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNodeHoldingSearchableSnapshot)
            .build();

        final MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
            searchableSnapshotIndexWithinSearchRange,
            repositoryName,
            snapshotId.getName(),
            indexWithinSearchRange,
            restoredIndexSettings,
            Strings.EMPTY_ARRAY,
            false,
            randomFrom(MountSearchableSnapshotRequest.Storage.values())
        );
        client().execute(MountSearchableSnapshotAction.INSTANCE, mountRequest).actionGet();

        final IndexMetadata indexMetadata = getIndexMetadata(searchableSnapshotIndexWithinSearchRange);
        assertThat(indexMetadata.getTimestampRange(), equalTo(IndexLongFieldRange.NO_SHARDS));

        DateFieldMapper.DateFieldType timestampFieldType = indicesService.getTimestampFieldType(indexMetadata.getIndex());
        assertThat(timestampFieldType, nullValue());

        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(DataStream.TIMESTAMP_FIELD_NAME)
            .from("2020-11-28T00:00:00.000000000Z", true)
            .to("2020-11-29T00:00:00.000000000Z");

        SearchRequest request = new SearchRequest().indices(searchableSnapshotIndexWithinSearchRange)
            .source(new SearchSourceBuilder().query(rangeQuery));

        // All shards failed, since all shards are unassigned and the IndexMetadata min/max timestamp
        // is not available yet
        expectThrows(SearchPhaseExecutionException.class, () -> client().search(request).actionGet());

        // test with SearchShards API
        boolean allowPartialSearchResults = false;
        SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
            new String[] { searchableSnapshotIndexWithinSearchRange },
            SearchRequest.DEFAULT_INDICES_OPTIONS,
            rangeQuery,
            null,
            null,
            allowPartialSearchResults,
            null
        );

        {
            SearchShardsResponse searchShardsResponse = null;
            try {
                client().execute(SearchShardsAction.INSTANCE, searchShardsRequest).actionGet();
            } catch (SearchPhaseExecutionException e) {
                // ignore as this is expected to happen
            }
            if (searchShardsResponse != null) {
                if (searchShardsResponse != null) {
                    for (SearchShardsGroup group : searchShardsResponse.getGroups()) {
                        assertFalse("no shard should be marked as skipped", group.skipped());
                    }
                }
            }
        }

        // Allow the searchable snapshots to be finally mounted
        unblockNode(repositoryName, dataNodeHoldingSearchableSnapshot);
        waitUntilRecoveryIsDone(searchableSnapshotIndexWithinSearchRange);
        ensureGreen(searchableSnapshotIndexWithinSearchRange);

        final IndexMetadata updatedIndexMetadata = getIndexMetadata(searchableSnapshotIndexWithinSearchRange);
        final IndexLongFieldRange updatedTimestampMillisRange = updatedIndexMetadata.getTimestampRange();
        final DateFieldMapper.DateFieldType dateFieldType = indicesService.getTimestampFieldType(updatedIndexMetadata.getIndex());
        assertThat(dateFieldType, notNullValue());
        final DateFieldMapper.Resolution resolution = dateFieldType.resolution();
        assertThat(updatedTimestampMillisRange.isComplete(), equalTo(true));
        assertThat(updatedTimestampMillisRange, not(sameInstance(IndexLongFieldRange.EMPTY)));
        assertThat(updatedTimestampMillisRange.getMin(), greaterThanOrEqualTo(resolution.convert(Instant.parse("2020-11-28T00:00:00Z"))));
        assertThat(updatedTimestampMillisRange.getMax(), lessThanOrEqualTo(resolution.convert(Instant.parse("2020-11-29T00:00:00Z"))));

        // Stop the node holding the searchable snapshots, and since we defined
        // the index allocation criteria to require the searchable snapshot
        // index to be allocated in that node, the shards should remain unassigned
        internalCluster().stopNode(dataNodeHoldingSearchableSnapshot);
        waitUntilAllShardsAreUnassigned(updatedIndexMetadata.getIndex());

        // The range query matches but the shards that are unavailable, in that case the search fails, as all shards that hold
        // data are unavailable
        expectThrows(SearchPhaseExecutionException.class, () -> client().search(request).actionGet());

        {
            SearchShardsResponse searchShardsResponse = null;
            try {
                client().execute(SearchShardsAction.INSTANCE, searchShardsRequest).actionGet();
            } catch (SearchPhaseExecutionException e) {
                // ignore as this is expected to happen
            }
            if (searchShardsResponse != null) {
                assertThat(searchShardsResponse.getGroups().size(), equalTo(indexWithinSearchRangeShardCount));
                List<List<SearchShardsGroup>> partitionedBySkipped = searchShardsResponse.getGroups()
                    .stream()
                    .collect(
                        Collectors.teeing(
                            Collectors.filtering(g -> g.skipped(), Collectors.toList()),
                            Collectors.filtering(g -> g.skipped() == false, Collectors.toList()),
                            List::of
                        )
                    );
                List<SearchShardsGroup> skipped = partitionedBySkipped.get(0);
                List<SearchShardsGroup> notSkipped = partitionedBySkipped.get(1);
                assertThat(skipped.size(), equalTo(0));
                assertThat(notSkipped.size(), equalTo(indexWithinSearchRangeShardCount));
            }
        }
    }

    private void createIndexWithTimestamp(String indexName, int numShards, Settings extraSettings) throws IOException {
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(DataStream.TIMESTAMP_FIELD_NAME)
                        .field("type", randomFrom("date", "date_nanos"))
                        .field("format", "strict_date_optional_time_nanos")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .setSettings(indexSettingsNoReplicas(numShards).put(INDEX_SOFT_DELETES_SETTING.getKey(), true).put(extraSettings))
        );
        ensureGreen(indexName);
    }

    private void indexDocumentsWithTimestampWithinDate(String indexName, int docCount, String timestampTemplate) throws Exception {
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            indexRequestBuilders.add(
                client().prepareIndex(indexName)
                    .setSource(
                        DataStream.TIMESTAMP_FIELD_NAME,
                        String.format(
                            Locale.ROOT,
                            timestampTemplate,
                            between(0, 23),
                            between(0, 59),
                            between(0, 59),
                            randomLongBetween(0, 999999999L)
                        )
                    )
            );
        }
        indexRandom(true, false, indexRequestBuilders);

        assertThat(
            indicesAdmin().prepareForceMerge(indexName).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
            equalTo(0)
        );
        refresh(indexName);
        forceMerge();
    }

    private IndexMetadata getIndexMetadata(String indexName) {
        return clusterAdmin().prepareState().clear().setMetadata(true).setIndices(indexName).get().getState().metadata().index(indexName);
    }

    private void waitUntilRecoveryIsDone(String index) throws Exception {
        assertBusy(() -> {
            RecoveryResponse recoveryResponse = indicesAdmin().prepareRecoveries(index).get();
            assertThat(recoveryResponse.hasRecoveries(), equalTo(true));
            for (List<RecoveryState> value : recoveryResponse.shardRecoveryStates().values()) {
                for (RecoveryState recoveryState : value) {
                    assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
                }
            }
        });
    }

    private void waitUntilAllShardsAreUnassigned(Index index) throws Exception {
        awaitClusterState(state -> state.getRoutingTable().index(index).allPrimaryShardsUnassigned());
    }
}
