/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class SearchableSnapshotsSearchIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    /**
     * Tests basic search functionality with a query sorted by field against partially mounted indices
     * The can match phase is always executed against read only indices, and for sorted queries it extracts the min and max range from
     * each shard. This will happen not only in the can match phase, but optionally also in the query phase.
     * See {@link org.elasticsearch.search.internal.ShardSearchRequest#canReturnNullResponseIfMatchNoDocs()}.
     * For keyword fields, it is not possible to retrieve min and max from the index reader on frozen, hence we need to make sure that
     * while that fails, the query will go ahead and won't return shard failures.
     */
    public void testKeywordSortedQueryOnFrozen() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        final String dataNodeHoldingRegularIndex = internalCluster().startDataOnlyNode();
        String dataNodeHoldingSearchableSnapshot = internalCluster().startDataOnlyNode();

        String[] indices = new String[] { "index-0001", "index-0002" };
        for (String index : indices) {
            Settings extraSettings = Settings.builder()
                .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNodeHoldingRegularIndex)
                .build();
            // we use a high number of shards because that's more likely to trigger can match as part of query phase:
            // see ShardSearchRequest#canReturnNullResponseIfMatchNoDocs
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(indexSettingsNoReplicas(10).put(INDEX_SOFT_DELETES_SETTING.getKey(), true).put(extraSettings))
            );
        }
        ensureGreen(indices);

        for (String index : indices) {
            final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
            indexRequestBuilders.add(prepareIndex(index).setSource("keyword", "value1"));
            indexRequestBuilders.add(prepareIndex(index).setSource("keyword", "value2"));
            indexRandom(true, false, indexRequestBuilders);
            assertThat(
                indicesAdmin().prepareForceMerge(index).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
                equalTo(0)
            );
            refresh(index);
            forceMerge();
        }

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "mock");

        final SnapshotId snapshotId = createSnapshot(repositoryName, "snapshot-1", List.of(indices[0])).snapshotId();
        assertAcked(indicesAdmin().prepareDelete(indices[0]));

        // Block the repository for the node holding the searchable snapshot shards
        // to delay its restore
        blockDataNode(repositoryName, dataNodeHoldingSearchableSnapshot);

        // Force the searchable snapshot to be allocated in a particular node
        Settings restoredIndexSettings = Settings.builder()
            .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNodeHoldingSearchableSnapshot)
            .build();
        String[] mountedIndices = new String[indices.length];
        for (int i = 0; i < indices.length; i++) {

            String index = indices[i];
            String mountedIndex = index + "-mounted";
            mountedIndices[i] = mountedIndex;
            final MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
                TEST_REQUEST_TIMEOUT,
                mountedIndex,
                repositoryName,
                snapshotId.getName(),
                indices[0],
                restoredIndexSettings,
                Strings.EMPTY_ARRAY,
                false,
                randomFrom(MountSearchableSnapshotRequest.Storage.values())
            );
            client().execute(MountSearchableSnapshotAction.INSTANCE, mountRequest).actionGet();
        }

        // Allow the searchable snapshots to be finally mounted
        unblockNode(repositoryName, dataNodeHoldingSearchableSnapshot);
        for (String mountedIndex : mountedIndices) {
            waitUntilRecoveryIsDone(mountedIndex);
        }
        ensureGreen(mountedIndices);

        SearchRequest request = new SearchRequest(mountedIndices).searchType(SearchType.QUERY_THEN_FETCH)
            .source(SearchSourceBuilder.searchSource().sort("keyword.keyword"))
            .allowPartialSearchResults(false);
        if (randomBoolean()) {
            request.setPreFilterShardSize(100);
        }

        assertResponse(client().search(request), searchResponse -> {
            assertThat(searchResponse.getSuccessfulShards(), equalTo(20));
            assertThat(searchResponse.getFailedShards(), equalTo(0));
            assertThat(searchResponse.getSkippedShards(), equalTo(0));
            assertThat(searchResponse.getTotalShards(), equalTo(20));
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(4L));
        });

        // check that field_caps empty field filtering works as well
        FieldCapabilitiesResponse response = client().prepareFieldCaps(mountedIndices).setFields("*").setincludeEmptyFields(false).get();
        assertNotNull(response.getField("keyword"));
    }
}
