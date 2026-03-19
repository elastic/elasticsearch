/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class ReindexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MainRestPlugin.class);
    }

    /**
     * Manual slicing without field defaults to _id. This test verifies that reindexing slice 0, then updating
     * all docs, then reindexing slice 1 produces the correct doc IDs in the destination (no duplicates, no missing).
     */
    public void testManualSliceWithoutFieldDefaultsToId() {
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = 20;

        createIndex(sourceIndex);
        indexRandom(
            true,
            IntStream.range(0, totalDocs)
                .mapToObj(i -> prepareIndex(sourceIndex).setId(String.valueOf(i)).setSource("foo", "v" + i))
                .toArray(IndexRequestBuilder[]::new)
        );
        assertHitCount(prepareSearch(sourceIndex).setSize(0), totalDocs);

        // Reindex slice 0 of 2 (no field - defaults to _id)
        ReindexRequest slice0Request = new ReindexRequest().setSourceIndices(sourceIndex).setDestIndex(destIndex).setRefresh(true);
        slice0Request.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(0, 2)));
        BulkByScrollResponse slice0Response = client().execute(ReindexAction.INSTANCE, slice0Request).actionGet();
        assertTrue("slice 0 should index some docs", slice0Response.getCreated() > 0);

        // Update all docs in source (use bulk index to add a field)
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < totalDocs; i++) {
            bulkRequest.add(prepareIndex(sourceIndex).setId(String.valueOf(i)).setSource("foo", "v" + i, "updated", true).request());
        }
        client().bulk(bulkRequest).actionGet();
        client().admin().indices().refresh(new RefreshRequest(sourceIndex)).actionGet();

        // Reindex slice 1 of 2
        ReindexRequest slice1Request = new ReindexRequest().setSourceIndices(sourceIndex).setDestIndex(destIndex).setRefresh(true);
        slice1Request.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(1, 2)));
        BulkByScrollResponse slice1Response = client().execute(ReindexAction.INSTANCE, slice1Request).actionGet();
        assertTrue("slice 1 should index some docs", slice1Response.getCreated() > 0);

        // Destination must have exactly totalDocs with correct IDs (no duplicates, no missing)
        assertHitCount(prepareSearch(destIndex).setSize(totalDocs), totalDocs);
        SearchResponse destSearch = prepareSearch(destIndex).setSize(totalDocs).get();
        try {
            Set<String> destIds = Arrays.stream(destSearch.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet());
            assertEquals("destination should have exactly " + totalDocs + " unique doc IDs", totalDocs, destIds.size());
            for (int i = 0; i < totalDocs; i++) {
                assertTrue("destination should contain doc " + i, destIds.contains(String.valueOf(i)));
            }
        } finally {
            destSearch.decRef();
        }
    }
}
