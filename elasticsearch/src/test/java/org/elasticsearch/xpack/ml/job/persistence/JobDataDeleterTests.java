/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelState;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.mock.orig.Mockito.mock;
import static org.elasticsearch.mock.orig.Mockito.times;
import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

public class JobDataDeleterTests extends ESTestCase {

    public void testDeleteResultsFromTime() {

        final long TOTAL_HIT_COUNT = 100L;
        final int PER_SCROLL_SEARCH_HIT_COUNT = 20;

        SearchResponse response = createSearchResponseWithHits(TOTAL_HIT_COUNT, PER_SCROLL_SEARCH_HIT_COUNT);
        BulkResponse bulkResponse = Mockito.mock(BulkResponse.class);

        Client client = new MockClientBuilder("myCluster")
                                .prepareSearchExecuteListener(AnomalyDetectorsIndex.jobResultsIndexName("foo"), response)
                                .prepareSearchScrollExecuteListener(response)
                                .prepareBulk(bulkResponse).build();

        JobDataDeleter bulkDeleter = new JobDataDeleter(client, "foo");

        // because of the mocking this runs in the current thread
        bulkDeleter.deleteResultsFromTime(new Date().getTime(), new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                assertTrue(aBoolean);
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }
        });

        verify(client.prepareBulk(), times((int)TOTAL_HIT_COUNT)).add(any(DeleteRequestBuilder.class));

        ActionListener<BulkResponse> bulkListener = new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }
        };

        when(client.prepareBulk().numberOfActions()).thenReturn(new Integer((int)TOTAL_HIT_COUNT));
        bulkDeleter.commit(bulkListener);

        verify(client.prepareBulk(), times(1)).execute(bulkListener);
    }

    public void testDeleteModelSnapShot() {
        String jobId = "foo";
        ModelSnapshot snapshot = new ModelSnapshot(jobId);
        snapshot.setSnapshotDocCount(5);
        snapshot.setSnapshotId("snap-1");

        BulkResponse bulkResponse = Mockito.mock(BulkResponse.class);
        Client client = new MockClientBuilder("myCluster").prepareBulk(bulkResponse).build();

        JobDataDeleter bulkDeleter = new JobDataDeleter(client, jobId);
        bulkDeleter.deleteModelSnapshot(snapshot);
        verify(client, times(5))
                .prepareDelete(eq(AnomalyDetectorsIndex.jobStateIndexName()), eq(ModelState.TYPE.getPreferredName()), anyString());
        verify(client, times(1))
                .prepareDelete(eq(AnomalyDetectorsIndex.jobResultsIndexName(jobId)), eq(ModelSnapshot.TYPE.getPreferredName()),
                        eq("snap-1"));
    }

    private SearchResponse createSearchResponseWithHits(long totalHitCount, int hitsPerSearchResult) {
        SearchHits hits = mockSearchHits(totalHitCount, hitsPerSearchResult);
        SearchResponse searchResponse = Mockito.mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(hits);
        when(searchResponse.getScrollId()).thenReturn("scroll1");
        return searchResponse;
    }

    private SearchHits mockSearchHits(long totalHitCount, int hitsPerSearchResult) {

        SearchHits hits = Mockito.mock(SearchHits.class);
        when(hits.getTotalHits()).thenReturn(totalHitCount);

        List<SearchHit> hitList = new ArrayList<>();
        for (int i=0; i<20; i++) {
            SearchHit hit = Mockito.mock(SearchHit.class);
            when(hit.getType()).thenReturn("mockSearchHit");
            when(hit.getId()).thenReturn("mockSeachHit-" + i);
            hitList.add(hit);
        }
        when(hits.getHits()).thenReturn(hitList.toArray(new SearchHit[hitList.size()]));
        when(hits.getHits()).thenReturn(hitList.toArray(new SearchHit[hitList.size()]));

        return hits;
    }
}
