/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class FetchedHitsTests extends ESTestCase {

    private final List<SearchHit> pooledHitsToRelease = new ArrayList<>();

    @After
    public void releasePooledSearchHits() {
        for (SearchHit hit : pooledHitsToRelease) {
            hit.decRef();
        }
        pooledHitsToRelease.clear();
    }

    public void testDropsAMatchWhoseFetchMissedAHitAndKeepsTheOthers() {
        SearchHit kept = hit("kept");
        SearchHit dropped = new SearchHit(1, "dropped");
        dropped.mustIncRef();
        List<String> matches = List.of("complete", "incomplete");
        List<List<SearchHit>> rows = List.of(List.of(kept), Arrays.asList(dropped, null));
        List<String> keptMatches = new ArrayList<>();
        List<List<SearchHit>> keptRows = new ArrayList<>();

        FetchedHits.dropIncomplete(matches, rows, keptMatches, keptRows);
        dropped.decRef();

        assertThat(keptMatches, contains("complete"));
        assertThat(keptRows, contains(List.of(kept)));
        assertFalse(dropped.hasReferences());
    }

    public void testDroppingOneRowKeepsTheReferenceHeldByAnotherRow() {
        SearchHit shared = new SearchHit(1, "shared");
        shared.mustIncRef();
        shared.mustIncRef();
        List<String> matches = List.of("kept", "dropped");
        List<List<SearchHit>> rows = List.of(List.of(shared), Arrays.asList(shared, null));
        List<String> keptMatches = new ArrayList<>();
        List<List<SearchHit>> keptRows = new ArrayList<>();

        FetchedHits.dropIncomplete(matches, rows, keptMatches, keptRows);

        assertThat(keptMatches, contains("kept"));
        assertTrue(shared.hasReferences());
        shared.decRef();
        assertTrue(shared.hasReferences());
        shared.decRef();
        assertFalse(shared.hasReferences());
    }

    public void testKeepsEveryMatchWhenEveryHitWasFetched() {
        SearchHit first = hit("1");
        SearchHit second = hit("2");
        List<String> matches = List.of("only");
        List<List<SearchHit>> rows = List.of(List.of(first, second));
        List<String> keptMatches = new ArrayList<>();
        List<List<SearchHit>> keptRows = new ArrayList<>();

        FetchedHits.dropIncomplete(matches, rows, keptMatches, keptRows);

        assertThat(keptMatches, contains("only"));
        assertThat(keptRows, contains(List.of(first, second)));
    }

    public void testDropsEveryMatchWhenEachOneMissedAHit() {
        SearchHit dropped = new SearchHit(2, "dropped");
        dropped.mustIncRef();
        List<String> matches = List.of("incomplete");
        List<List<SearchHit>> rows = List.of(Arrays.asList(null, dropped));
        List<String> keptMatches = new ArrayList<>();
        List<List<SearchHit>> keptRows = new ArrayList<>();

        FetchedHits.dropIncomplete(matches, rows, keptMatches, keptRows);
        dropped.decRef();

        assertThat(keptMatches, empty());
        assertThat(keptRows, empty());
        assertFalse(dropped.hasReferences());
    }

    private SearchHit hit(String id) {
        SearchHit hit = new SearchHit(pooledHitsToRelease.size(), id);
        pooledHitsToRelease.add(hit);
        return hit;
    }
}
