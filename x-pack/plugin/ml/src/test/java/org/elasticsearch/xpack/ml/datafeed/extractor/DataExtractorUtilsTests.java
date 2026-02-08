/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;

public class DataExtractorUtilsTests extends ESTestCase {

    public void testCheckForSkippedClustersNoSkips() {
        // Clusters with no skipped clusters should not throw
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(3, 3, 0);
        SearchResponse response = newSearchResponseWithClusters(clusters);
        try {
            DataExtractorUtils.checkForSkippedClusters(response); // should not throw
        } finally {
            response.decRef();
        }
    }

    public void testCheckForSkippedClustersWithSkippedClusters() {
        // Clusters with some skipped should throw ResourceNotFoundException
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(3, 2, 1);
        SearchResponse response = newSearchResponseWithClusters(clusters);
        try {
            ResourceNotFoundException e = expectThrows(
                ResourceNotFoundException.class,
                () -> DataExtractorUtils.checkForSkippedClusters(response)
            );
            assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("1"));
            assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("3"));
        } finally {
            response.decRef();
        }
    }

    public void testCheckForSkippedClustersNullClusters() {
        // Null clusters (non-CCS/CPS search) should not throw
        SearchResponse response = newSearchResponseWithClusters(null);
        try {
            DataExtractorUtils.checkForSkippedClusters(response); // should not throw
        } finally {
            response.decRef();
        }
    }

    public void testCheckForSkippedClustersEmptyClusters() {
        // Empty clusters should not throw
        SearchResponse response = newSearchResponseWithClusters(SearchResponse.Clusters.EMPTY);
        try {
            DataExtractorUtils.checkForSkippedClusters(response); // should not throw
        } finally {
            response.decRef();
        }
    }

    /**
     * Simulate a CPS scenario: multiple projects with some skipped.
     * CPS projects appear as remote clusters in SearchResponse.Clusters.
     * This test verifies the existing CCS check also catches CPS skips.
     */
    public void testCheckForSkippedClustersWithCpsProjectsSkipped() {
        // 5 total clusters (1 local + 4 CPS projects), 2 skipped
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(5, 3, 2);
        SearchResponse response = newSearchResponseWithClusters(clusters);
        try {
            ResourceNotFoundException e = expectThrows(
                ResourceNotFoundException.class,
                () -> DataExtractorUtils.checkForSkippedClusters(response)
            );
            assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("2"));
            assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("5"));
        } finally {
            response.decRef();
        }
    }

    private static SearchResponse newSearchResponseWithClusters(SearchResponse.Clusters clusters) {
        return new SearchResponse(
            SearchHits.EMPTY_WITH_TOTAL_HITS,
            null,
            null,
            false,
            null,
            null,
            1,
            null,
            1,
            1,
            0,
            100L,
            ShardSearchFailure.EMPTY_ARRAY,
            clusters
        );
    }
}
