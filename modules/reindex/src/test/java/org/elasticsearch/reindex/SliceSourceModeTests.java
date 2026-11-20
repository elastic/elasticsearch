/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.nullValue;

public class SliceSourceModeTests extends ESTestCase {

    private IndexNameExpressionResolver indexResolver;
    private ProjectMetadata projectMetadata;

    @Before
    public void createFixtures() {
        // reading index.slice.enabled fails without the feature flag, so the fixtures are only built when it is on
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        indexResolver = TestIndexNameExpressionResolver.newInstance();
        projectMetadata = ProjectMetadata.builder(randomUniqueProjectId())
            .put(index("sliced-a", true), true)
            .put(index("sliced-b", true), true)
            .put(index("plain", false), true)
            .build();
    }

    public void testSliceEnabledSourceWithoutSliceIsMarked() {
        SearchRequest source = new SearchRequest("sliced-a");
        SliceSourceMode.markIfSliceEnabled(source, indexResolver, projectMetadata);

        assertTrue(source.isRoutingFromSlice());
        // every slice is still read
        assertThat(source.routing(), nullValue());
        assertThat(source.searchSlice(), nullValue());
    }

    public void testNonSliceSourceIsNotMarked() {
        SearchRequest source = new SearchRequest("plain");
        SliceSourceMode.markIfSliceEnabled(source, indexResolver, projectMetadata);

        assertFalse(source.isRoutingFromSlice());
    }

    public void testSourceMixingSliceEnabledAndNonSliceIndicesIsMarked() {
        for (String expression : new String[] { "plain,sliced-a", "*" }) {
            SearchRequest source = new SearchRequest(expression.split(","));
            SliceSourceMode.markIfSliceEnabled(source, indexResolver, projectMetadata);

            assertTrue(expression, source.isRoutingFromSlice());
        }
    }

    public void testWildcardSourceOnlyMatchingNonSliceIndicesIsNotMarked() {
        SearchRequest source = new SearchRequest("pla*");
        SliceSourceMode.markIfSliceEnabled(source, indexResolver, projectMetadata);

        assertFalse(source.isRoutingFromSlice());
    }

    public void testExplicitSliceIsLeftAlone() {
        SearchRequest source = new SearchRequest("sliced-a").searchSlice("tenant-a");
        SliceSourceMode.markIfSliceEnabled(source, indexResolver, projectMetadata);

        assertTrue(source.isRoutingFromSlice());
        assertEquals("tenant-a", source.searchSlice());
        assertEquals("tenant-a", source.routing());
    }

    public void testPlainRoutingIsNotTurnedIntoASlice() {
        SearchRequest source = new SearchRequest("sliced-a").routing("r1");
        SliceSourceMode.markIfSliceEnabled(source, indexResolver, projectMetadata);

        assertFalse(source.isRoutingFromSlice());
        assertEquals("r1", source.routing());
    }

    public void testRemoteIndexNamesAreNotResolvedLocally() {
        // a remote index that happens to share its name with a local slice-enabled index says nothing about the remote cluster
        SearchRequest remoteOnly = new SearchRequest("cluster_one:sliced-a");
        SliceSourceMode.markIfSliceEnabled(remoteOnly, indexResolver, projectMetadata);
        assertFalse(remoteOnly.isRoutingFromSlice());

        SearchRequest remoteAndLocal = new SearchRequest("cluster_one:sliced-a", "sliced-b");
        SliceSourceMode.markIfSliceEnabled(remoteAndLocal, indexResolver, projectMetadata);
        assertTrue(remoteAndLocal.isRoutingFromSlice());
    }

    private static IndexMetadata index(String name, boolean sliceEnabled) {
        return IndexMetadata.builder(name)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.SLICE_ENABLED.getKey(), sliceEnabled).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
