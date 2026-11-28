/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.action.SliceMissingException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * A slice-enabled index requires routing by default, and that must hold for exactly that index in the cluster state: not for a plain
 * index that happens to have the same mapping, and still after the mapping is read back from disk
 * (see https://github.com/elastic/elasticsearch/issues/167192).
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SliceRoutingRequiredIT extends ESIntegTestCase {

    @Before
    public void requireSliceFeatureFlag() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    /**
     * The plain index is created and mapped after the slice-enabled one, so the slice-enabled index registers the shared mapping
     * first. This is the order of the issue.
     */
    public void testPlainIndexCreatedAfterSliceEnabledIndexDoesNotRequireRouting() {
        createSliceEnabledIndex("sliced");
        indexWithSlice("sliced", "1");
        createPlainIndex("plain");
        indexWithoutRouting("plain", "2");
        assertRoutingRequirements();
    }

    /**
     * The reverse order: the plain index registers the shared mapping first, which must not take the required routing away from the
     * slice-enabled index.
     */
    public void testPlainIndexCreatedBeforeSliceEnabledIndexDoesNotRequireRouting() {
        createPlainIndex("plain");
        indexWithoutRouting("plain", "2");
        createSliceEnabledIndex("sliced");
        indexWithSlice("sliced", "1");
        assertRoutingRequirements();
    }

    /**
     * After a full restart the mapping is read back from disk, from its source alone, and routing must still be required.
     */
    public void testRequiredRoutingSurvivesFullRestart() throws Exception {
        createSliceEnabledIndex("sliced");
        indexWithSlice("sliced", "1");
        assertThat(indexMetadata("sliced").mapping().routingRequired(), equalTo(true));

        internalCluster().fullRestart();
        ensureGreen("sliced");

        assertRequiresRouting(indexMetadata("sliced"));
    }

    private void assertRoutingRequirements() {
        IndexMetadata sliced = indexMetadata("sliced");
        IndexMetadata plain = indexMetadata("plain");
        // both indices got the same dynamic mapping from the same document, but the slice-enabled index requires routing
        assertThat(sliced.mapping().routingRequired(), equalTo(true));
        assertThat(plain.mapping().routingRequired(), equalTo(false));
        assertThat(sliced.mapping(), not(sameInstance(plain.mapping())));

        // the plain index accepts unrouted reads and writes, before and after the mapping was shared
        assertThat(client().get(new GetRequest("plain", "2")).actionGet().isExists(), equalTo(true));
        indexWithoutRouting("plain", "3");
        assertThat(client().get(new GetRequest("plain", "3")).actionGet().isExists(), equalTo(true));

        assertRequiresRouting(sliced);
    }

    /**
     * Requests on a slice-enabled index are rejected for the missing {@code _slice} before any routing check, so the routing
     * derived from the metadata is checked directly: it is what the data path uses to demand routing.
     */
    private static void assertRequiresRouting(IndexMetadata indexMetadata) {
        assertThat(indexMetadata.mapping().routingRequired(), equalTo(true));
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
        expectThrows(SliceMissingException.class, () -> indexRouting.getShard("1", null));
        assertThat(indexRouting.getShard("1", "tenant-a"), equalTo(0));
    }

    private IndexMetadata indexMetadata(String index) {
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata().getProject().index(index);
    }

    private void createSliceEnabledIndex(String index) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder().put("index.number_of_shards", 1).put(IndexSettings.SLICE_ENABLED.getKey(), true)
            )
        );
        ensureGreen(index);
    }

    private void createPlainIndex(String index) {
        assertAcked(prepareCreate(index).setSettings(Settings.builder().put("index.number_of_shards", 1)));
        ensureGreen(index);
    }

    /** Indexes the same document into every index, so all of them get the same dynamic mapping. */
    private void indexWithSlice(String index, String id) {
        client().index(new IndexRequest(index).id(id).source("company", "cat").routing("tenant-a").setRoutingFromSlice(true)).actionGet();
    }

    private void indexWithoutRouting(String index, String id) {
        client().index(new IndexRequest(index).id(id).source("company", "cat")).actionGet();
    }
}
