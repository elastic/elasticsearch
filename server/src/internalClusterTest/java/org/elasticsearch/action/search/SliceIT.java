/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.index.IndexVersionUtils.randomVersionOnOrAfter;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that slice results are consistent depending on index version.
 * Indices created after SHARD_OBLIVIOUS_SLICING do not map slices to shards,
 * so the set of documents that belong to each slice changes. But we want to
 * be sure that the map of documents to slices is stable aside from that
 * consideration, so that we can rely on the union of all slices producing the
 * same results as an unsliced query regardless of when the slice was created.
 */
public class SliceIT extends ESIntegTestCase {
    // Tests create indices with older versions
    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    // port of scroll/12_slices/Sliced scroll
    public void testSlicedScrollOld() {
        sliceDocMap(true, false);
    }

    // port of scroll/12_slices/Sliced scroll with doc values
    public void testSlicedScrollDocValuesOld() {
        sliceDocMap(true, true);
    }

    public void testSlicedScrollNew() {
        sliceDocMap(false, false);
    }

    public void testSlicedScrollDocValuesNew() {
        sliceDocMap(false, true);
    }

    private void sliceDocMap(boolean old, boolean docValues) {
        var indexVersion = randomVersionOnOrAfter(IndexVersions.SHARD_OBLIVIOUS_SLICING);
        if (old) {
            indexVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.SHARD_OBLIVIOUS_SLICING);
        }
        logger.info("--> using index version: {}", indexVersion);

        final var indexName = "sliced_scroll";
        final var shards = 5;
        final var slices = 2;
        final var docs = 4;

        createIndex(indexName, indexSettings(indexVersion, shards, 1).build());
        for (int i = 0; i < docs; i++) {
            index(indexName, Integer.toString(i), Map.of("foo", i));
        }
        refresh(indexName);

        final var results = new HashMap<Integer, Set<String>>();
        for (int i = 0; i < slices; i++) {
            final var sliceBuilder = docValues ? new SliceBuilder("foo", i, slices) : new SliceBuilder(i, slices);

            final var result = prepareSearch(indexName).setScroll(TimeValue.ONE_MINUTE).slice(sliceBuilder).get();
            assertThat(result.getSuccessfulShards(), equalTo(shards));

            final var shard = i;
            final var scrollId = result.getScrollId();
            result.getHits().forEach(hit -> results.computeIfAbsent(shard, HashSet::new).add(hit.getId()));
            result.decRef();

            clearScroll(scrollId);
        }

        if (old) {
            assertThat(results, equalTo(Map.of(0, Set.of("0", "2", "3"), 1, Set.of("1"))));
        } else {
            if (docValues) {
                assertThat(results, equalTo(Map.of(0, Set.of("0", "3"), 1, Set.of("1", "2"))));
            } else {
                assertThat(results, equalTo(Map.of(0, Set.of("3"), 1, Set.of("0", "1", "2"))));
            }
        }
    }
}
