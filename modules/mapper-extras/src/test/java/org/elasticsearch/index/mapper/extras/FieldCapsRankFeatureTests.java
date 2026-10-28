/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesBuilder;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class FieldCapsRankFeatureTests extends ESIntegTestCase {
    private final String INDEX = "index-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MapperExtrasPlugin.class);
        return plugins;
    }

    @Before
    public void setUpIndices() {
        assertAcked(
            prepareCreate(INDEX).setWaitForActiveShards(ActiveShardCount.ALL)
                .setSettings(indexSettings())
                .setMapping("fooRank", "type=rank_feature", "barRank", "type=rank_feature")
        );
    }

    public void testRankFeatureInIndex() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX).setFields("*").setincludeEmptyFields(false).get();
        assertFalse(response.get().containsKey("fooRank"));
        assertFalse(response.get().containsKey("barRank"));
        prepareIndex(INDEX).setSource("fooRank", 8).setSource("barRank", 8).get();
        refresh(INDEX);

        response = client().prepareFieldCaps(INDEX).setFields("*").setincludeEmptyFields(false).get();
        assertEquals(1, response.getIndices().length);
        assertEquals(response.getIndices()[0], INDEX);
        assertThat(response.get(), Matchers.hasKey("fooRank"));
        // Check the capabilities for the 'fooRank' field.
        Map<String, FieldCapabilities> fooRankField = response.getField("fooRank");
        assertEquals(1, fooRankField.size());
        assertThat(fooRankField, Matchers.hasKey("rank_feature"));
        assertEquals(fieldCapabilities("fooRank"), fooRankField.get("rank_feature"));
    }

    public void testRankFeatureInIndexAfterRestart() throws Exception {
        prepareIndex(INDEX).setSource("fooRank", 8).get();
        internalCluster().fullRestart();
        ensureGreen(INDEX);

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX).setFields("*").setincludeEmptyFields(false).get();

        assertEquals(1, response.getIndices().length);
        assertEquals(response.getIndices()[0], INDEX);
        assertThat(response.get(), Matchers.hasKey("fooRank"));
        // Check the capabilities for the 'fooRank' field.
        Map<String, FieldCapabilities> fooRankField = response.getField("fooRank");
        assertEquals(1, fooRankField.size());
        assertThat(fooRankField, Matchers.hasKey("rank_feature"));
        assertEquals(fieldCapabilities("fooRank"), fooRankField.get("rank_feature"));
    }

    public void testAllRankFeatureReturnedIfOneIsPresent() {
        prepareIndex(INDEX).setSource("fooRank", 8).get();
        refresh(INDEX);

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX).setFields("*").setincludeEmptyFields(false).get();

        assertEquals(1, response.getIndices().length);
        assertEquals(response.getIndices()[0], INDEX);
        assertThat(response.get(), Matchers.hasKey("fooRank"));
        // Check the capabilities for the 'fooRank' field.
        Map<String, FieldCapabilities> fooRankField = response.getField("fooRank");
        assertEquals(1, fooRankField.size());
        assertThat(fooRankField, Matchers.hasKey("rank_feature"));
        assertEquals(fieldCapabilities("fooRank"), fooRankField.get("rank_feature"));
        assertThat(response.get(), Matchers.hasKey("barRank"));
        // Check the capabilities for the 'barRank' field.
        Map<String, FieldCapabilities> barRankField = response.getField("barRank");
        assertEquals(1, barRankField.size());
        assertThat(barRankField, Matchers.hasKey("rank_feature"));
        assertEquals(fieldCapabilities("barRank"), barRankField.get("rank_feature"));
    }

    private static FieldCapabilities fieldCapabilities(String fieldName) {
        return new FieldCapabilitiesBuilder(fieldName, "rank_feature").isAggregatable(false).build();
    }
}
