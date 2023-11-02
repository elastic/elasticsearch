/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TermsAggregatorFactoryTests extends ESTestCase {
    public void testPickEmpty() {
        AggregatorFactories empty = mock(AggregatorFactories.class);
        when(empty.countAggregators()).thenReturn(0);
        assertThat(
            TermsAggregatorFactory.pickSubAggColectMode(empty, randomInt(), randomInt()),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST)
        );
    }

    public void testPickNonEmpty() {
        AggregatorFactories nonEmpty = mock(AggregatorFactories.class);
        when(nonEmpty.countAggregators()).thenReturn(1);
        assertThat(
            TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, Integer.MAX_VALUE, -1),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST)
        );
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, -1), equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, 5), equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, 10), equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, 100), equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 1, 2), equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 1, 100), equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
    }

    public void testSelectDefaultExecutionMode() {
        var context = mock(AggregationContext.class);
        when(context.getIndexSettings()).thenReturn(createIndexSettings(null));
        assertThat(TermsAggregatorFactory.selectDefaultExecutionMode(context), equalTo(ExecutionMode.GLOBAL_ORDINALS));
    }

    public void testSelectDefaultExecutionModeTsdbMap() {
        var context = mock(AggregationContext.class);
        when(context.nowInMillis()).thenReturn(1698954974000L);
        var settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field1")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2024-01-01T00:00:00.00Z")
            .build();
        when(context.getIndexSettings()).thenReturn(createIndexSettings(settings));
        assertThat(TermsAggregatorFactory.selectDefaultExecutionMode(context), equalTo(ExecutionMode.MAP));
    }

    public void testSelectDefaultExecutionModeTsdbGlobalOrdinals() {
        var context = mock(AggregationContext.class);
        when(context.nowInMillis()).thenReturn(1698954974000L);
        var settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field1")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2023-01-01T00:00:00.00Z")
            .build();
        when(context.getIndexSettings()).thenReturn(createIndexSettings(settings));
        assertThat(TermsAggregatorFactory.selectDefaultExecutionMode(context), equalTo(ExecutionMode.GLOBAL_ORDINALS));
    }

    private IndexSettings createIndexSettings(Settings additionalSettings) {
        var settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
        if (additionalSettings != null) {
            settings.put(additionalSettings);
        }
        return new IndexSettings(
            IndexMetadata.builder("_index")
                .settings(settings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            Settings.EMPTY
        );
    }
}
