/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregatorSupplier;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ValuesSourceRegistryTests extends ESTestCase {

    public void testAggregatorNotFoundException() {
        AggregationContext context = mock(AggregationContext.class);
        AggregationScript.Factory mockAggScriptFactory = mock(AggregationScript.Factory.class);
        when(mockAggScriptFactory.newFactory(Mockito.any(), Mockito.any())).thenReturn(mock(AggregationScript.LeafFactory.class));
        when(context.compile(Mockito.any(), Mockito.any())).thenReturn(mockAggScriptFactory);

        ValuesSourceConfig fieldOnly = ValuesSourceConfig.resolve(
            context,
            null,
            "field",
            null,
            null,
            null,
            null,
            CoreValuesSourceType.KEYWORD
        );

        ValuesSourceConfig scriptOnly = ValuesSourceConfig.resolve(
            context,
            null,
            null,
            mockScript("fakeScript"),
            null,
            null,
            null,
            CoreValuesSourceType.KEYWORD
        );
        ValuesSourceRegistry.RegistryKey<HistogramAggregatorSupplier> key = new ValuesSourceRegistry.RegistryKey<>(
            "bogus",
            HistogramAggregatorSupplier.class
        );
        ValuesSourceRegistry registry = new ValuesSourceRegistry(Collections.singletonMap(key, Collections.emptyList()), null);
        expectThrows(IllegalArgumentException.class, () -> registry.getAggregator(key, fieldOnly));
        expectThrows(IllegalArgumentException.class, () -> registry.getAggregator(key, scriptOnly));
    }
}
