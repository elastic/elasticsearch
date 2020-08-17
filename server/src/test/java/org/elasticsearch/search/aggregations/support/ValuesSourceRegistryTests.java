/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregatorSupplier;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ValuesSourceRegistryTests extends ESTestCase {

    public void testAggregatorNotFoundException() {
        final QueryShardContext queryShardContext = mock(QueryShardContext.class);
        final AggregationScript.Factory mockAggScriptFactory = mock(AggregationScript.Factory.class);
        when(mockAggScriptFactory.newFactory(Mockito.any(), Mockito.any())).thenReturn(mock(AggregationScript.LeafFactory.class));
        when(queryShardContext.compile(Mockito.any(), Mockito.any())).thenReturn(mockAggScriptFactory);

        ValuesSourceConfig fieldOnly = ValuesSourceConfig.resolve(
            queryShardContext,
            null,
            "field",
            null,
            null,
            null,
            null,
            CoreValuesSourceType.BYTES
        );

        ValuesSourceConfig scriptOnly = ValuesSourceConfig.resolve(
            queryShardContext,
            null,
            null,
            mockScript("fakeScript"),
            null,
            null,
            null,
            CoreValuesSourceType.BYTES
        );
        ValuesSourceRegistry.RegistryKey key = new ValuesSourceRegistry.RegistryKey("bogus", HistogramAggregatorSupplier.class);
        ValuesSourceRegistry registry = new ValuesSourceRegistry(
            Map.of(key, List.of()),
            null
        );
        expectThrows(IllegalArgumentException.class, () -> registry.getAggregator(key, fieldOnly));
        expectThrows(IllegalArgumentException.class, () -> registry.getAggregator(key, scriptOnly));
    }
}
