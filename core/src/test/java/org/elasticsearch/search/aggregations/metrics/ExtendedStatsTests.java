/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.junit.Test;

import java.util.Collections;

public class ExtendedStatsTests extends AbstractNumericMetricTestCase<ExtendedStatsAggregationBuilder> {

    @Override
    protected ExtendedStatsAggregationBuilder doCreateTestAggregatorFactory() {
        ExtendedStatsAggregationBuilder factory = new ExtendedStatsAggregationBuilder("foo");
        if (randomBoolean()) {
            factory.sigma(randomDoubleBetween(0.0, 10.0, true));
        }
        return factory;
    }

    @Test
    public void getStdDeviationBoundsByPath() throws Exception {
        double min = 1.0;
        double max = 4.0;

        ExtendedStats stats = new InternalExtendedStats("test", 2, min + max, min, max, Math.pow(min, 2) + Math.pow(max, 2),
            1.0, DocValueFormat.RAW, Collections.emptyList(), Collections.emptyMap());

        assertEquals(stats.getStdDeviationBound(ExtendedStats.Bounds.LOWER), stats.getProperty("std_deviation_bounds.lower"));
        assertEquals(stats.getStdDeviationBound(ExtendedStats.Bounds.UPPER), stats.getProperty("std_deviation_bounds.upper"));
    }
}
