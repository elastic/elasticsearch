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

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

public class PercentileRanksTests extends BaseAggregationTestCase<PercentileRanksAggregationBuilder> {

    @Override
    protected PercentileRanksAggregationBuilder createTestAggregatorBuilder() {
        int valuesSize = randomIntBetween(1, 20);
        double[] values = new double[valuesSize];
        for (int i = 0; i < valuesSize; i++) {
            values[i] = randomDouble() * 100;
        }
        PercentileRanksAggregationBuilder factory = new PercentileRanksAggregationBuilder(randomAlphaOfLengthBetween(1, 20), values);
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }

        if (randomBoolean()) {
            factory.numberOfSignificantValueDigits(randomIntBetween(0, 5));
        }
        if (randomBoolean()) {
            factory.compression(randomIntBetween(1, 50000));
        }
        String field = randomNumericField();
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        return factory;
    }

}
