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

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksAggregationBuilder;

public class PercentileRanksTests extends BaseAggregationTestCase<PercentileRanksAggregationBuilder> {

    @Override
    protected PercentileRanksAggregationBuilder createTestAggregatorBuilder() {
        PercentileRanksAggregationBuilder factory = new PercentileRanksAggregationBuilder(randomAsciiOfLengthBetween(1, 20));
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }
        int valuesSize = randomIntBetween(1, 20);
        double[] values = new double[valuesSize];
        for (int i = 0; i < valuesSize; i++) {
            values[i] = randomDouble() * 100;
        }
        factory.values(values);
        if (randomBoolean()) {
            factory.numberOfSignificantValueDigits(randomIntBetween(0, 5));
        }
        if (randomBoolean()) {
            factory.compression(randomIntBetween(1, 50000));
        }
        String field = randomNumericField();
        int randomFieldBranch = randomInt(3);
        switch (randomFieldBranch) {
        case 0:
            factory.field(field);
            break;
        case 1:
            factory.field(field);
            factory.script(new Script("_value + 1"));
            break;
        case 2:
            factory.script(new Script("doc[" + field + "] + 1"));
            break;
        }
        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        return factory;
    }

}
