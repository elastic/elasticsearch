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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregator.ExecutionMode;

public class DiversifiedSamplerTests extends BaseAggregationTestCase<DiversifiedAggregationBuilder> {

    @Override
    protected final DiversifiedAggregationBuilder createTestAggregatorBuilder() {
        DiversifiedAggregationBuilder factory = new DiversifiedAggregationBuilder("foo");
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
        if (randomBoolean()) {
            factory.maxDocsPerValue(randomIntBetween(1, 1000));
        }
        if (randomBoolean()) {
            factory.shardSize(randomIntBetween(1, 1000));
        }
        if (randomBoolean()) {
            factory.executionHint(randomFrom(ExecutionMode.values()).toString());
        }
        return factory;
    }

}
