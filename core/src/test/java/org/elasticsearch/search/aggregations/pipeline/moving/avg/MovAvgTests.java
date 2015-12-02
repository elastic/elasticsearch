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

package org.elasticsearch.search.aggregations.pipeline.moving.avg;

import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregator.Factory;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.EwmaModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltLinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltWintersModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltWintersModel.SeasonalityType;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.LinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.SimpleModel;;

public class MovAvgTests extends BasePipelineAggregationTestCase<MovAvgPipelineAggregator.Factory> {

    @Override
    protected Factory createTestAggregatorFactory() {
        String name = randomAsciiOfLengthBetween(3, 20);
        String[] bucketsPaths = new String[1];
        bucketsPaths[0] = randomAsciiOfLengthBetween(3, 20);
        Factory factory = new Factory(name, bucketsPaths);
        if (randomBoolean()) {
            factory.format(randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(GapPolicy.values()));
        }
        if (randomBoolean()) {
            switch (randomInt(4)) {
            case 0:
                factory.model(new SimpleModel());
                factory.window(randomIntBetween(1, 100));
                break;
            case 1:
                factory.model(new LinearModel());
                factory.window(randomIntBetween(1, 100));
                break;
            case 2:
                if (randomBoolean()) {
                    factory.model(new EwmaModel());
                    factory.window(randomIntBetween(1, 100));
                } else {
                    factory.model(new EwmaModel(randomDouble()));
                    factory.window(randomIntBetween(1, 100));
                }
                break;
            case 3:
                if (randomBoolean()) {
                    factory.model(new HoltLinearModel());
                    factory.window(randomIntBetween(1, 100));
                } else {
                    factory.model(new HoltLinearModel(randomDouble(), randomDouble()));
                    factory.window(randomIntBetween(1, 100));
                }
                break;
            case 4:
            default:
                if (randomBoolean()) {
                    factory.model(new HoltWintersModel());
                    factory.window(randomIntBetween(2, 100));
                } else {
                    int period = randomIntBetween(1, 100);
                    factory.model(new HoltWintersModel(randomDouble(), randomDouble(), randomDouble(), period,
                            randomFrom(SeasonalityType.values()), randomBoolean()));
                    factory.window(randomIntBetween(2 * period, 200 * period));
                }
                break;
            }
        }
        factory.predict(randomIntBetween(1, 50));
        if (factory.model().canBeMinimized() && randomBoolean()) {
            factory.minimize(randomBoolean());
        }
        return factory;
    }

}
