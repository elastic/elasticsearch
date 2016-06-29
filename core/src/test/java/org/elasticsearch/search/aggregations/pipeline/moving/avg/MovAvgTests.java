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

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.EwmaModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltLinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltWintersModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltWintersModel.SeasonalityType;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.LinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.SimpleModel;;

public class MovAvgTests extends BasePipelineAggregationTestCase<MovAvgPipelineAggregationBuilder> {

    @Override
    protected MovAvgPipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAsciiOfLengthBetween(3, 20);
        String bucketsPath = randomAsciiOfLengthBetween(3, 20);
        MovAvgPipelineAggregationBuilder factory = new MovAvgPipelineAggregationBuilder(name, bucketsPath);
        if (randomBoolean()) {
            factory.format(randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(GapPolicy.values()));
        }
        if (randomBoolean()) {
            switch (randomInt(4)) {
            case 0:
                factory.modelBuilder(new SimpleModel.SimpleModelBuilder());
                factory.window(randomIntBetween(1, 100));
                break;
            case 1:
                factory.modelBuilder(new LinearModel.LinearModelBuilder());
                factory.window(randomIntBetween(1, 100));
                break;
            case 2:
                if (randomBoolean()) {
                    factory.modelBuilder(new EwmaModel.EWMAModelBuilder());
                    factory.window(randomIntBetween(1, 100));
                } else {
                    factory.modelBuilder(new EwmaModel.EWMAModelBuilder().alpha(randomDouble()));
                    factory.window(randomIntBetween(1, 100));
                }
                break;
            case 3:
                if (randomBoolean()) {
                    factory.modelBuilder(new HoltLinearModel.HoltLinearModelBuilder());
                    factory.window(randomIntBetween(1, 100));
                } else {
                    factory.modelBuilder(new HoltLinearModel.HoltLinearModelBuilder().alpha(randomDouble()).beta(randomDouble()));
                    factory.window(randomIntBetween(1, 100));
                }
                break;
            case 4:
            default:
                if (randomBoolean()) {
                    factory.modelBuilder(new HoltWintersModel.HoltWintersModelBuilder());
                    factory.window(randomIntBetween(2, 100));
                } else {
                    int period = randomIntBetween(1, 100);
                    factory.modelBuilder(
                            new HoltWintersModel.HoltWintersModelBuilder().alpha(randomDouble()).beta(randomDouble()).gamma(randomDouble())
                                    .period(period).seasonalityType(randomFrom(SeasonalityType.values())).pad(randomBoolean()));
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

    public void testDefaultParsing() throws Exception {
        MovAvgPipelineAggregationBuilder expected = new MovAvgPipelineAggregationBuilder("commits_moving_avg", "commits");
        String json = "{" +
            "    \"commits_moving_avg\": {" +
            "        \"moving_avg\": {" +
                "            \"buckets_path\": \"commits\"" +
            "        }" +
            "    }" +
            "}";
        XContentParser parser = XContentFactory.xContent(json).createParser(json);
        QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(expected.getName(), parser.currentName());
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(expected.type(), parser.currentName());
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        PipelineAggregationBuilder newAgg = aggParsers.pipelineParser(expected.getWriteableName(), ParseFieldMatcher.STRICT)
                .parse(expected.getName(), parseContext);
        assertSame(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertSame(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertSame(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
        assertNotNull(newAgg);
        assertNotSame(newAgg, expected);
        assertEquals(expected, newAgg);
        assertEquals(expected.hashCode(), newAgg.hashCode());
    }
}
