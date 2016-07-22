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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregationBuilder;

import static org.hamcrest.Matchers.equalTo;

public class PercentilesBucketTests extends AbstractBucketMetricsTestCase<PercentilesBucketPipelineAggregationBuilder> {

    @Override
    protected PercentilesBucketPipelineAggregationBuilder doCreateTestAggregatorFactory(String name, String bucketsPath) {
        PercentilesBucketPipelineAggregationBuilder factory = new PercentilesBucketPipelineAggregationBuilder(name, bucketsPath);
        if (randomBoolean()) {
            int numPercents = randomIntBetween(1, 20);
            double[] percents = new double[numPercents];
            for (int i = 0; i < numPercents; i++) {
                percents[i] = randomDoubleBetween(0.0, 100.0, false);
            }
            factory.percents(percents);
        }
        return factory;
    }

    public void testPercentsFromMixedArray() throws Exception {
        String content = XContentFactory.jsonBuilder()
            .startObject()
                .field("buckets_path", "test")
                .array("percents", 0, 20.0, 50, 75.99)
            .endObject()
            .string();

        XContentParser parser = XContentFactory.xContent(content).createParser(content);
        QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
        parser.nextToken(); // skip object start

        PercentilesBucketPipelineAggregationBuilder builder = (PercentilesBucketPipelineAggregationBuilder) aggParsers
            .pipelineParser(PercentilesBucketPipelineAggregationBuilder.NAME, parseFieldMatcher)
            .parse("test", parseContext);

        assertThat(builder.percents(), equalTo(new double[]{0.0, 20.0, 50.0, 75.99}));
    }
}
