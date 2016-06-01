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
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregationBuilder;

import static org.hamcrest.Matchers.equalTo;

public class ExtendedStatsBucketTests extends AbstractBucketMetricsTestCase<ExtendedStatsBucketPipelineAggregationBuilder> {

    @Override
    protected ExtendedStatsBucketPipelineAggregationBuilder doCreateTestAggregatorFactory(String name, String bucketsPath) {
        ExtendedStatsBucketPipelineAggregationBuilder factory = new ExtendedStatsBucketPipelineAggregationBuilder(name, bucketsPath);
        if (randomBoolean()) {
            factory.sigma(randomDoubleBetween(0.0, 10.0, false));
        }
        return factory;
    }

    public void testSigmaFromInt() throws Exception {
        String content = XContentFactory.jsonBuilder()
            .startObject()
                .field("sigma", 5)
                .field("buckets_path", "test")
            .endObject()
            .string();

        XContentParser parser = XContentFactory.xContent(content).createParser(content);
        QueryParseContext parseContext = new QueryParseContext(queriesRegistry, parser, parseFieldMatcher);
        parser.nextToken(); // skip object start

        ExtendedStatsBucketPipelineAggregationBuilder builder = (ExtendedStatsBucketPipelineAggregationBuilder) aggParsers
            .pipelineParser(ExtendedStatsBucketPipelineAggregator.TYPE.name(), parseFieldMatcher)
            .parse("test", parseContext);

        assertThat(builder.sigma(), equalTo(5.0));
    }
}
