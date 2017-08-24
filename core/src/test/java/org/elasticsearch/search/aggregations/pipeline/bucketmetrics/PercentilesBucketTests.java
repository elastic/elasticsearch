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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("name")
                    .startObject("percentiles_bucket")
                        .field("buckets_path", "test")
                        .array("percents", 0, 20.0, 50, 75.99)
                    .endObject()
                .endObject()
            .endObject();

        PercentilesBucketPipelineAggregationBuilder builder = (PercentilesBucketPipelineAggregationBuilder) parse(createParser(content));

        assertThat(builder.percents(), equalTo(new double[]{0.0, 20.0, 50.0, 75.99}));
    }

    public void testValidate() {
        AggregationBuilder singleBucketAgg = new GlobalAggregationBuilder("global");
        AggregationBuilder multiBucketAgg = new TermsAggregationBuilder("terms", ValueType.STRING);
        final List<AggregationBuilder> aggBuilders = new ArrayList<>();
        aggBuilders.add(singleBucketAgg);
        aggBuilders.add(multiBucketAgg);

        // First try to point to a non-existent agg
        final PercentilesBucketPipelineAggregationBuilder builder = new PercentilesBucketPipelineAggregationBuilder("name",
                "invalid_agg>metric");
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> builder.validate(null, aggBuilders, Collections.emptyList()));
        assertEquals(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                + " aggregation does not exist for aggregation [name]: invalid_agg>metric", ex.getMessage());

        // Now try to point to a single bucket agg
        PercentilesBucketPipelineAggregationBuilder builder2 = new PercentilesBucketPipelineAggregationBuilder("name", "global>metric");
        ex = expectThrows(IllegalArgumentException.class, () -> builder2.validate(null, aggBuilders, Collections.emptyList()));
        assertEquals("The first aggregation in " + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                + " must be a multi-bucket aggregation for aggregation [name] found :" + GlobalAggregationBuilder.class.getName()
                + " for buckets path: global>metric", ex.getMessage());

        // Now try to point to a valid multi-bucket agg (no exception should be
        // thrown)
        PercentilesBucketPipelineAggregationBuilder builder3 = new PercentilesBucketPipelineAggregationBuilder("name", "terms>metric");
        builder3.validate(null, aggBuilders, Collections.emptyList());
    }
}
