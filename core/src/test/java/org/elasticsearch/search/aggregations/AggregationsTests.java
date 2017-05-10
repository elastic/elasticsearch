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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogramTests;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogramTests;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.LongTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.StringTermsTests;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStatsTests;
import org.elasticsearch.search.aggregations.metrics.InternalMaxTests;
import org.elasticsearch.search.aggregations.metrics.InternalStatsBucketTests;
import org.elasticsearch.search.aggregations.metrics.InternalStatsTests;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvgTests;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinalityTests;
import org.elasticsearch.search.aggregations.metrics.geobounds.InternalGeoBoundsTests;
import org.elasticsearch.search.aggregations.metrics.geocentroid.InternalGeoCentroidTests;
import org.elasticsearch.search.aggregations.metrics.min.InternalMinTests;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentilesRanksTests;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentilesTests;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentilesRanksTests;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentilesTests;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSumTests;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCountTests;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValueTests;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.InternalBucketMetricValueTests;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.InternalPercentilesBucketTests;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.InternalExtendedStatsBucketTests;
import org.elasticsearch.search.aggregations.pipeline.derivative.InternalDerivativeTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;

/**
 * This class tests that aggregations parsing works properly. It checks that we can parse
 * different aggregations and adds sub-aggregations where applicable.
 *
 */
public class AggregationsTests extends ESTestCase {

    private static final List<InternalAggregationTestCase> aggsTests = getAggsTests();

    private static List<InternalAggregationTestCase> getAggsTests() {
        List<InternalAggregationTestCase> aggsTests = new ArrayList<>();
        aggsTests.add(new InternalCardinalityTests());
        aggsTests.add(new InternalTDigestPercentilesTests());
        aggsTests.add(new InternalTDigestPercentilesRanksTests());
        aggsTests.add(new InternalHDRPercentilesTests());
        aggsTests.add(new InternalHDRPercentilesRanksTests());
        aggsTests.add(new InternalPercentilesBucketTests());
        aggsTests.add(new InternalMinTests());
        aggsTests.add(new InternalMaxTests());
        aggsTests.add(new InternalAvgTests());
        aggsTests.add(new InternalSumTests());
        aggsTests.add(new InternalValueCountTests());
        aggsTests.add(new InternalSimpleValueTests());
        aggsTests.add(new InternalDerivativeTests());
        aggsTests.add(new InternalBucketMetricValueTests());
        aggsTests.add(new InternalStatsTests());
        aggsTests.add(new InternalStatsBucketTests());
        aggsTests.add(new InternalExtendedStatsTests());
        aggsTests.add(new InternalExtendedStatsBucketTests());
        aggsTests.add(new InternalGeoBoundsTests());
        aggsTests.add(new InternalGeoCentroidTests());
        aggsTests.add(new InternalHistogramTests());
        aggsTests.add(new InternalDateHistogramTests());
        aggsTests.add(new LongTermsTests());
        aggsTests.add(new DoubleTermsTests());
        aggsTests.add(new StringTermsTests());
        return Collections.unmodifiableList(aggsTests);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(InternalAggregationTestCase.getNamedXContents());
    }

    @Before
    public void init() throws Exception {
        for (InternalAggregationTestCase aggsTest : aggsTests) {
            aggsTest.setUp();
        }
    }

    @After
    public void cleanUp() throws Exception {
        for (InternalAggregationTestCase aggsTest : aggsTests) {
            aggsTest.tearDown();
        }
    }

    public void testAllAggsAreBeingTested() {
        assertEquals(InternalAggregationTestCase.getNamedXContents().size(), aggsTests.size());
        Set<String> aggs = aggsTests.stream().map((testCase) -> testCase.createTestInstance().getType()).collect(Collectors.toSet());
        for (NamedXContentRegistry.Entry entry : InternalAggregationTestCase.getNamedXContents()) {
            assertTrue(aggs.contains(entry.name.getPreferredName()));
        }
    }

    public void testFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        Aggregations aggregations = createTestInstance();
        BytesReference originalBytes = toShuffledXContent(aggregations, xContentType, params, randomBoolean());
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(Aggregations.AGGREGATIONS_FIELD, parser.currentName());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            Aggregations parsedAggregations = Aggregations.fromXContent(parser);
            BytesReference parsedBytes = XContentHelper.toXContent(parsedAggregations, xContentType, randomBoolean());
            ElasticsearchAssertions.assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
        }
    }

    private static InternalAggregations createTestInstance() {
        return createTestInstance(0, 5);
    }

    private static InternalAggregations createTestInstance(final int currentDepth, final int maxDepth) {
        int numAggs = randomIntBetween(1, 5);
        List<InternalAggregation> aggs = new ArrayList<>(numAggs);
        for (int i = 0; i < numAggs; i++) {
            InternalAggregationTestCase testCase = randomFrom(aggsTests);
            if (testCase instanceof InternalMultiBucketAggregationTestCase && currentDepth < maxDepth) {
                InternalMultiBucketAggregationTestCase multiBucketAggTestCase = (InternalMultiBucketAggregationTestCase) testCase;
                multiBucketAggTestCase.subAggregationsSupplier = () -> createTestInstance(currentDepth + 1, maxDepth);
            }
            if (testCase instanceof InternalSingleBucketAggregationTestCase && currentDepth < maxDepth) {
                InternalSingleBucketAggregationTestCase singleBucketAggTestCase = (InternalSingleBucketAggregationTestCase) testCase;
                singleBucketAggTestCase.subAggregationsSupplier = () -> createTestInstance(currentDepth + 1, maxDepth);
            }
            aggs.add(testCase.createTestInstance());
        }
        return new InternalAggregations(aggs);
    }
}
