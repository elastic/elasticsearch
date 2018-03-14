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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.aggregations.bucket.adjacency.InternalAdjacencyMatrixTests;
import org.elasticsearch.search.aggregations.bucket.composite.InternalCompositeTests;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilterTests;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFiltersTests;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGridTests;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobalTests;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogramTests;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogramTests;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissingTests;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNestedTests;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNestedTests;
import org.elasticsearch.search.aggregations.bucket.range.InternalBinaryRangeTests;
import org.elasticsearch.search.aggregations.bucket.range.InternalDateRangeTests;
import org.elasticsearch.search.aggregations.bucket.range.InternalGeoDistanceTests;
import org.elasticsearch.search.aggregations.bucket.range.InternalRangeTests;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSamplerTests;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantLongTermsTests;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.LongTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.StringTermsTests;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStatsTests;
import org.elasticsearch.search.aggregations.metrics.InternalMaxTests;
import org.elasticsearch.search.aggregations.metrics.InternalMinTests;
import org.elasticsearch.search.aggregations.metrics.InternalStatsBucketTests;
import org.elasticsearch.search.aggregations.metrics.InternalStatsTests;
import org.elasticsearch.search.aggregations.metrics.InternalSumTests;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvgTests;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinalityTests;
import org.elasticsearch.search.aggregations.metrics.geobounds.InternalGeoBoundsTests;
import org.elasticsearch.search.aggregations.metrics.geocentroid.InternalGeoCentroidTests;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentilesRanksTests;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentilesTests;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentilesRanksTests;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentilesTests;
import org.elasticsearch.search.aggregations.metrics.scripted.InternalScriptedMetricTests;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHitsTests;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCountTests;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValueTests;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.InternalBucketMetricValueTests;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.InternalPercentilesBucketTests;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.InternalExtendedStatsBucketTests;
import org.elasticsearch.search.aggregations.pipeline.derivative.InternalDerivativeTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;

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
        aggsTests.add(new InternalMissingTests());
        aggsTests.add(new InternalNestedTests());
        aggsTests.add(new InternalReverseNestedTests());
        aggsTests.add(new InternalGlobalTests());
        aggsTests.add(new InternalFilterTests());
        aggsTests.add(new InternalSamplerTests());
        aggsTests.add(new InternalGeoHashGridTests());
        aggsTests.add(new InternalRangeTests());
        aggsTests.add(new InternalDateRangeTests());
        aggsTests.add(new InternalGeoDistanceTests());
        aggsTests.add(new InternalFiltersTests());
        aggsTests.add(new InternalAdjacencyMatrixTests());
        aggsTests.add(new SignificantLongTermsTests());
        aggsTests.add(new SignificantStringTermsTests());
        aggsTests.add(new InternalScriptedMetricTests());
        aggsTests.add(new InternalBinaryRangeTests());
        aggsTests.add(new InternalTopHitsTests());
        aggsTests.add(new InternalCompositeTests());
        return Collections.unmodifiableList(aggsTests);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(InternalAggregationTestCase.getDefaultNamedXContents());
    }

    @Before
    public void init() throws Exception {
        for (InternalAggregationTestCase aggsTest : aggsTests) {
            if (aggsTest instanceof InternalMultiBucketAggregationTestCase) {
                // Lower down the number of buckets generated by multi bucket aggregation tests in
                // order to avoid too many aggregations to be created.
                ((InternalMultiBucketAggregationTestCase) aggsTest).setMaxNumberOfBuckets(3);
            }
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
        assertEquals(InternalAggregationTestCase.getDefaultNamedXContents().size(), aggsTests.size());
        Set<String> aggs = aggsTests.stream().map((testCase) -> testCase.createTestInstance().getType()).collect(Collectors.toSet());
        for (NamedXContentRegistry.Entry entry : InternalAggregationTestCase.getDefaultNamedXContents()) {
            assertTrue(aggs.contains(entry.name.getPreferredName()));
        }
    }

    public void testFromXContent() throws IOException {
        parseAndAssert(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        parseAndAssert(true);
    }

    /**
     * Test that parsing works for a randomly created Aggregations object with a
     * randomized aggregation tree. The test randomly chooses an
     * {@link XContentType}, randomizes the order of the {@link XContent} fields
     * and randomly sets the `humanReadable` flag when rendering the
     * {@link XContent}.
     *
     * @param addRandomFields
     *            if set, this will also add random {@link XContent} fields to
     *            tests that the parsers are lenient to future additions to rest
     *            responses
     */
    private void parseAndAssert(boolean addRandomFields) throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        Aggregations aggregations = createTestInstance();
        BytesReference originalBytes = toShuffledXContent(aggregations, xContentType, params, randomBoolean());
        BytesReference mutated;
        if (addRandomFields) {
            /*
             * - don't insert into the root object because it should only contain the named aggregations to test
             *
             * - don't insert into the "meta" object, because we pass on everything we find there
             *
             * - we don't want to directly insert anything random into "buckets"  objects, they are used with
             * "keyed" aggregations and contain named bucket objects. Any new named object on this level should
             * also be a bucket and be parsed as such.
             *
             * - we cannot insert randomly into VALUE or VALUES objects e.g. in Percentiles, the keys need to be numeric there
             *
             * - we cannot insert into ExtendedMatrixStats "covariance" or "correlation" fields, their syntax is strict
             *
             * - exclude "key", it can be an array of objects and we need strict values
             */
            Predicate<String> excludes = path -> (path.isEmpty() || path.endsWith("aggregations")
                    || path.endsWith(Aggregation.CommonFields.META.getPreferredName())
                    || path.endsWith(Aggregation.CommonFields.BUCKETS.getPreferredName())
                    || path.endsWith(CommonFields.VALUES.getPreferredName()) || path.endsWith("covariance") || path.endsWith("correlation")
                    || path.contains(CommonFields.VALUE.getPreferredName())
                    || path.endsWith(CommonFields.KEY.getPreferredName()));
            mutated = insertRandomFields(xContentType, originalBytes, excludes, random());
        } else {
            mutated = originalBytes;
        }
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(Aggregations.AGGREGATIONS_FIELD, parser.currentName());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            Aggregations parsedAggregations = Aggregations.fromXContent(parser);
            BytesReference parsedBytes = XContentHelper.toXContent(parsedAggregations, xContentType, randomBoolean());
            ElasticsearchAssertions.assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
        }
    }

    public void testParsingExceptionOnUnknownAggregation() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("unknownAggregation");
            builder.endObject();
        }
        builder.endObject();
        BytesReference originalBytes = builder.bytes();
        try (XContentParser parser = createParser(builder.contentType().xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            ParsingException ex = expectThrows(ParsingException.class, () -> Aggregations.fromXContent(parser));
            assertEquals("Could not parse aggregation keyed as [unknownAggregation]", ex.getMessage());
        }
    }

    public final InternalAggregations createTestInstance() {
        return createTestInstance(1, 0, 5);
    }

    private static InternalAggregations createTestInstance(final int minNumAggs, final int currentDepth, final int maxDepth) {
        int numAggs = randomIntBetween(minNumAggs, 4);
        List<InternalAggregation> aggs = new ArrayList<>(numAggs);
        for (int i = 0; i < numAggs; i++) {
            InternalAggregationTestCase testCase = randomFrom(aggsTests);
            if (testCase instanceof InternalMultiBucketAggregationTestCase) {
                InternalMultiBucketAggregationTestCase multiBucketAggTestCase = (InternalMultiBucketAggregationTestCase) testCase;
                if (currentDepth < maxDepth) {
                    multiBucketAggTestCase.setSubAggregationsSupplier(
                        () -> createTestInstance(0, currentDepth + 1, maxDepth)
                    );
                } else {
                    multiBucketAggTestCase.setSubAggregationsSupplier(
                        () -> InternalAggregations.EMPTY
                    );
                }
            } else if (testCase instanceof InternalSingleBucketAggregationTestCase) {
                InternalSingleBucketAggregationTestCase singleBucketAggTestCase = (InternalSingleBucketAggregationTestCase) testCase;
                if (currentDepth < maxDepth) {
                    singleBucketAggTestCase.subAggregationsSupplier = () -> createTestInstance(0, currentDepth + 1, maxDepth);
                } else {
                    singleBucketAggTestCase.subAggregationsSupplier = () -> InternalAggregations.EMPTY;
                }
            }
            aggs.add(testCase.createTestInstance());
        }
        return new InternalAggregations(aggs);
    }
}
