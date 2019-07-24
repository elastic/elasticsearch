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
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridTests;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridTests;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobalTests;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalAutoDateHistogramTests;
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
import org.elasticsearch.search.aggregations.metrics.InternalAvgTests;
import org.elasticsearch.search.aggregations.metrics.InternalCardinalityTests;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStatsTests;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBoundsTests;
import org.elasticsearch.search.aggregations.metrics.InternalGeoCentroidTests;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentilesRanksTests;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentilesTests;
import org.elasticsearch.search.aggregations.metrics.InternalMaxTests;
import org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviationTests;
import org.elasticsearch.search.aggregations.metrics.InternalMinTests;
import org.elasticsearch.search.aggregations.metrics.InternalScriptedMetricTests;
import org.elasticsearch.search.aggregations.metrics.InternalStatsBucketTests;
import org.elasticsearch.search.aggregations.metrics.InternalStatsTests;
import org.elasticsearch.search.aggregations.metrics.InternalSumTests;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentilesRanksTests;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentilesTests;
import org.elasticsearch.search.aggregations.metrics.InternalTopHitsTests;
import org.elasticsearch.search.aggregations.metrics.InternalValueCountTests;
import org.elasticsearch.search.aggregations.metrics.InternalWeightedAvgTests;
import org.elasticsearch.search.aggregations.pipeline.InternalBucketMetricValueTests;
import org.elasticsearch.search.aggregations.pipeline.InternalDerivativeTests;
import org.elasticsearch.search.aggregations.pipeline.InternalExtendedStatsBucketTests;
import org.elasticsearch.search.aggregations.pipeline.InternalPercentilesBucketTests;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValueTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
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

    private static final List<InternalAggregationTestCase<?>> aggsTests = List.of(
            new InternalCardinalityTests(),
            new InternalTDigestPercentilesTests(),
            new InternalTDigestPercentilesRanksTests(),
            new InternalHDRPercentilesTests(),
            new InternalHDRPercentilesRanksTests(),
            new InternalPercentilesBucketTests(),
            new InternalMinTests(),
            new InternalMaxTests(),
            new InternalAvgTests(),
            new InternalWeightedAvgTests(),
            new InternalSumTests(),
            new InternalValueCountTests(),
            new InternalSimpleValueTests(),
            new InternalDerivativeTests(),
            new InternalBucketMetricValueTests(),
            new InternalStatsTests(),
            new InternalStatsBucketTests(),
            new InternalExtendedStatsTests(),
            new InternalExtendedStatsBucketTests(),
            new InternalGeoBoundsTests(),
            new InternalGeoCentroidTests(),
            new InternalHistogramTests(),
            new InternalDateHistogramTests(),
            new InternalAutoDateHistogramTests(),
            new LongTermsTests(),
            new DoubleTermsTests(),
            new StringTermsTests(),
            new InternalMissingTests(),
            new InternalNestedTests(),
            new InternalReverseNestedTests(),
            new InternalGlobalTests(),
            new InternalFilterTests(),
            new InternalSamplerTests(),
            new GeoHashGridTests(),
            new GeoTileGridTests(),
            new InternalRangeTests(),
            new InternalDateRangeTests(),
            new InternalGeoDistanceTests(),
            new InternalFiltersTests(),
            new InternalAdjacencyMatrixTests(),
            new SignificantLongTermsTests(),
            new SignificantStringTermsTests(),
            new InternalScriptedMetricTests(),
            new InternalBinaryRangeTests(),
            new InternalTopHitsTests(),
            new InternalCompositeTests(),
            new InternalMedianAbsoluteDeviationTests());

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(InternalAggregationTestCase.getDefaultNamedXContents());
    }

    @Before
    public void init() throws Exception {
        for (InternalAggregationTestCase<?> aggsTest : aggsTests) {
            if (aggsTest instanceof InternalMultiBucketAggregationTestCase) {
                // Lower down the number of buckets generated by multi bucket aggregation tests in
                // order to avoid too many aggregations to be created.
                ((InternalMultiBucketAggregationTestCase<?>) aggsTest).setMaxNumberOfBuckets(3);
            }
            aggsTest.setUp();
        }
    }

    @After
    public void cleanUp() throws Exception {
        for (InternalAggregationTestCase<?> aggsTest : aggsTests) {
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
        BytesReference originalBytes = BytesReference.bytes(builder);
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
            InternalAggregationTestCase<?> testCase = randomFrom(aggsTests);
            if (testCase instanceof InternalMultiBucketAggregationTestCase) {
                InternalMultiBucketAggregationTestCase<?> multiBucketAggTestCase = (InternalMultiBucketAggregationTestCase<?>) testCase;
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
                InternalSingleBucketAggregationTestCase<?> singleBucketAggTestCase = (InternalSingleBucketAggregationTestCase<?>) testCase;
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
