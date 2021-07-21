/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.InternalMappedSignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.InternalSignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.search.aggregations.AggregationBuilders.significantTerms;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Abstract test case for testing significant term heuristics
 */
public abstract class AbstractSignificanceHeuristicTestCase extends ESTestCase {

    /**
     * @return A random instance of the heuristic to test
     */
    protected abstract SignificanceHeuristic getHeuristic();

    // test that stream output can actually be read - does not replace bwc test
    public void testStreamResponse() throws Exception {
        Version version = randomVersion(random());
        InternalMappedSignificantTerms<?, ?> sigTerms = getRandomSignificantTerms(getHeuristic());

        // write
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(version);
        if (version.before(Version.V_7_8_0)) {
            sigTerms.mergePipelineTreeForBWCSerialization(PipelineAggregator.PipelineTree.EMPTY);
        }
        out.writeNamedWriteable(sigTerms);

        // read
        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        StreamInput in = new InputStreamStreamInput(inBuffer);
        // populates the registry through side effects
        in = new NamedWriteableAwareStreamInput(in, writableRegistry());
        in.setVersion(version);
        InternalMappedSignificantTerms<?, ?> read = (InternalMappedSignificantTerms<?, ?>) in.readNamedWriteable(InternalAggregation.class);

        assertEquals(sigTerms.getSignificanceHeuristic(), read.getSignificanceHeuristic());
        SignificantTerms.Bucket originalBucket = sigTerms.getBuckets().get(0);
        SignificantTerms.Bucket streamedBucket = read.getBuckets().get(0);
        assertThat(originalBucket.getKeyAsString(), equalTo(streamedBucket.getKeyAsString()));
        assertThat(originalBucket.getSupersetDf(), equalTo(streamedBucket.getSupersetDf()));
        assertThat(originalBucket.getSubsetDf(), equalTo(streamedBucket.getSubsetDf()));
        assertThat(streamedBucket.getSubsetSize(), equalTo(10L));
        assertThat(streamedBucket.getSupersetSize(), equalTo(20L));
    }

    InternalMappedSignificantTerms<?, ?> getRandomSignificantTerms(SignificanceHeuristic heuristic) {
        if (randomBoolean()) {
            SignificantLongTerms.Bucket bucket = new SignificantLongTerms.Bucket(1, 2, 3, 4, 123, InternalAggregations.EMPTY,
                    DocValueFormat.RAW, randomDoubleBetween(0, 100, true));
            return new SignificantLongTerms("some_name", 1, 1, null, DocValueFormat.RAW, 10, 20, heuristic, singletonList(bucket));
        } else {
            SignificantStringTerms.Bucket bucket = new SignificantStringTerms.Bucket(new BytesRef("someterm"), 1, 2, 3, 4,
                    InternalAggregations.EMPTY, DocValueFormat.RAW, randomDoubleBetween(0, 100, true));
            return new SignificantStringTerms("some_name", 1, 1, null, DocValueFormat.RAW, 10, 20, heuristic, singletonList(bucket));
        }
    }

    public void testReduce() {
        List<InternalAggregation> aggs = createInternalAggregations();
        InternalAggregation.ReduceContext context = InternalAggregationTestCase.emptyReduceContextBuilder().forFinalReduction();
        SignificantTerms reducedAgg = (SignificantTerms) aggs.get(0).reduce(aggs, context);
        assertThat(reducedAgg.getBuckets().size(), equalTo(2));
        assertThat(reducedAgg.getBuckets().get(0).getSubsetDf(), equalTo(8L));
        assertThat(reducedAgg.getBuckets().get(0).getSubsetSize(), equalTo(16L));
        assertThat(reducedAgg.getBuckets().get(0).getSupersetDf(), equalTo(10L));
        assertThat(reducedAgg.getBuckets().get(0).getSupersetSize(), equalTo(30L));
        assertThat(reducedAgg.getBuckets().get(1).getSubsetDf(), equalTo(8L));
        assertThat(reducedAgg.getBuckets().get(1).getSubsetSize(), equalTo(16L));
        assertThat(reducedAgg.getBuckets().get(1).getSupersetDf(), equalTo(10L));
        assertThat(reducedAgg.getBuckets().get(1).getSupersetSize(), equalTo(30L));
    }

    public void testBasicScoreProperties() {
        testBasicScoreProperties(getHeuristic(), true);
    }

    protected void testBasicScoreProperties(SignificanceHeuristic heuristic, boolean testZeroScore) {
        assertThat(heuristic.getScore(1, 1, 1, 3), greaterThan(0.0));
        assertThat(heuristic.getScore(1, 1, 2, 3), lessThan(heuristic.getScore(1, 1, 1, 3)));
        assertThat(heuristic.getScore(1, 1, 3, 4), lessThan(heuristic.getScore(1, 1, 2, 4)));
        if (testZeroScore) {
            assertThat(heuristic.getScore(0, 1, 2, 3), equalTo(0.0));
        }

        double score = 0.0;
        try {
            long a = randomLong();
            long b = randomLong();
            long c = randomLong();
            long d = randomLong();
            score = heuristic.getScore(a, b, c, d);
        } catch (IllegalArgumentException e) {
        }
        assertThat(score, greaterThanOrEqualTo(0.0));
    }

    /**
     * Testing heuristic specific assertions
     * Typically, this method would call either
     * {@link AbstractSignificanceHeuristicTestCase#testBackgroundAssertions(SignificanceHeuristic, SignificanceHeuristic)}
     * or {@link AbstractSignificanceHeuristicTestCase#testAssertions(SignificanceHeuristic)}
     * depending on which was appropriate
     */
    public abstract void testAssertions();

    public void testParseFromString() throws IOException {
        SignificanceHeuristic significanceHeuristic = getHeuristic();
        try (XContentBuilder builder = JsonXContent.contentBuilder()){
            builder.startObject()
                .field("field", "text")
                .field("min_doc_count", "200");
            significanceHeuristic.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            try (XContentParser stParser = createParser(builder)) {
                SignificanceHeuristic parsedHeuristic = parseSignificanceHeuristic(stParser);
                assertThat(significanceHeuristic, equalTo(parsedHeuristic));
            }
        }
    }

    public void testParseFromAggBuilder() throws IOException {
        SignificanceHeuristic significanceHeuristic = getHeuristic();
        SignificantTermsAggregationBuilder stBuilder = significantTerms("testagg");
        stBuilder.significanceHeuristic(significanceHeuristic).field("text").minDocCount(200);
        XContentBuilder stXContentBuilder = XContentFactory.jsonBuilder();
        stBuilder.internalXContent(stXContentBuilder, null);
        XContentParser stParser = createParser(JsonXContent.jsonXContent, Strings.toString(stXContentBuilder));
        SignificanceHeuristic parsedHeuristic = parseSignificanceHeuristic(stParser);
        assertThat(significanceHeuristic, equalTo(parsedHeuristic));
    }

    public void testParseFailure() throws IOException {
        SignificanceHeuristic significanceHeuristic = getHeuristic();
        try (XContentBuilder builder = JsonXContent.contentBuilder()){
            builder.startObject()
                .field("field", "text")
                .startObject(significanceHeuristic.getWriteableName())
                .field("unknown_field", false)
                .endObject()
                .field("min_doc_count", "200")
                .endObject();
            try (XContentParser stParser = createParser(builder)) {
                try {
                    parseSignificanceHeuristic(stParser);
                    fail("parsing the heurstic should have failed");
                } catch (XContentParseException e) {
                    assertThat(e.getMessage(), containsString("unknown field [unknown_field]"));
                }
            }
        }
    }

    // Create aggregations as they might come from three different shards and return as list.
    private List<InternalAggregation> createInternalAggregations() {
        SignificanceHeuristic significanceHeuristic = getHeuristic();
        AbstractSignificanceHeuristicTestCase.TestAggFactory<?, ?> factory = randomBoolean() ?
            new AbstractSignificanceHeuristicTestCase.StringTestAggFactory() :
            new AbstractSignificanceHeuristicTestCase.LongTestAggFactory();

        List<InternalAggregation> aggs = new ArrayList<>();
        aggs.add(factory.createAggregation(significanceHeuristic, 4, 10, 1, (f, i) -> f.createBucket(4, 4, 5, 10, 0)));
        aggs.add(factory.createAggregation(significanceHeuristic, 4, 10, 1, (f, i) -> f.createBucket(4, 4, 5, 10, 1)));
        aggs.add(factory.createAggregation(significanceHeuristic, 8, 10, 2, (f, i) -> f.createBucket(4, 4, 5, 10, i)));
        return aggs;
    }

    private abstract class TestAggFactory<A extends InternalSignificantTerms<A, B>, B extends InternalSignificantTerms.Bucket<B>> {
        final A createAggregation(SignificanceHeuristic significanceHeuristic, long subsetSize, long supersetSize, int bucketCount,
                BiFunction<TestAggFactory<?, B>, Integer, B> bucketFactory) {
            List<B> buckets = IntStream.range(0, bucketCount).mapToObj(i -> bucketFactory.apply(this, i))
                    .collect(Collectors.toList());
            return createAggregation(significanceHeuristic, subsetSize, supersetSize, buckets);
        }

        abstract A createAggregation(SignificanceHeuristic significanceHeuristic, long subsetSize, long supersetSize, List<B> buckets);

        abstract B createBucket(long subsetDF, long subsetSize, long supersetDF, long supersetSize, long label);
    }
    private class StringTestAggFactory extends TestAggFactory<SignificantStringTerms, SignificantStringTerms.Bucket> {
        @Override
        SignificantStringTerms createAggregation(SignificanceHeuristic significanceHeuristic, long subsetSize, long supersetSize,
                List<SignificantStringTerms.Bucket> buckets) {
            return new SignificantStringTerms("sig_terms", 2, -1,
                    emptyMap(), DocValueFormat.RAW, subsetSize, supersetSize, significanceHeuristic, buckets);
        }

        @Override
        SignificantStringTerms.Bucket createBucket(long subsetDF, long subsetSize, long supersetDF, long supersetSize, long label) {
            return new SignificantStringTerms.Bucket(new BytesRef(Long.toString(label).getBytes(StandardCharsets.UTF_8)), subsetDF,
                    subsetSize, supersetDF, supersetSize, InternalAggregations.EMPTY, DocValueFormat.RAW, 0);
        }
    }
    private class LongTestAggFactory extends TestAggFactory<SignificantLongTerms, SignificantLongTerms.Bucket> {
        @Override
        SignificantLongTerms createAggregation(SignificanceHeuristic significanceHeuristic, long subsetSize, long supersetSize,
                List<SignificantLongTerms.Bucket> buckets) {
            return new SignificantLongTerms("sig_terms", 2, -1, emptyMap(), DocValueFormat.RAW,
                    subsetSize, supersetSize, significanceHeuristic, buckets);
        }

        @Override
        SignificantLongTerms.Bucket createBucket(long subsetDF, long subsetSize, long supersetDF, long supersetSize, long label) {
            return new SignificantLongTerms.Bucket(subsetDF, subsetSize, supersetDF, supersetSize, label, InternalAggregations.EMPTY,
                DocValueFormat.RAW, 0);
        }
    }

    private static SignificanceHeuristic parseSignificanceHeuristic(XContentParser stParser) throws IOException {
        stParser.nextToken();
        SignificantTermsAggregationBuilder aggregatorFactory = SignificantTermsAggregationBuilder.parse("testagg", stParser);
        stParser.nextToken();
        assertThat(aggregatorFactory.getBucketCountThresholds().getMinDocCount(), equalTo(200L));
        assertThat(stParser.currentToken(), equalTo(null));
        stParser.close();
        return aggregatorFactory.significanceHeuristic();
    }

    protected void testBackgroundAssertions(SignificanceHeuristic heuristicIsSuperset, SignificanceHeuristic heuristicNotSuperset) {
        try {
            heuristicIsSuperset.getScore(2, 3, 1, 4);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("subsetFreq > supersetFreq"));
        }
        try {
            heuristicIsSuperset.getScore(1, 4, 2, 3);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("subsetSize > supersetSize"));
        }
        try {
            heuristicIsSuperset.getScore(2, 1, 3, 4);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("subsetFreq > subsetSize"));
        }
        try {
            heuristicIsSuperset.getScore(1, 2, 4, 3);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("supersetFreq > supersetSize"));
        }
        try {
            heuristicIsSuperset.getScore(1, 3, 4, 4);
            fail();
        } catch (IllegalArgumentException assertionError) {
            assertNotNull(assertionError.getMessage());
            assertTrue(assertionError.getMessage().contains("supersetFreq - subsetFreq > supersetSize - subsetSize"));
        }
        try {
            int idx = randomInt(3);
            long[] values = {1, 2, 3, 4};
            values[idx] *= -1;
            heuristicIsSuperset.getScore(values[0], values[1], values[2], values[3]);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("Frequencies of subset and superset must be positive"));
        }
        try {
            heuristicNotSuperset.getScore(2, 1, 3, 4);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("subsetFreq > subsetSize"));
        }
        try {
            heuristicNotSuperset.getScore(1, 2, 4, 3);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("supersetFreq > supersetSize"));
        }
        try {
            int idx = randomInt(3);
            long[] values = {1, 2, 3, 4};
            values[idx] *= -1;
            heuristicNotSuperset.getScore(values[0], values[1], values[2], values[3]);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("Frequencies of subset and superset must be positive"));
        }
    }

    protected void testAssertions(SignificanceHeuristic heuristic) {
        try {
            int idx = randomInt(3);
            long[] values = {1, 2, 3, 4};
            values[idx] *= -1;
            heuristic.getScore(values[0], values[1], values[2], values[3]);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("Frequencies of subset and superset must be positive"));
        }
        try {
            heuristic.getScore(1, 2, 4, 3);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("supersetFreq > supersetSize"));
        }
        try {
            heuristic.getScore(2, 1, 3, 4);
            fail();
        } catch (IllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("subsetFreq > subsetSize"));
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, false, emptyList()).getNamedXContents());
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, false, emptyList()).getNamedWriteables());
    }
}
