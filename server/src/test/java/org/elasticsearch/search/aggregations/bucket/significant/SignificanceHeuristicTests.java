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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.GND;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.PercentageScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SignificanceHeuristicTests extends ESTestCase {
    static class SignificantTermsTestSearchContext extends TestSearchContext {

        SignificantTermsTestSearchContext() {
            super(null);
        }

        @Override
        public int numberOfShards() {
            return 1;
        }

        @Override
        public SearchShardTarget shardTarget() {
            return new SearchShardTarget("no node, this is a unit test", new ShardId("no index, this is a unit test", "_na_", 0),
                null, OriginalIndices.NONE);
        }
    }

    // test that stream output can actually be read - does not replace bwc test
    public void testStreamResponse() throws Exception {
        Version version = randomVersion(random());
        InternalMappedSignificantTerms<?, ?> sigTerms = getRandomSignificantTerms(getRandomSignificanceheuristic());

        // write
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(version);
        out.writeNamedWriteable(sigTerms);

        // read
        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        StreamInput in = new InputStreamStreamInput(inBuffer);
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList()); // populates the registry through side effects
        NamedWriteableRegistry registry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        in = new NamedWriteableAwareStreamInput(in, registry);
        in.setVersion(version);
        InternalMappedSignificantTerms<?, ?> read = (InternalMappedSignificantTerms<?, ?>) in.readNamedWriteable(InternalAggregation.class);

        assertEquals(sigTerms.significanceHeuristic, read.significanceHeuristic);
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
            return new SignificantLongTerms("some_name", 1, 1, emptyList(), null, DocValueFormat.RAW, 10, 20, heuristic,
                    singletonList(bucket));
        } else {
            SignificantStringTerms.Bucket bucket = new SignificantStringTerms.Bucket(new BytesRef("someterm"), 1, 2, 3, 4,
                    InternalAggregations.EMPTY, DocValueFormat.RAW, randomDoubleBetween(0, 100, true));
            return new SignificantStringTerms("some_name", 1, 1, emptyList(), null, DocValueFormat.RAW, 10, 20, heuristic,
                    singletonList(bucket));
        }
    }

    public static SignificanceHeuristic getRandomSignificanceheuristic() {
        List<SignificanceHeuristic> heuristics = new ArrayList<>();
        heuristics.add(new JLHScore());
        heuristics.add(new MutualInformation(randomBoolean(), randomBoolean()));
        heuristics.add(new GND(randomBoolean()));
        heuristics.add(new ChiSquare(randomBoolean(), randomBoolean()));
        return heuristics.get(randomInt(3));
    }

    public void testReduce() {
        List<InternalAggregation> aggs = createInternalAggregations();
        InternalAggregation.ReduceContext context = new InternalAggregation.ReduceContext(null, null, true);
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

    // Create aggregations as they might come from three different shards and return as list.
    private List<InternalAggregation> createInternalAggregations() {
        SignificanceHeuristic significanceHeuristic = getRandomSignificanceheuristic();
        TestAggFactory<?, ?> factory = randomBoolean() ? new StringTestAggFactory() : new LongTestAggFactory();

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
            return new SignificantStringTerms("sig_terms", 2, -1, emptyList(),
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
            return new SignificantLongTerms("sig_terms", 2, -1, new ArrayList<PipelineAggregator>(), emptyMap(), DocValueFormat.RAW,
                    subsetSize, supersetSize, significanceHeuristic, buckets);
        }

        @Override
        SignificantLongTerms.Bucket createBucket(long subsetDF, long subsetSize, long supersetDF, long supersetSize, long label) {
            return new SignificantLongTerms.Bucket(subsetDF, subsetSize, supersetDF, supersetSize, label, InternalAggregations.EMPTY,
                    DocValueFormat.RAW, 0);
        }
    }

    // test that
    // 1. The output of the builders can actually be parsed
    // 2. The parser does not swallow parameters after a significance heuristic was defined
    public void testBuilderAndParser() throws Exception {
        // test jlh with string
        assertTrue(parseFromString("\"jlh\":{}") instanceof JLHScore);
        // test gnd with string
        assertTrue(parseFromString("\"gnd\":{}") instanceof GND);
        // test mutual information with string
        boolean includeNegatives = randomBoolean();
        boolean backgroundIsSuperset = randomBoolean();
        String mutual = "\"mutual_information\":{\"include_negatives\": " + includeNegatives + ", \"background_is_superset\":"
                + backgroundIsSuperset + "}";
        assertEquals(new MutualInformation(includeNegatives, backgroundIsSuperset),
                parseFromString(mutual));
        String chiSquare = "\"chi_square\":{\"include_negatives\": " + includeNegatives + ", \"background_is_superset\":"
                + backgroundIsSuperset + "}";
        assertEquals(new ChiSquare(includeNegatives, backgroundIsSuperset),
                parseFromString(chiSquare));

        // test with builders
        assertThat(parseFromBuilder(new JLHScore()), instanceOf(JLHScore.class));
        assertThat(parseFromBuilder(new GND(backgroundIsSuperset)), instanceOf(GND.class));
        assertEquals(new MutualInformation(includeNegatives, backgroundIsSuperset),
                parseFromBuilder(new MutualInformation(includeNegatives, backgroundIsSuperset)));
        assertEquals(new ChiSquare(includeNegatives, backgroundIsSuperset),
                parseFromBuilder(new ChiSquare(includeNegatives, backgroundIsSuperset)));

        // test exceptions
        String expectedError = "unknown field [unknown_field]";
        checkParseException("\"mutual_information\":{\"include_negatives\": false, \"unknown_field\": false}", expectedError);
        checkParseException("\"chi_square\":{\"unknown_field\": true}", expectedError);
        checkParseException("\"jlh\":{\"unknown_field\": true}", expectedError);
        checkParseException("\"gnd\":{\"unknown_field\": true}", expectedError);
    }

    protected void checkParseException(String faultyHeuristicDefinition, String expectedError) throws IOException {

        try (XContentParser stParser = createParser(JsonXContent.jsonXContent,
                    "{\"field\":\"text\", " + faultyHeuristicDefinition + ",\"min_doc_count\":200}")) {
            stParser.nextToken();
            SignificantTermsAggregationBuilder.parse("testagg", stParser);
            fail();
        } catch (XContentParseException e) {
            assertThat(e.getMessage(), containsString(expectedError));
        }
    }

    protected SignificanceHeuristic parseFromBuilder(SignificanceHeuristic significanceHeuristic) throws IOException {
        SignificantTermsAggregationBuilder stBuilder = significantTerms("testagg");
        stBuilder.significanceHeuristic(significanceHeuristic).field("text").minDocCount(200);
        XContentBuilder stXContentBuilder = XContentFactory.jsonBuilder();
        stBuilder.internalXContent(stXContentBuilder, null);
        XContentParser stParser = createParser(JsonXContent.jsonXContent, Strings.toString(stXContentBuilder));
        return parseSignificanceHeuristic(stParser);
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

    protected SignificanceHeuristic parseFromString(String heuristicString) throws IOException {
        try (XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"text\", " + heuristicString + ", \"min_doc_count\":200}")) {
            return parseSignificanceHeuristic(stParser);
        }
    }

    void testBackgroundAssertions(SignificanceHeuristic heuristicIsSuperset, SignificanceHeuristic heuristicNotSuperset) {
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

    void testAssertions(SignificanceHeuristic heuristic) {
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

    public void testAssertions() throws Exception {
        testBackgroundAssertions(new MutualInformation(true, true), new MutualInformation(true, false));
        testBackgroundAssertions(new ChiSquare(true, true), new ChiSquare(true, false));
        testBackgroundAssertions(new GND(true), new GND(false));
        testAssertions(new PercentageScore());
        testAssertions(new JLHScore());
    }

    public void testBasicScoreProperties() {
        basicScoreProperties(new JLHScore(), true);
        basicScoreProperties(new GND(true), true);
        basicScoreProperties(new PercentageScore(), true);
        basicScoreProperties(new MutualInformation(true, true), false);
        basicScoreProperties(new ChiSquare(true, true), false);
    }

    public void basicScoreProperties(SignificanceHeuristic heuristic, boolean test0) {
        assertThat(heuristic.getScore(1, 1, 1, 3), greaterThan(0.0));
        assertThat(heuristic.getScore(1, 1, 2, 3), lessThan(heuristic.getScore(1, 1, 1, 3)));
        assertThat(heuristic.getScore(1, 1, 3, 4), lessThan(heuristic.getScore(1, 1, 2, 4)));
        if (test0) {
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

    public void testScoreMutual() throws Exception {
        SignificanceHeuristic heuristic = new MutualInformation(true, true);
        assertThat(heuristic.getScore(1, 1, 1, 3), greaterThan(0.0));
        assertThat(heuristic.getScore(1, 1, 2, 3), lessThan(heuristic.getScore(1, 1, 1, 3)));
        assertThat(heuristic.getScore(2, 2, 2, 4), equalTo(1.0));
        assertThat(heuristic.getScore(0, 2, 2, 4), equalTo(1.0));
        assertThat(heuristic.getScore(2, 2, 4, 4), equalTo(0.0));
        assertThat(heuristic.getScore(1, 2, 2, 4), equalTo(0.0));
        assertThat(heuristic.getScore(3, 6, 9, 18), equalTo(0.0));

        double score = 0.0;
        try {
            long a = randomLong();
            long b = randomLong();
            long c = randomLong();
            long d = randomLong();
            score = heuristic.getScore(a, b, c, d);
        } catch (IllegalArgumentException e) {
        }
        assertThat(score, lessThanOrEqualTo(1.0));
        assertThat(score, greaterThanOrEqualTo(0.0));
        heuristic = new MutualInformation(false, true);
        assertThat(heuristic.getScore(0, 1, 2, 3), equalTo(Double.NEGATIVE_INFINITY));

        heuristic = new MutualInformation(true, false);
        score = heuristic.getScore(2, 3, 1, 4);
        assertThat(score, greaterThanOrEqualTo(0.0));
        assertThat(score, lessThanOrEqualTo(1.0));
        score = heuristic.getScore(1, 4, 2, 3);
        assertThat(score, greaterThanOrEqualTo(0.0));
        assertThat(score, lessThanOrEqualTo(1.0));
        score = heuristic.getScore(1, 3, 4, 4);
        assertThat(score, greaterThanOrEqualTo(0.0));
        assertThat(score, lessThanOrEqualTo(1.0));
    }

    public void testGNDCornerCases() throws Exception {
        GND gnd = new GND(true);
        //term is only in the subset, not at all in the other set but that is because the other set is empty.
        // this should actually not happen because only terms that are in the subset are considered now,
        // however, in this case the score should be 0 because a term that does not exist cannot be relevant...
        assertThat(gnd.getScore(0, randomIntBetween(1, 2), 0, randomIntBetween(2,3)), equalTo(0.0));
        // the terms do not co-occur at all - should be 0
        assertThat(gnd.getScore(0, randomIntBetween(1, 2), randomIntBetween(2, 3), randomIntBetween(5,6)), equalTo(0.0));
        // comparison between two terms that do not exist - probably not relevant
        assertThat(gnd.getScore(0, 0, 0, randomIntBetween(1,2)), equalTo(0.0));
        // terms co-occur perfectly - should be 1
        assertThat(gnd.getScore(1, 1, 1, 1), equalTo(1.0));
        gnd = new GND(false);
        assertThat(gnd.getScore(0, 0, 0, 0), equalTo(0.0));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents());
    }
}
