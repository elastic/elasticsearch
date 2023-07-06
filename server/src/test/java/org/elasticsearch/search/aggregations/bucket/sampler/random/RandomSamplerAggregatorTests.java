/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notANumber;

public class RandomSamplerAggregatorTests extends AggregatorTestCase {

    private static final String NUMERIC_FIELD_NAME = "value";
    private static final String RANDOM_NUMERIC_FIELD_NAME = "random_numeric";
    private static final String KEYWORD_FIELD_NAME = "keyword";
    private static final String KEYWORD_FIELD_VALUE = "foo";
    private static final long TRUE_MIN = 2L;
    private static final long TRUE_MAX = 1005L;

    public void testAggregationSampling() throws IOException {
        double[] avgs = new double[5];
        long[] counts = new long[5];
        AtomicInteger integer = new AtomicInteger();
        do {
            testCase(RandomSamplerAggregatorTests::writeTestDocs, (InternalRandomSampler result) -> {
                counts[integer.get()] = result.getDocCount();
                if (result.getDocCount() > 0) {
                    Avg agg = result.getAggregations().get("avg");
                    assertThat(Strings.toString(result), agg.getValue(), allOf(not(notANumber()), IsFinite.isFinite()));
                    avgs[integer.get()] = agg.getValue();
                }
            },
                new AggTestConfig(
                    new RandomSamplerAggregationBuilder("my_agg").subAggregation(AggregationBuilders.avg("avg").field(NUMERIC_FIELD_NAME))
                        .setProbability(0.25),
                    longField(NUMERIC_FIELD_NAME)
                )
            );
        } while (integer.incrementAndGet() < 5);
        long avgCount = LongStream.of(counts).sum() / integer.get();
        double avgAvg = DoubleStream.of(avgs).sum() / integer.get();
        assertThat(avgCount, allOf(greaterThanOrEqualTo(20L), lessThanOrEqualTo(70L)));
        assertThat(avgAvg, closeTo(1.5, 0.5));
    }

    public void testAggregationSamplingNestedAggsScaled() throws IOException {
        // in case 0 docs get sampled, which can rarely happen
        // in case the test index has many segments.
        // subaggs should be scaled along with upper level aggs
        // sampled doc count is NOT scaled, and thus should be lower
        testCase(RandomSamplerAggregatorTests::writeTestDocs, (InternalRandomSampler result) -> {
            long sampledDocCount = result.getDocCount();
            Filter agg = result.getAggregations().get("filter_outer");
            long outerFilterDocCount = agg.getDocCount();
            Filter innerAgg = agg.getAggregations().get("filter_inner");
            long innerFilterDocCount = innerAgg.getDocCount();
            if (sampledDocCount == 0) {
                // in case 0 docs get sampled, which can rarely happen
                // in case the test index has many segments.
                assertThat(sampledDocCount, equalTo(0L));
                assertThat(innerFilterDocCount, equalTo(0L));
                assertThat(outerFilterDocCount, equalTo(0L));
            } else {
                // subaggs should be scaled along with upper level aggs
                assertThat(outerFilterDocCount, equalTo(innerFilterDocCount));
                // sampled doc count is NOT scaled, and thus should be lower
                assertThat(outerFilterDocCount, greaterThan(sampledDocCount));
            }
        },
            new AggTestConfig(
                new RandomSamplerAggregationBuilder("my_agg").subAggregation(
                    AggregationBuilders.filter("filter_outer", QueryBuilders.termsQuery(KEYWORD_FIELD_NAME, KEYWORD_FIELD_VALUE))
                        .subAggregation(
                            AggregationBuilders.filter("filter_inner", QueryBuilders.termsQuery(KEYWORD_FIELD_NAME, KEYWORD_FIELD_VALUE))
                        )
                ).setProbability(0.25),
                longField(NUMERIC_FIELD_NAME),
                keywordField(KEYWORD_FIELD_NAME)
            )
        );
    }

    public void testAggregationSamplingOptimizedMinAndMax() throws IOException {
        testCase(RandomSamplerAggregatorTests::writeTestDocsWithTrueMinMax, (InternalRandomSampler result) -> {
            Min min = result.getAggregations().get("min");
            Max max = result.getAggregations().get("max");
            assertThat(min.value(), equalTo((double) TRUE_MIN));
            assertThat(max.value(), equalTo((double) TRUE_MAX));
        },
            new AggTestConfig(
                new RandomSamplerAggregationBuilder("my_agg").subAggregation(
                    AggregationBuilders.max("max").field(RANDOM_NUMERIC_FIELD_NAME)
                ).subAggregation(AggregationBuilders.min("min").field(RANDOM_NUMERIC_FIELD_NAME)).setProbability(0.25),
                longField(RANDOM_NUMERIC_FIELD_NAME)
            )
        );
    }

    private static void writeTestDocsWithTrueMinMax(RandomIndexWriter w) throws IOException {
        for (int i = 0; i < 75; i++) {
            w.addDocument(List.of(new LongPoint(RANDOM_NUMERIC_FIELD_NAME, randomLongBetween(3, 1000))));
        }
        w.addDocument(List.of(new LongPoint(RANDOM_NUMERIC_FIELD_NAME, TRUE_MIN)));
        w.addDocument(List.of(new LongPoint(RANDOM_NUMERIC_FIELD_NAME, TRUE_MAX)));
    }

    private static void writeTestDocs(RandomIndexWriter w) throws IOException {
        for (int i = 0; i < 75; i++) {
            w.addDocument(
                List.of(
                    new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 1),
                    new KeywordFieldMapper.KeywordField(
                        KEYWORD_FIELD_NAME,
                        new BytesRef(KEYWORD_FIELD_VALUE),
                        KeywordFieldMapper.Defaults.FIELD_TYPE
                    )
                )
            );
        }
        for (int i = 0; i < 75; i++) {
            w.addDocument(
                List.of(
                    new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 2),
                    new KeywordFieldMapper.KeywordField(
                        KEYWORD_FIELD_NAME,
                        new BytesRef(KEYWORD_FIELD_VALUE),
                        KeywordFieldMapper.Defaults.FIELD_TYPE
                    )
                )
            );
        }
    }

    private static class IsFinite extends TypeSafeMatcher<Double> {
        public static Matcher<Double> isFinite() {
            return new IsFinite();
        }

        @Override
        protected boolean matchesSafely(Double item) {
            return Double.isFinite(item);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a finite double value");
        }
    }

    public void testSampleBuilder() throws Exception {
        class SampleBuilder implements AutoCloseable {

            SampleBuilder(String path, double p) throws IOException {
                writer = new BufferedWriter(new FileWriter(path));
                geometric = new FastGeometric(ESTestCase::randomInt, p);
                nextOffsetToWrite = geometric.next();
            }

            boolean maybeWrite(String line) throws IOException {
                if (offset++ == nextOffsetToWrite) {
                    writer.write(line);
                    nextOffsetToWrite += geometric.next();
                    return true;
                }
                return false;
            }

            private final BufferedWriter writer;
            private final FastGeometric geometric;
            private int nextOffsetToWrite = 0;
            private int offset = 0;

            @Override
            public void close() throws Exception {
                writer.close();
            }
        }

        class AvgCalculator {
            AvgCalculator() {}

            void add(double val) {
                sum += val;
                count++;
            }

            void add(double val, int weight) {
                sum += val * weight;
                count += weight;
            }

            double get() {
                return sum / count;
            }

            double count() {
                return count;
            }

            double sum = 0;
            int count = 0;
        }

        /**
         * Bootstrap aggregation error estimate
         *  1. Use 9 boostrap runs on the lower tier (0.2%), with each run:
         *     - For each sampled value, get a weight from Poisson(1)
         *     - Update the aggregate with the sampled value multiplied by the weight (i.e. as if there was more samples with the
         *       same value)
         *  2. Get mean, variance and skew for the agg values of the 9 bootstrap runs.
         *  3. Use formula to calculate confidence intervals.
         */
        class BootstrapAggregation {
            private final int BOOTSTRAP_COUNT = 9;
            private final Map<String, List<AvgCalculator>> bootstrapsPerName = new TreeMap<>();
            private final Map<String, Double> lower = new TreeMap<>();
            private final Map<String, Double> upper = new TreeMap<>();

            BootstrapAggregation(Collection<String> names, String bottomTier) throws IOException {
                for (String name : names) {
                    List<AvgCalculator> calculators = new ArrayList<>();
                    for (int i = 0; i < BOOTSTRAP_COUNT; i++) {
                        calculators.add(new AvgCalculator());
                    }
                    bootstrapsPerName.put(name, calculators);
                }
                load(bottomTier);
            }

            private void load(String bottomTier) throws IOException {
                try (BufferedReader br = new BufferedReader(new FileReader(bottomTier))) {
                    String line;
                    FastPoisson poisson = new FastPoisson(ESTestCase::randomInt);
                    while ((line = br.readLine()) != null) {
                        String[] tokens = line.split(" ");
                        double value = Double.parseDouble(tokens[2]);
                        var calculators = bootstrapsPerName.get(tokens[1]);
                        for (int i = 0; i < BOOTSTRAP_COUNT; i++) {
                            calculators.get(i).add(value, poisson.next());
                        }
                    }
                }
            }

            void calculateConfidenceIntervals(double alpha, Map<String, Double> sampledResults) {
                for (var entry : bootstrapsPerName.entrySet()) {
                    DescriptiveStatistics stats = new DescriptiveStatistics();
                    for (int i = 0; i < BOOTSTRAP_COUNT; i++) {
                        stats.addValue(entry.getValue().get(i).get() * 100);
                    }

                    double mean = stats.getMean();
                    double std = stats.getStandardDeviation();
                    double a = stats.getSkewness() / 6;

                    NormalDistribution normalZero = new NormalDistribution();
                    NormalDistribution normalFitted = new NormalDistribution(mean, std);

                    double count = entry.getValue().get(0).count();
                    TDistribution tDistribution = new TDistribution(count - 1);
                    // alpha = (1 - alpha) / 2;
                    alpha = normalZero.cumulativeProbability(
                        Math.sqrt(count / (count - 1)) * tDistribution.inverseCumulativeProbability((1 - alpha) / 2)
                    );

                    double z = normalFitted.inverseCumulativeProbability(sampledResults.get(entry.getKey()));
                    double zl = normalZero.inverseCumulativeProbability(alpha);
                    double zu = normalZero.inverseCumulativeProbability(1 - alpha);
                    double pl = normalZero.cumulativeProbability(z + (z + zl) / (1 - a * (z + zl)));
                    double pu = normalZero.cumulativeProbability(z + (z + zu) / (1 - a * (z + zu)));
                    lower.put(entry.getKey(), normalFitted.inverseCumulativeProbability(pl));
                    upper.put(entry.getKey(), normalFitted.inverseCumulativeProbability(pu));
                }
            }

            double getLowerConfidence(String name) {
                return lower.get(name);
            }

            double getUpperConfidence(String name) {
                return upper.get(name);
            }
        }

        final String BASE_DIR = "/Users/kkrik/IdeaProjects/elasticsearch/server/build/testrun/test/temp/";
        final String BASE_PATH = BASE_DIR + "container_cpu_usage_24h";
        final String[] TIER_PATHS = { "_1", "_0_2", "_0_04", "_0_008", "_0_002" };
        final double[] TIER_SAMPLING = { 1, 0.2, 0.04, 0.008, 0.002 };
        final boolean USE_NESTED_SAMPLING = true;

        assert TIER_SAMPLING.length == TIER_PATHS.length;
        for (int iteration = 0; iteration < 100; iteration++) {
            String tempDate = null;
            String tempName = null;

            double[] tierAppliedSampling = TIER_SAMPLING.clone();
            if (USE_NESTED_SAMPLING) {
                for (int i = 1; i < tierAppliedSampling.length; i++) {
                    tierAppliedSampling[i] /= TIER_SAMPLING[i - 1];
                }
            }

            try (
                BufferedReader br = new BufferedReader(new FileReader(BASE_PATH));
                BufferedWriter bw = new BufferedWriter(new FileWriter(BASE_PATH + TIER_PATHS[0]));
                SampleBuilder sb1 = new SampleBuilder(BASE_PATH + TIER_PATHS[1], tierAppliedSampling[1]);
                SampleBuilder sb2 = new SampleBuilder(BASE_PATH + TIER_PATHS[2], tierAppliedSampling[2]);
                SampleBuilder sb3 = new SampleBuilder(BASE_PATH + TIER_PATHS[3], tierAppliedSampling[3]);
                SampleBuilder sb4 = new SampleBuilder(BASE_PATH + TIER_PATHS[4], tierAppliedSampling[4]);
            ) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (tempDate == null) {
                        tempDate = line;
                    } else if (tempName == null) {
                        tempName = line;
                    } else {
                        String out = tempDate + " " + tempName + " " + line + "\n";
                        tempName = tempDate = null;

                        bw.write(out);
                        if (!USE_NESTED_SAMPLING) {
                            sb1.maybeWrite(out);
                            sb2.maybeWrite(out);
                            sb3.maybeWrite(out);
                            sb4.maybeWrite(out);
                        } else if (sb1.maybeWrite(out) && sb2.maybeWrite(out) && sb3.maybeWrite(out) && sb4.maybeWrite(out)) {
                        }
                    }
                }
            }

            List<Map<String, Double>> results = new ArrayList<>();
            Set<String> names = new TreeSet<>();
            for (String path : TIER_PATHS) {
                Map<String, AvgCalculator> aggs = new HashMap<>();

                try (BufferedReader br = new BufferedReader(new FileReader(BASE_PATH + path))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] tokens = line.split(" ");
                        assert tokens.length == 3;
                        aggs.computeIfAbsent(tokens[1], (s) -> new AvgCalculator()).add(Double.parseDouble(tokens[2]));
                    }
                }

                names.addAll(aggs.keySet());
                results.add(aggs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get() * 100)));
            }

            BootstrapAggregation bootstrapAggregation = new BootstrapAggregation(names, BASE_PATH + TIER_PATHS[TIER_PATHS.length - 1]);
            bootstrapAggregation.calculateConfidenceIntervals(0.67, results.get(results.size() - 1));

            double[] sampled = new double[results.size()];
            double[] lower = new double[results.size()];
            double[] upper = new double[results.size()];
            double SQRT_5 = Math.sqrt(5.0);

            try (PrintWriter writer = new PrintWriter(BASE_PATH + "_result_" + iteration)) {
                for (String key : names) {
                    for (int i = 0; i < results.size(); i++) {
                        sampled[i] = results.get(i).get(key);
                    }
                    for (int i = 1; i < results.size() - 1; i++) {
                        double delta = 1.7 * Math.abs(sampled[i] - sampled[i + 1]) / SQRT_5;
                        lower[i] = sampled[i] - delta;
                        upper[i] = sampled[i] + delta;
                    }

                    lower[results.size() - 1] = bootstrapAggregation.getLowerConfidence(key);
                    upper[results.size() - 1] = bootstrapAggregation.getUpperConfidence(key);

                    writer.printf("%30s   %.6f", key, sampled[0]);
                    for (int i = 1; i < results.size(); i++) {
                        boolean found = sampled[0] > lower[i] && sampled[0] < upper[i];
                        writer.printf("    %.6f [%.6f  %.6f] %s", sampled[i], lower[i], upper[i], found ? "Y" : "N");
                    }
                    writer.printf("%n");
                }
            }
        }
    }
}
