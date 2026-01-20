/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.SamplingService.SampleStats;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotSame;

public class SamplingServiceSampleStatsTests extends AbstractWireSerializingTestCase<SampleStats> {

    @Override
    protected Writeable.Reader<SampleStats> instanceReader() {
        return SampleStats::new;
    }

    @Override
    protected SampleStats createTestInstance() {
        SampleStats stats = new SampleStats();
        stats.samples.add(randomReasonableLong());
        stats.potentialSamples.add(randomReasonableLong());
        stats.samplesRejectedForMaxSamplesExceeded.add(randomReasonableLong());
        stats.samplesRejectedForCondition.add(randomReasonableLong());
        stats.samplesRejectedForRate.add(randomReasonableLong());
        stats.samplesRejectedForException.add(randomReasonableLong());
        stats.samplesRejectedForSize.add(randomReasonableLong());
        stats.timeSamplingInNanos.add(randomReasonableLong());
        stats.timeEvaluatingConditionInNanos.add(randomReasonableLong());
        stats.timeCompilingConditionInNanos.add(randomReasonableLong());
        stats.lastException = randomBoolean() ? null : new ElasticsearchException(randomAlphanumericOfLength(10));
        return stats;
    }

    /*
     * This is to avoid overflow errors in these tests.
     */
    private long randomReasonableLong() {
        long randomLong = randomNonNegativeLong();
        if (randomLong > Long.MAX_VALUE / 2) {
            return randomLong / 2;
        } else {
            return randomLong;
        }
    }

    @Override
    protected SampleStats mutateInstance(SampleStats instance) throws IOException {
        SampleStats mutated = instance.combine(new SampleStats());
        switch (between(0, 10)) {
            case 0 -> mutated.samples.add(1);
            case 1 -> mutated.potentialSamples.add(1);
            case 2 -> mutated.samplesRejectedForMaxSamplesExceeded.add(1);
            case 3 -> mutated.samplesRejectedForCondition.add(1);
            case 4 -> mutated.samplesRejectedForRate.add(1);
            case 5 -> mutated.samplesRejectedForException.add(1);
            case 6 -> mutated.samplesRejectedForSize.add(1);
            case 7 -> mutated.timeSamplingInNanos.add(1);
            case 8 -> mutated.timeEvaluatingConditionInNanos.add(1);
            case 9 -> mutated.timeCompilingConditionInNanos.add(1);
            case 10 -> mutated.lastException = mutated.lastException == null
                ? new ElasticsearchException(randomAlphanumericOfLength(10))
                : null;
            default -> throw new IllegalArgumentException("Should never get here");
        }
        return mutated;
    }

    public void testCombine() {
        SampleStats stats1 = createTestInstance();
        stats1.lastException = null;
        SampleStats combinedWithEmpty = stats1.combine(new SampleStats());
        assertThat(combinedWithEmpty, equalTo(stats1));
        assertNotSame(stats1, combinedWithEmpty);
        SampleStats stats2 = createTestInstance();
        SampleStats stats1CombineStats2 = stats1.combine(stats2);
        SampleStats stats2CombineStats1 = stats2.combine(stats1);
        assertThat(stats1CombineStats2, equalTo(stats2CombineStats1));
        assertThat(stats1CombineStats2.getSamples(), equalTo(stats1.getSamples() + stats2.getSamples()));
        assertThat(stats1CombineStats2.getPotentialSamples(), equalTo(stats1.getPotentialSamples() + stats2.getPotentialSamples()));
        assertThat(
            stats1CombineStats2.getSamplesRejectedForMaxSamplesExceeded(),
            equalTo(stats1.getSamplesRejectedForMaxSamplesExceeded() + stats2.getSamplesRejectedForMaxSamplesExceeded())
        );
        assertThat(
            stats1CombineStats2.getSamplesRejectedForCondition(),
            equalTo(stats1.getSamplesRejectedForCondition() + stats2.getSamplesRejectedForCondition())
        );
        assertThat(
            stats1CombineStats2.getSamplesRejectedForRate(),
            equalTo(stats1.getSamplesRejectedForRate() + stats2.getSamplesRejectedForRate())
        );
        assertThat(
            stats1CombineStats2.getSamplesRejectedForException(),
            equalTo(stats1.getSamplesRejectedForException() + stats2.getSamplesRejectedForException())
        );
        assertThat(
            stats1CombineStats2.getSamplesRejectedForSize(),
            equalTo(stats1.getSamplesRejectedForSize() + stats2.getSamplesRejectedForSize())
        );
        assertThat(
            stats1CombineStats2.getTimeSampling(),
            equalTo(TimeValue.timeValueNanos(stats1.getTimeSampling().nanos() + stats2.getTimeSampling().nanos()))
        );
        assertThat(
            stats1CombineStats2.getTimeEvaluatingCondition(),
            equalTo(TimeValue.timeValueNanos(stats1.getTimeEvaluatingCondition().nanos() + stats2.getTimeEvaluatingCondition().nanos()))
        );
        assertThat(
            stats1CombineStats2.getTimeCompilingCondition(),
            equalTo(TimeValue.timeValueNanos(stats1.getTimeCompilingCondition().nanos() + stats2.getTimeCompilingCondition().nanos()))
        );
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        /*
         * SampleStats class is only used in the user response, so there is no parser for it, meaning it can't be tested with
         * AbstractXContentSerializingTestCase.
         */
        SampleStats sampleStats = createTestInstance();
        boolean humanReadable = randomBoolean();
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            builder.humanReadable(humanReadable);
            sampleStats.toXContent(builder, EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                Map<String, Object> parserMap = parser.map();
                assertThat(parserMap.get("potential_samples"), equalTo(sampleStats.getPotentialSamples()));
                assertThat(
                    parserMap.get("samples_rejected_for_max_samples_exceeded"),
                    equalTo(sampleStats.getSamplesRejectedForMaxSamplesExceeded())
                );
                assertNumberEqualsLong(parserMap.get("samples_rejected_for_condition"), sampleStats.getSamplesRejectedForCondition());
                assertNumberEqualsLong(parserMap.get("samples_rejected_for_rate"), sampleStats.getSamplesRejectedForRate());
                assertNumberEqualsLong(parserMap.get("samples_rejected_for_exception"), sampleStats.getSamplesRejectedForException());
                assertNumberEqualsLong(parserMap.get("samples_rejected_for_size"), sampleStats.getSamplesRejectedForSize());
                assertNumberEqualsLong(parserMap.get("samples_accepted"), sampleStats.getSamples());
                assertNumberEqualsLong(
                    ((Number) parserMap.get("time_sampling_millis")).longValue(),
                    sampleStats.getTimeSampling().millis()
                );
                assertNumberEqualsLong(
                    ((Number) parserMap.get("time_compiling_condition_millis")).longValue(),
                    sampleStats.getTimeCompilingCondition().millis()
                );
                if (humanReadable) {
                    assertThat(parserMap.get("time_sampling"), equalTo(sampleStats.getTimeSampling().toHumanReadableString(1)));
                    assertThat(
                        parserMap.get("time_compiling_condition"),
                        equalTo(sampleStats.getTimeCompilingCondition().toHumanReadableString(1))
                    );
                } else {
                    assertThat(parserMap.containsKey("time_sampling"), equalTo(false));
                    assertThat(parserMap.containsKey("time_compiling_condition"), equalTo(false));
                }
                if (sampleStats.getLastException() == null) {
                    assertThat(parserMap.containsKey("last_exception"), equalTo(false));
                } else {
                    Map<String, Object> exceptionMap = (Map<String, Object>) parserMap.get("last_exception");
                    assertThat(exceptionMap.get("message"), equalTo(sampleStats.getLastException().getMessage()));
                }
            }
        }
    }

    /*
     * The XContentParser::map will return numbers as Integers if they are small enough. In that case, the actual and expected will not be
     * equal if the expected is a long. This method gets the long value of the actual result before asserting that they are equal.
     */
    private void assertNumberEqualsLong(Object actual, long expected) {
        assertThat(actual, instanceOf(Number.class));
        assertThat(((Number) actual).longValue(), equalTo(expected));
    }
}
