/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesFeatureSetUsageTests extends AbstractWireSerializingTestCase<TimeSeriesFeatureSetUsage> {

    @Override
    protected Writeable.Reader<TimeSeriesFeatureSetUsage> instanceReader() {
        return TimeSeriesFeatureSetUsage::new;
    }

    @Override
    protected TimeSeriesFeatureSetUsage createTestInstance() {
        int randomisationBranch = randomIntBetween(0, 3);
        return switch (randomisationBranch) {
            // Doc stats cover standalone time series indices too, so they can be non-zero without any data streams.
            case 0 -> new TimeSeriesFeatureSetUsage(0, 0, randomNonNegativeLong(), randomNonNegativeLong(), null, null);
            case 1 -> new TimeSeriesFeatureSetUsage(
                randomIntBetween(0, 100),
                randomIntBetween(100, 100000),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                TimeSeriesFeatureSetUsage.DownsamplingFeatureStats.EMPTY,
                Map.of()
            );
            case 2 -> new TimeSeriesFeatureSetUsage(
                randomIntBetween(0, 100),
                randomIntBetween(100, 100000),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomDownsamplingFeatureStats(),
                randomIlmPolicyStats(),
                randomDownsamplingFeatureStats(),
                randomIntervalMap()
            );
            case 3 -> new TimeSeriesFeatureSetUsage(
                randomIntBetween(0, 100),
                randomIntBetween(100, 100000),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomDownsamplingFeatureStats(),
                randomIntervalMap()
            );
            default -> throw new AssertionError("Illegal randomisation branch: " + randomisationBranch);
        };
    }

    @Override
    protected TimeSeriesFeatureSetUsage mutateInstance(TimeSeriesFeatureSetUsage instance) throws IOException {
        var dataStreamCount = instance.getTimeSeriesDataStreamCount();
        if (dataStreamCount == 0) {
            return new TimeSeriesFeatureSetUsage(
                randomIntBetween(1, 100),
                randomIntBetween(100, 100000),
                instance.getNumDocs(),
                instance.getSizeInBytes(),
                TimeSeriesFeatureSetUsage.DownsamplingFeatureStats.EMPTY,
                Map.of()
            );
        }
        var indexCount = instance.getTimeSeriesIndexCount();
        var numDocs = instance.getNumDocs();
        var sizeInBytes = instance.getSizeInBytes();
        var ilm = instance.getDownsamplingUsage().ilmDownsamplingStats();
        var ilmPhases = instance.getDownsamplingUsage().ilmPolicyStats();
        var dlm = instance.getDownsamplingUsage().dlmDownsamplingStats();
        var indexPerInterval = instance.getDownsamplingUsage().indexCountPerInterval();
        int randomisationBranch = between(0, 7);
        switch (randomisationBranch) {
            case 0 -> dataStreamCount += randomIntBetween(1, 100);
            case 1 -> indexCount += randomIntBetween(1, 100);
            case 2 -> ilm = randomValueOtherThan(ilm, this::randomDownsamplingFeatureStats);
            case 3 -> ilmPhases = randomValueOtherThan(ilmPhases, this::randomIlmPolicyStats);
            case 4 -> dlm = randomValueOtherThan(dlm, this::randomDownsamplingFeatureStats);
            case 5 -> indexPerInterval = randomValueOtherThan(indexPerInterval, this::randomIntervalMap);
            case 6 -> numDocs = randomValueOtherThan(numDocs, ESTestCase::randomNonNegativeLong);
            case 7 -> sizeInBytes = randomValueOtherThan(sizeInBytes, ESTestCase::randomNonNegativeLong);
            default -> throw new AssertionError("Illegal randomisation branch: " + randomisationBranch);
        }
        return new TimeSeriesFeatureSetUsage(dataStreamCount, indexCount, numDocs, sizeInBytes, ilm, ilmPhases, dlm, indexPerInterval);
    }

    public void testDocStatsNotSentToOlderNodes() throws IOException {
        TimeSeriesFeatureSetUsage original = new TimeSeriesFeatureSetUsage(
            randomIntBetween(0, 100),
            randomIntBetween(100, 100000),
            randomLongBetween(1, Long.MAX_VALUE),
            randomLongBetween(1, Long.MAX_VALUE),
            TimeSeriesFeatureSetUsage.DownsamplingFeatureStats.EMPTY,
            Map.of()
        );
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(TimeSeriesFeatureSetUsage.TIME_SERIES_USAGE_DOC_STATS);

        TimeSeriesFeatureSetUsage copy = copyInstance(original, oldVersion);

        assertThat(copy.getNumDocs(), equalTo(0L));
        assertThat(copy.getSizeInBytes(), equalTo(0L));
        assertThat(copy.getTimeSeriesDataStreamCount(), equalTo(original.getTimeSeriesDataStreamCount()));
        assertThat(copy.getTimeSeriesIndexCount(), equalTo(original.getTimeSeriesIndexCount()));
    }

    private TimeSeriesFeatureSetUsage.DownsamplingFeatureStats randomDownsamplingFeatureStats() {
        return new TimeSeriesFeatureSetUsage.DownsamplingFeatureStats(
            randomIntBetween(1, 100),
            randomIntBetween(1, 100),
            randomIntBetween(1, 10),
            randomDoubleBetween(1.0, 10.0, true),
            randomIntBetween(1, 10),
            randomIntBetween(1, 10),
            randomIntBetween(1, 10),
            randomIntBetween(1, 10)
        );
    }

    private TimeSeriesFeatureSetUsage.IlmPolicyStats randomIlmPolicyStats() {
        return new TimeSeriesFeatureSetUsage.IlmPolicyStats(
            randomNonEmptySubsetOf(Set.of("hot", "warm", "cold")).stream()
                .collect(Collectors.toMap(k -> k, ignored -> randomNonNegativeLong())),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100)
        );
    }

    private Map<String, Long> randomIntervalMap() {
        return randomMap(1, 10, () -> Tuple.tuple(randomIntBetween(1, 400) + randomFrom("m", "h", "d"), randomNonNegativeLong()));
    }
}
