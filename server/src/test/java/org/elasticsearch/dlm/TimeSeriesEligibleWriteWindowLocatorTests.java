/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.dlm;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesEligibleWriteWindowLocatorTests extends ESTestCase {

    private static final TimeSeriesEligibleWriteWindowLocator DLM_ONLY = new TimeSeriesEligibleWriteWindowLocator();

    public void testInfiniteWriteWindow() {
        DataStream dataStream = dataStream("metrics-test", null);
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        assertThat(DLM_ONLY.getEligibleWriteWindowStart(dataStream, project, null, randomNonNegativeLong()), equalTo(-1L));

        DataStreamLifecycle disabled = DataStreamLifecycle.dataLifecycleBuilder()
            .dataRetention(TimeValue.timeValueDays(30))
            .enabled(false)
            .build();
        dataStream = dataStream("metrics-test", disabled);
        project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        assertThat(DLM_ONLY.getEligibleWriteWindowStart(dataStream, project, null, randomNonNegativeLong()), equalTo(-1L));

        DataStreamLifecycle enabled = DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE;
        dataStream = dataStream("metrics-test", enabled);
        project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        assertThat(DLM_ONLY.getEligibleWriteWindowStart(dataStream, project, null, randomNonNegativeLong()), equalTo(-1L));

        DataStreamLifecycle withRetention = DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(30)).build();
        Index index = new Index(DataStream.getDefaultBackingIndexName("non-tsdb", 1), randomAlphaOfLength(10));
        dataStream = DataStream.builder("non-tsdb", List.of(index)).setLifecycle(withRetention).build();
        assertThat(DLM_ONLY.getEligibleWriteWindowStart(dataStream, project, null, randomNonNegativeLong()), equalTo(-1L));
    }

    public void testWriteWindowDefinedByRetention() {
        // Configured retention
        {
            TimeValue retention = TimeValue.timeValueDays(30);
            DataStreamLifecycle lifecycle = DataStreamLifecycle.dataLifecycleBuilder().dataRetention(retention).build();
            DataStream dataStream = dataStream("metrics-test", lifecycle);
            ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
            long requestTimestamp = randomNonNegativeLong();
            assertThat(
                DLM_ONLY.getEligibleWriteWindowStart(dataStream, project, null, requestTimestamp),
                equalTo(requestTimestamp - retention.getMillis())
            );
        }
        // Global retention
        {
            TimeValue retention = TimeValue.timeValueDays(30);
            TimeValue globalMax = TimeValue.timeValueDays(7);
            DataStreamLifecycle.Builder lifecycleBuilder = DataStreamLifecycle.dataLifecycleBuilder();
            if (randomBoolean()) {
                lifecycleBuilder.dataRetention(retention);
            }
            DataStreamLifecycle lifecycle = lifecycleBuilder.build();
            DataStream dataStream = dataStream("metrics-test", lifecycle);
            ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
            DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(null, globalMax);
            long requestTimestamp = randomNonNegativeLong();
            assertThat(
                DLM_ONLY.getEligibleWriteWindowStart(dataStream, project, globalRetention, requestTimestamp),
                equalTo(requestTimestamp - globalMax.getMillis())
            );
        }
    }

    public void testWriteWindowDefinedByFrozenAfter() {
        TimeValue frozenAfter = TimeValue.timeValueDays(7);
        DataStreamLifecycle lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .frozenAfter(frozenAfter)
            .dataRetention(TimeValue.timeValueDays(10))
            .build();
        DataStream dataStream = dataStream("metrics-test", lifecycle);
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        long requestTimestamp = randomNonNegativeLong();
        assertThat(
            DLM_ONLY.getEligibleWriteWindowStart(dataStream, project, null, requestTimestamp),
            equalTo(requestTimestamp - frozenAfter.getMillis())
        );
    }

    public void testWriteWindowDefinedByDownsample() {
        TimeValue downsampleAfter = TimeValue.timeValueDays(randomIntBetween(1, 10));
        TimeValue frozenAfter = TimeValue.timeValueDays(randomIntBetween(11, 20));
        DataStreamLifecycle lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsamplingRounds(List.of(new DataStreamLifecycle.DownsamplingRound(downsampleAfter, new DateHistogramInterval("1h"))))
            .frozenAfter(frozenAfter)
            .build();
        DataStream dataStream = dataStream("metrics-test", lifecycle);
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        long requestTimestamp = randomNonNegativeLong();
        assertThat(
            DLM_ONLY.getEligibleWriteWindowStart(dataStream, project, null, requestTimestamp),
            equalTo(requestTimestamp - downsampleAfter.getMillis())
        );
    }

    public void testIlmPolicyDelegatesEligibleWindowStart() {
        long expectedWindow = randomBoolean() ? randomNonNegativeLong() : -1;
        TimeSeriesEligibleWriteWindowLocator locator = new TimeSeriesEligibleWriteWindowLocator() {
            @Override
            public String getEffectiveIlmPolicy(DataStream ds, ProjectMetadata project) {
                return "my-policy";
            }

            @Override
            public long getEligibleWriteWindowFromPolicy(String policy, ProjectMetadata project) {
                assertEquals("my-policy", policy);
                return expectedWindow;
            }
        };

        DataStream dataStream = dataStream("metrics-test", null);
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        assertThat(locator.getEligibleWriteWindowStart(dataStream, project, null, randomNonNegativeLong()), equalTo(expectedWindow));
    }

    private static DataStream dataStream(String name, DataStreamLifecycle lifecycle) {
        Index index = new Index(DataStream.getDefaultBackingIndexName(name, 1), randomAlphaOfLength(10));
        return DataStream.builder(name, List.of(index)).setLifecycle(lifecycle).setIndexMode(IndexMode.TIME_SERIES).build();
    }
}
