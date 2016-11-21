/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.action.GetJobAction.Response;
import org.elasticsearch.xpack.prelert.job.AnalysisConfig;
import org.elasticsearch.xpack.prelert.job.AnalysisLimits;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.DataDescription;
import org.elasticsearch.xpack.prelert.job.Detector;
import org.elasticsearch.xpack.prelert.job.IgnoreDowntime;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelDebugConfig;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.SchedulerConfig;
import org.elasticsearch.xpack.prelert.job.transform.TransformConfig;
import org.elasticsearch.xpack.prelert.job.transform.TransformType;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class GetJobActionResponseTests extends AbstractStreamableTestCase<GetJobAction.Response> {

    @Override
    protected Response createTestInstance() {
        final Response result;
        if (randomBoolean()) {
            result = new Response();
        } else {
            String jobId = randomAsciiOfLength(10);
            String description = randomBoolean() ? randomAsciiOfLength(10) : null;
            Date createTime = new Date(randomPositiveLong());
            Date finishedTime = randomBoolean() ? new Date(randomPositiveLong()) : null;
            Date lastDataTime = randomBoolean() ? new Date(randomPositiveLong()) : null;
            long timeout = randomPositiveLong();
            AnalysisConfig analysisConfig = new AnalysisConfig.Builder(
                    Collections.singletonList(new Detector.Builder("metric", "some_field").build())).build();
            AnalysisLimits analysisLimits = new AnalysisLimits(randomPositiveLong(), randomPositiveLong());
            SchedulerConfig.Builder schedulerConfig = new SchedulerConfig.Builder(SchedulerConfig.DataSource.FILE);
            schedulerConfig.setFilePath("/file/path");
            DataDescription dataDescription = randomBoolean() ? new DataDescription.Builder().build() : null;
            int numTransformers = randomIntBetween(0, 32);
            List<TransformConfig> transformConfigList = new ArrayList<>(numTransformers);
            for (int i = 0; i < numTransformers; i++) {
                transformConfigList.add(new TransformConfig(TransformType.UPPERCASE.prettyName()));
            }
            ModelDebugConfig modelDebugConfig = randomBoolean() ? new ModelDebugConfig(randomDouble(), randomAsciiOfLength(10)) : null;
            IgnoreDowntime ignoreDowntime = randomFrom(IgnoreDowntime.values());
            Long normalizationWindowDays = randomBoolean() ? randomLong() : null;
            Long backgroundPersistInterval = randomBoolean() ? randomLong() : null;
            Long modelSnapshotRetentionDays = randomBoolean() ? randomLong() : null;
            Long resultsRetentionDays = randomBoolean() ? randomLong() : null;
            Map<String, Object> customConfig = randomBoolean() ? Collections.singletonMap(randomAsciiOfLength(10), randomAsciiOfLength(10))
                    : null;
            String modelSnapshotId = randomBoolean() ? randomAsciiOfLength(10) : null;
            Job job = new Job(jobId, description, createTime, finishedTime, lastDataTime,
                    timeout, analysisConfig, analysisLimits, schedulerConfig.build(), dataDescription, transformConfigList,
                    modelDebugConfig, ignoreDowntime, normalizationWindowDays, backgroundPersistInterval,
                    modelSnapshotRetentionDays, resultsRetentionDays, customConfig, modelSnapshotId);


            DataCounts dataCounts = null;
            ModelSizeStats sizeStats = null;

            if (randomBoolean()) {
            dataCounts = new DataCounts(randomAsciiOfLength(10), randomIntBetween(1, 1_000_000),
                        randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                        randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                        new DateTime(randomDateTimeZone()).toDate(), new DateTime(randomDateTimeZone()).toDate());
            }
            if (randomBoolean()) {
                sizeStats = new ModelSizeStats.Builder("foo").build();
            }
            result = new Response(new GetJobAction.Response.JobInfo(job, dataCounts, sizeStats));
        }

        return result;
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
