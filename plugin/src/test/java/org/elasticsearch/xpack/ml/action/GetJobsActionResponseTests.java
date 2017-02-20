/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.action.GetJobsAction.Response;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.ModelDebugConfig;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class GetJobsActionResponseTests extends AbstractStreamableTestCase<GetJobsAction.Response> {

    @Override
    protected Response createTestInstance() {
        final Response result;

        int listSize = randomInt(10);
        List<Job> jobList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String jobId = "job" + j;
            String description = randomBoolean() ? randomAsciiOfLength(100) : null;
            Date createTime = new Date(randomNonNegativeLong());
            Date finishedTime = randomBoolean() ? new Date(randomNonNegativeLong()) : null;
            Date lastDataTime = randomBoolean() ? new Date(randomNonNegativeLong()) : null;
            long timeout = randomNonNegativeLong();
            AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
                    Collections.singletonList(new Detector.Builder("metric", "some_field").build()));
            AnalysisLimits analysisLimits = new AnalysisLimits(randomNonNegativeLong(), randomNonNegativeLong());
            DataDescription.Builder dataDescription = new DataDescription.Builder();
            ModelDebugConfig modelDebugConfig = randomBoolean() ? new ModelDebugConfig(randomDouble(), randomAsciiOfLength(10)) : null;
            Long normalizationWindowDays = randomBoolean() ? Long.valueOf(randomIntBetween(0, 365)) : null;
            Long backgroundPersistInterval = randomBoolean() ? Long.valueOf(randomIntBetween(3600, 86400)) : null;
            Long modelSnapshotRetentionDays = randomBoolean() ? Long.valueOf(randomIntBetween(0, 365)) : null;
            Long resultsRetentionDays = randomBoolean() ? Long.valueOf(randomIntBetween(0, 365)) : null;
            Map<String, Object> customConfig = randomBoolean() ? Collections.singletonMap(randomAsciiOfLength(10), randomAsciiOfLength(10))
                    : null;
            String modelSnapshotId = randomBoolean() ? randomAsciiOfLength(10) : null;
            String indexName =  "index" + j;
            Job.Builder builder = new Job.Builder();
            builder.setId(jobId);
            builder.setDescription(description);
            builder.setCreateTime(createTime);
            builder.setFinishedTime(finishedTime);
            builder.setLastDataTime(lastDataTime);
            builder.setAnalysisConfig(analysisConfig);
            builder.setAnalysisLimits(analysisLimits);
            builder.setDataDescription(dataDescription);
            builder.setModelDebugConfig(modelDebugConfig);
            builder.setRenormalizationWindowDays(normalizationWindowDays);
            builder.setBackgroundPersistInterval(backgroundPersistInterval);
            builder.setModelSnapshotRetentionDays(modelSnapshotRetentionDays);
            builder.setResultsRetentionDays(resultsRetentionDays);
            builder.setCustomSettings(customConfig);
            builder.setModelSnapshotId(modelSnapshotId);
            builder.setResultsIndexName(indexName);
            builder.setDeleted(randomBoolean());
            Job job = builder.build();

            jobList.add(job);
        }

        result = new Response(new QueryPage<>(jobList, jobList.size(), Job.RESULTS_FIELD));

        return result;
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
