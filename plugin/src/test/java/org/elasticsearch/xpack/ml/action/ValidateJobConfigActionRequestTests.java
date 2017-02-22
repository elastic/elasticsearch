/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.ValidateJobConfigAction.Request;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class ValidateJobConfigActionRequestTests extends AbstractStreamableXContentTestCase<ValidateJobConfigAction.Request> {

    @Override
    protected Request createTestInstance() {
        List<Detector> detectors = new ArrayList<>();
        detectors.add(new Detector.Builder(randomFrom(Detector.FIELD_NAME_FUNCTIONS), randomAsciiOfLengthBetween(1, 20)).build());
        detectors.add(new Detector.Builder(randomFrom(Detector.COUNT_WITHOUT_FIELD_FUNCTIONS), null).build());
        AnalysisConfig.Builder analysisConfigBuilder = new AnalysisConfig.Builder(detectors);
        analysisConfigBuilder.setBucketSpan(randomIntBetween(60, 86400));
        if (randomBoolean()) {
            analysisConfigBuilder.setLatency(randomIntBetween(0, 12));
        }
        if (randomBoolean()) {
            analysisConfigBuilder.setCategorizationFieldName(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            analysisConfigBuilder.setSummaryCountFieldName(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            List<String> influencers = new ArrayList<>();
            for (int i = randomIntBetween(1, 5); i > 0; --i) {
                influencers.add(randomAsciiOfLengthBetween(1, 20));
            }
            analysisConfigBuilder.setInfluencers(influencers);
        }
        if (randomBoolean()) {
            analysisConfigBuilder.setOverlappingBuckets(randomBoolean());
        }
        if (randomBoolean()) {
            analysisConfigBuilder.setMultivariateByFields(randomBoolean());
        }
        Job.Builder job = new Job.Builder("ok");
        job.setAnalysisConfig(analysisConfigBuilder);
        if (randomBoolean()) {
            DataDescription.Builder dataDescription = new DataDescription.Builder();
            if (randomBoolean()) {
                dataDescription.setFormat(DataDescription.DataFormat.DELIMITED);
                if (randomBoolean()) {
                    dataDescription.setFieldDelimiter(';');
                }
                if (randomBoolean()) {
                    dataDescription.setQuoteCharacter('\'');
                }
            } else {
                dataDescription.setFormat(DataDescription.DataFormat.JSON);
            }
            dataDescription.setTimeField(randomAsciiOfLengthBetween(1, 20));
            if (randomBoolean()) {
                dataDescription.setTimeFormat("yyyy-MM-dd HH:mm:ssX");
            }
            job.setDataDescription(dataDescription);
        }
        if (randomBoolean()) {
            job.setAnalysisLimits(new AnalysisLimits(randomNonNegativeLong(), randomNonNegativeLong()));
        }
        job.setCreateTime(new Date());
        job.setId(randomAsciiOfLength(5).replace(".", "-").replace("_", "-").toLowerCase(Locale.ENGLISH));
        return new Request(job.build());
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return Request.parseRequest(parser);
    }

}
