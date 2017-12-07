/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.ml.calendars.SpecialEvent;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.config.ModelPlotConfig;
import org.joda.time.DateTime;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UpdateProcessActionRequestTests extends AbstractStreamableTestCase<UpdateProcessAction.Request> {


    @Override
    protected UpdateProcessAction.Request createTestInstance() {
        ModelPlotConfig config = null;
        if (randomBoolean()) {
            config = new ModelPlotConfig(randomBoolean(), randomAlphaOfLength(10));
        }
        List<JobUpdate.DetectorUpdate> updates = null;
        if (randomBoolean()) {
            updates = new ArrayList<>();
            int detectorUpdateCount = randomIntBetween(0, 5);
            for (int i = 0; i < detectorUpdateCount; i++) {
                updates.add(new JobUpdate.DetectorUpdate(randomInt(), randomAlphaOfLength(10), null));
            }
        }
        return new UpdateProcessAction.Request(randomAlphaOfLength(10), config, updates, randomBoolean());
    }

    @Override
    protected UpdateProcessAction.Request createBlankInstance() {
        return new UpdateProcessAction.Request();
    }
}