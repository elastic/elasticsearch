/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.MlFilterTests;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfigTests;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;

import java.util.ArrayList;
import java.util.List;

public class UpdateProcessActionRequestTests extends AbstractWireSerializingTestCase<UpdateProcessAction.Request> {

    @Override
    protected UpdateProcessAction.Request createTestInstance() {
        ModelPlotConfig modelPlotConfig = null;
        if (randomBoolean()) {
            modelPlotConfig = ModelPlotConfigTests.createRandomized();
        }
        PerPartitionCategorizationConfig perPartitionCategorizationConfig = null;
        if (randomBoolean()) {
            perPartitionCategorizationConfig = new PerPartitionCategorizationConfig(true, randomBoolean());
        }
        List<JobUpdate.DetectorUpdate> updates = null;
        if (randomBoolean()) {
            updates = new ArrayList<>();
            int detectorUpdateCount = randomIntBetween(0, 5);
            for (int i = 0; i < detectorUpdateCount; i++) {
                updates.add(new JobUpdate.DetectorUpdate(randomInt(), randomAlphaOfLength(10), null));
            }
        }
        MlFilter filter = null;
        if (randomBoolean()) {
            filter = MlFilterTests.createTestFilter();
        }
        return new UpdateProcessAction.Request(
            randomAlphaOfLength(10),
            modelPlotConfig,
            perPartitionCategorizationConfig,
            updates,
            filter,
            randomBoolean()
        );
    }

    @Override
    protected Writeable.Reader<UpdateProcessAction.Request> instanceReader() {
        return UpdateProcessAction.Request::new;
    }
}
