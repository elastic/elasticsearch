/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.config.ModelDebugConfig;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;

import java.util.List;

public class UpdateProcessActionRequestTests extends AbstractStreamableTestCase<UpdateProcessAction.Request> {


    @Override
    protected UpdateProcessAction.Request createTestInstance() {
        ModelDebugConfig config = null;
        if (randomBoolean()) {
            config = new ModelDebugConfig(5.0, "debug,config");
        }
        List<JobUpdate.DetectorUpdate> updates = null;
        if (randomBoolean()) {

        }
        return new UpdateProcessAction.Request(randomAsciiOfLength(10), config, updates);
    }

    @Override
    protected UpdateProcessAction.Request createBlankInstance() {
        return new UpdateProcessAction.Request();
    }
}