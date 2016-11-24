/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.status;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.prelert.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.prelert.job.usage.UsageReporter;

import static org.mockito.Mockito.mock;

/**
 * Dummy StatusReporter for testing abstract class
 */
class DummyStatusReporter extends StatusReporter {

    DummyStatusReporter(UsageReporter usageReporter) {
        super(Settings.EMPTY, "DummyJobId", usageReporter, mock(JobDataCountsPersister.class));
    }

}
