/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.status;

import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.prelert.job.usage.UsageReporter;

/**
 * Dummy StatusReporter for testing abstract class
 */
public class DummyStatusReporter extends StatusReporter {
    boolean statusReported = false;

    public DummyStatusReporter(Environment env, UsageReporter usageReporter) {
        super(env, env.settings(), "DummyJobId", usageReporter, new JobDataCountsPersister() {
            @Override
            public void persistDataCounts(String jobId, DataCounts counts) {

            }
        }, null, 1);
    }

    public DummyStatusReporter(Environment env, DataCounts counts,
            UsageReporter usageReporter) {
        super(env, env.settings(), "DummyJobId", counts, usageReporter, new JobDataCountsPersister() {
            @Override
            public void persistDataCounts(String jobId, DataCounts counts) {

            }
        }, null, 1);
    }


    public boolean isStatusReported() {
        return statusReported;
    }
}
