/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.extraction;

import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.SchedulerConfig;

public interface DataExtractorFactory {
    DataExtractor newExtractor(SchedulerConfig schedulerConfig, Job job);
}
