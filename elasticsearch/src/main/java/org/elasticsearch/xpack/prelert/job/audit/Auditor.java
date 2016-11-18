/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.audit;

public interface Auditor
{
    void info(String message);
    void warning(String message);
    void error(String message);
    void activity(String message);
    void activity(int totalJobs, int totalDetectors, int runningJobs, int runningDetectors);
}
