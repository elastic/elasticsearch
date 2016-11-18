/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.usage;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.prelert.job.persistence.UsagePersister;

public class DummyUsageReporter extends UsageReporter {
    long totalByteCount;
    long totalFieldCount;
    long totalRecordCount;

    public DummyUsageReporter(Settings settings, String jobId, Logger logger) {
        super(settings, jobId, new UsagePersister() {
            @Override
            public void persistUsage(String jobId, long bytesRead, long fieldsRead, long recordsRead) {

            }
        }, logger);

        totalByteCount = 0;
        totalFieldCount = 0;
        totalRecordCount = 0;
    }

    public DummyUsageReporter(Settings settings, String jobId, UsagePersister persister, Logger logger) {
        super(settings, jobId, persister, logger);

        totalByteCount = 0;
        totalFieldCount = 0;
        totalRecordCount = 0;
    }


    @Override
    public void addBytesRead(long bytesRead) {
        super.addBytesRead(bytesRead);

        totalByteCount += bytesRead;
    }

    @Override
    public void addFieldsRecordsRead(long fieldsRead) {
        super.addFieldsRecordsRead(fieldsRead);

        totalFieldCount += fieldsRead;
        ++totalRecordCount;
    }

    public long getTotalBytesRead() {
        return totalByteCount;
    }

    public long getTotalFieldsRead() {
        return totalFieldCount;
    }

    public long getTotalRecordsRead() {
        return totalRecordCount;
    }

}
