/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.usage;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.ml.job.persistence.UsagePersister;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;

import java.util.Locale;

/**
 * Reports the number of bytes, fields and records read.
 * Persistence is done via {@linkplain UsagePersister}
 * The main difference betweeen this and the {@linkplain DataCountsReporter}
 * is that this writes hourly reports i.e. how much data was read in an hour
 */
public class UsageReporter extends AbstractComponent {

    public static final Setting<Long> UPDATE_INTERVAL_SETTING = Setting.longSetting("usage.update.interval", 300, 0, Property.NodeScope);

    private final String jobId;

    private long bytesReadSinceLastReport;
    private long fieldsReadSinceLastReport;
    private long recordsReadSinceLastReport;

    private long lastUpdateTimeMs;
    private long updateIntervalMs;

    private final UsagePersister persister;

    public UsageReporter(Settings settings, String jobId, UsagePersister persister) {
        super(settings);
        bytesReadSinceLastReport = 0;
        fieldsReadSinceLastReport = 0;
        recordsReadSinceLastReport = 0;

        this.jobId = jobId;
        this.persister = persister;
        lastUpdateTimeMs = System.currentTimeMillis();

        long interval = UPDATE_INTERVAL_SETTING.get(settings);
        updateIntervalMs = interval * 1000;
        this.logger.info("Setting usage update interval to " + interval + " seconds");
    }

    /**
     * Add <code>bytesRead</code> to the running total
     */
    public void addBytesRead(long bytesRead) {
        bytesReadSinceLastReport += bytesRead;

        long now = System.currentTimeMillis();

        if (now - lastUpdateTimeMs > updateIntervalMs) {
            reportUsage(now);
        }
    }

    public void addFieldsRecordsRead(long fieldsRead) {
        fieldsReadSinceLastReport += fieldsRead;
        ++recordsReadSinceLastReport;
    }

    public long getBytesReadSinceLastReport() {
        return bytesReadSinceLastReport;
    }

    public long getFieldsReadSinceLastReport() {
        return fieldsReadSinceLastReport;
    }

    public long getRecordsReadSinceLastReport() {
        return recordsReadSinceLastReport;
    }


    public String getJobId() {
        return jobId;
    }

    public Logger getLogger() {
        return logger;
    }

    /**
     * Logs total bytes written and calls {@linkplain UsagePersister#persistUsage(String, long, long, long)}
     * bytesReadSinceLastReport, fieldsReadSinceLastReport and
     * recordsReadSinceLastReport are reset to 0 after this has been called.
     */
    public void reportUsage() {
        reportUsage(System.currentTimeMillis());
    }

    /**
     * See {@linkplain #reportUsage()}
     *
     * @param epochMs The time now - saved as the last update time
     */
    private void reportUsage(long epochMs) {
        logger.info(String.format(Locale.ROOT, "An additional %dKiB, %d fields and %d records read by job %s",
                bytesReadSinceLastReport >> 10, fieldsReadSinceLastReport, recordsReadSinceLastReport, jobId));

        try {
            persister.persistUsage(jobId, bytesReadSinceLastReport, fieldsReadSinceLastReport, recordsReadSinceLastReport);
        } catch (ElasticsearchException e) {
            logger.error("Error persisting usage for job " + jobId, e);
        }

        lastUpdateTimeMs = epochMs;

        bytesReadSinceLastReport = 0;
        fieldsReadSinceLastReport = 0;
        recordsReadSinceLastReport = 0;
    }
}
