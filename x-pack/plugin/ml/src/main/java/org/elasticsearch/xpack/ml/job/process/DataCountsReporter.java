/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.process.diagnostics.DataStreamDiagnostics;

import java.util.Date;
import java.util.Locale;
import java.util.function.Predicate;

/**
 * Status reporter for tracking counts of the good/bad records written to the API.
 * Call one of the reportXXX() methods to update the records counts.
 *
 * Stats are logged at specific stages
 * <ol>
 * <li>Every 10,000 records for the first 100,000 records</li>
 * <li>Every 100,000 records until 1,000,000 records</li>
 * <li>Every 1,000,000 records until 10,000,000 records</li>
 * <li>and so on...</li>
 * </ol>
 * The {@link #reportingBoundaryFunction} member points to a different
 * function depending on which reporting stage is the current, the function
 * changes when each of the reporting stages are passed. If the
 * function returns {@code true} the usage is logged.
 */
public class DataCountsReporter {

    private static final Logger logger = LogManager.getLogger(DataCountsReporter.class);

    private final Job job;
    private final JobDataCountsPersister dataCountsPersister;

    private final DataCounts totalRecordStats;
    private volatile DataCounts incrementalRecordStats;

    private long analyzedFieldsPerRecord = 1;

    private long lastRecordCountQuotient = 0;
    private long logEvery = 1;
    private long logCount = 0;

    private Predicate<Long> reportingBoundaryFunction;

    private DataStreamDiagnostics diagnostics;

    public DataCountsReporter(Job job, DataCounts counts, JobDataCountsPersister dataCountsPersister) {
        this.job = job;
        this.dataCountsPersister = dataCountsPersister;

        totalRecordStats = counts;
        incrementalRecordStats = new DataCounts(job.getId());
        diagnostics = new DataStreamDiagnostics(job, counts);

        reportingBoundaryFunction = this::reportEvery10000Records;
    }

    /**
     * Increment the number of records written by 1 and increment
     * the total number of fields read.
     *
     * @param inputFieldCount Number of fields in the record.
     *                        Note this is not the number of processed fields (by field etc)
     *                        but the actual number of fields in the record
     * @param recordTimeMs    The time of the record written
     *                        in milliseconds from the epoch.
     * @param latestRecordTimeMs The time of the latest (in time) record written.
     *                           May be greater than or equal to `recordTimeMs`
     */
    public void reportRecordWritten(long inputFieldCount, long recordTimeMs, long latestRecordTimeMs) {
        final Date latestRecordDate = new Date(latestRecordTimeMs);

        totalRecordStats.incrementInputFieldCount(inputFieldCount);
        totalRecordStats.incrementProcessedRecordCount(1);
        totalRecordStats.setLatestRecordTimeStamp(latestRecordDate);

        incrementalRecordStats.incrementInputFieldCount(inputFieldCount);
        incrementalRecordStats.incrementProcessedRecordCount(1);
        incrementalRecordStats.setLatestRecordTimeStamp(latestRecordDate);

        boolean isFirstReport = totalRecordStats.getEarliestRecordTimeStamp() == null;
        if (isFirstReport) {
            final Date recordDate = new Date(recordTimeMs);
            totalRecordStats.setEarliestRecordTimeStamp(recordDate);
            incrementalRecordStats.setEarliestRecordTimeStamp(recordDate);
        }

        // report at various boundaries
        long totalRecords = getInputRecordCount();
        if (reportingBoundaryFunction.test(totalRecords)) {
            logStatus(totalRecords);
        }

        diagnostics.checkRecord(recordTimeMs);
    }

    /**
     * Update only the incremental stats with the newest record time
     *
     * @param latestRecordTimeMs latest record time as epoch millis
     */
    public void reportLatestTimeIncrementalStats(long latestRecordTimeMs) {
        incrementalRecordStats.setLatestRecordTimeStamp(new Date(latestRecordTimeMs));
    }

    /**
     * Increments the date parse error count
     */
    public void reportDateParseError(long inputFieldCount) {
        totalRecordStats.incrementInvalidDateCount(1);
        totalRecordStats.incrementInputFieldCount(inputFieldCount);

        incrementalRecordStats.incrementInvalidDateCount(1);
        incrementalRecordStats.incrementInputFieldCount(inputFieldCount);
    }

    /**
     * Increments the missing field count
     * Records with missing fields are still processed
     */
    public void reportMissingField() {
        totalRecordStats.incrementMissingFieldCount(1);
        incrementalRecordStats.incrementMissingFieldCount(1);
    }

    public void reportMissingFields(long missingCount) {
        totalRecordStats.incrementMissingFieldCount(missingCount);
        incrementalRecordStats.incrementMissingFieldCount(missingCount);
    }

    /**
     * Add <code>newBytes</code> to the total volume processed
     */
    public void reportBytesRead(long newBytes) {
        totalRecordStats.incrementInputBytes(newBytes);
        incrementalRecordStats.incrementInputBytes(newBytes);
    }

    /**
     * Increments the out of order record count
     */
    public void reportOutOfOrderRecord(long inputFieldCount) {
        totalRecordStats.incrementOutOfOrderTimeStampCount(1);
        totalRecordStats.incrementInputFieldCount(inputFieldCount);

        incrementalRecordStats.incrementOutOfOrderTimeStampCount(1);
        incrementalRecordStats.incrementInputFieldCount(inputFieldCount);
    }

    /**
     * Total records seen = records written to the Engine (processed record
     * count) + date parse error records count + out of order record count.
     * <p>
     * Records with missing fields are counted as they are still written.
     */
    public long getInputRecordCount() {
        return totalRecordStats.getInputRecordCount();
    }

    public long getProcessedRecordCount() {
        return totalRecordStats.getProcessedRecordCount();
    }

    public long getDateParseErrorsCount() {
        return totalRecordStats.getInvalidDateCount();
    }

    public long getMissingFieldErrorCount() {
        return totalRecordStats.getMissingFieldCount();
    }

    public long getOutOfOrderRecordCount() {
        return totalRecordStats.getOutOfOrderTimeStampCount();
    }

    public long getEmptyBucketCount() {
        return totalRecordStats.getEmptyBucketCount();
    }

    public long getSparseBucketCount() {
        return totalRecordStats.getSparseBucketCount();
    }

    public long getBucketCount() {
        return totalRecordStats.getBucketCount();
    }

    public long getBytesRead() {
        return totalRecordStats.getInputBytes();
    }

    public Date getLatestRecordTime() {
        return totalRecordStats.getLatestRecordTimeStamp();
    }

    public Date getLatestEmptyBucketTime() {
        return totalRecordStats.getLatestEmptyBucketTimeStamp();
    }

    public Date getLatestSparseBucketTime() {
        return totalRecordStats.getLatestSparseBucketTimeStamp();
    }

    public long getProcessedFieldCount() {
        totalRecordStats.calcProcessedFieldCount(getAnalysedFieldsPerRecord());
        return totalRecordStats.getProcessedFieldCount();
    }

    public long getInputFieldCount() {
        return totalRecordStats.getInputFieldCount();
    }

    public void setAnalysedFieldsPerRecord(long value) {
        analyzedFieldsPerRecord = value;
    }

    public long getAnalysedFieldsPerRecord() {
        return analyzedFieldsPerRecord;
    }

    /**
     * Report the counts now regardless of whether or not we are at a reporting boundary.
     */
    public void finishReporting() {
        Date now = new Date();
        incrementalRecordStats.setLastDataTimeStamp(now);
        totalRecordStats.setLastDataTimeStamp(now);
        diagnostics.flush();
        retrieveDiagnosticsIntermediateResults();
        dataCountsPersister.persistDataCounts(job.getId(), runningTotalStats());
    }

    /**
     * Log the status.  This is done progressively less frequently as the job
     * processes more data.  Logging every 10000 records when the data rate is
     * 40000 per second quickly rolls the logs.
     */
    protected boolean logStatus(long totalRecords) {
        if (++logCount % logEvery != 0) {
            return false;
        }

        String status = String.format(
            Locale.ROOT,
            "[%s] %d records written to autodetect; missingFieldCount=%d, invalidDateCount=%d, outOfOrderCount=%d",
            job.getId(),
            getProcessedRecordCount(),
            getMissingFieldErrorCount(),
            getDateParseErrorsCount(),
            getOutOfOrderRecordCount()
        );

        logger.info(status);

        int log10TotalRecords = (int) Math.floor(Math.log10(totalRecords));
        // Start reducing the logging rate after a million records have been seen
        if (log10TotalRecords > 5) {
            logEvery = (int) Math.pow(10.0, log10TotalRecords - 5);
            logCount = 0;
        }
        return true;
    }

    private boolean reportEvery10000Records(long totalRecords) {
        if (totalRecords > 100_000) {
            lastRecordCountQuotient = totalRecords / 100_000;
            reportingBoundaryFunction = this::reportEvery100000Records;
            return false;
        }

        long quotient = totalRecords / 10_000;
        if (quotient > lastRecordCountQuotient) {
            lastRecordCountQuotient = quotient;
            return true;
        }

        return false;
    }

    private boolean reportEvery100000Records(long totalRecords) {
        long quotient = totalRecords / 100_000;
        if (quotient > lastRecordCountQuotient) {
            lastRecordCountQuotient = quotient;
            return true;
        }

        return false;
    }

    public void startNewIncrementalCount() {
        incrementalRecordStats = new DataCounts(job.getId());
        retrieveDiagnosticsIntermediateResults();
    }

    public DataCounts incrementalStats() {
        incrementalRecordStats.calcProcessedFieldCount(getAnalysedFieldsPerRecord());
        return incrementalRecordStats;
    }

    public synchronized DataCounts runningTotalStats() {
        totalRecordStats.calcProcessedFieldCount(getAnalysedFieldsPerRecord());
        return totalRecordStats;
    }

    private void retrieveDiagnosticsIntermediateResults() {
        totalRecordStats.incrementBucketCount(diagnostics.getBucketCount());
        totalRecordStats.incrementEmptyBucketCount(diagnostics.getEmptyBucketCount());
        totalRecordStats.incrementSparseBucketCount(diagnostics.getSparseBucketCount());
        totalRecordStats.updateLatestEmptyBucketTimeStamp(diagnostics.getLatestEmptyBucketTime());
        totalRecordStats.updateLatestSparseBucketTimeStamp(diagnostics.getLatestSparseBucketTime());

        incrementalRecordStats.incrementBucketCount(diagnostics.getBucketCount());
        incrementalRecordStats.incrementEmptyBucketCount(diagnostics.getEmptyBucketCount());
        incrementalRecordStats.incrementSparseBucketCount(diagnostics.getSparseBucketCount());
        incrementalRecordStats.updateLatestEmptyBucketTimeStamp(diagnostics.getLatestEmptyBucketTime());
        incrementalRecordStats.updateLatestSparseBucketTimeStamp(diagnostics.getLatestSparseBucketTime());

        diagnostics.resetCounts();
    }

}
