/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;

import java.io.Closeable;
import java.util.Date;
import java.util.Locale;
import java.util.function.Function;


/**
 * Status reporter for tracking counts of the good/bad records written to the API.
 * Call one of the reportXXX() methods to update the records counts.
 *
 * Stats are logged at specific stages
 * <ol>
 * <li>Every 100 records for the first 1000 records</li>
 * <li>Every 1000 records for the first 20000 records</li>
 * <li>Every 10000 records after 20000 records</li>
 * </ol>
 * The {@link #reportingBoundaryFunction} member points to a different
 * function depending on which reporting stage is the current, the function
 * changes when each of the reporting stages are passed. If the
 * function returns {@code true} the usage is logged.
 *
 * DataCounts are persisted periodically in a datafeed task via
 * {@linkplain JobDataCountsPersister},  {@link #close()} must be called to
 * cancel the datafeed task.
 */
public class DataCountsReporter extends AbstractComponent implements Closeable {
    /**
     * The max percentage of date parse errors allowed before
     * an exception is thrown.
     */
    public static final Setting<Integer> ACCEPTABLE_PERCENTAGE_DATE_PARSE_ERRORS_SETTING = Setting.intSetting("max.percent.date.errors", 25,
            Property.NodeScope);

    /**
     * The max percentage of out of order records allowed before
     * an exception is thrown.
     */
    public static final Setting<Integer> ACCEPTABLE_PERCENTAGE_OUT_OF_ORDER_ERRORS_SETTING = Setting
            .intSetting("max.percent.outoforder.errors", 25, Property.NodeScope);

    private static final TimeValue PERSIST_INTERVAL = TimeValue.timeValueMillis(10_000L);

    private final Job job;
    private final JobDataCountsPersister dataCountsPersister;

    private final DataCounts totalRecordStats;
    private volatile DataCounts incrementalRecordStats;

    private long analyzedFieldsPerRecord = 1;

    private long lastRecordCountQuotient = 0;
    private long logEvery = 1;
    private long logCount = 0;

    private final int acceptablePercentDateParseErrors;
    private final int acceptablePercentOutOfOrderErrors;

    private Function<Long, Boolean> reportingBoundaryFunction;

    private volatile boolean persistDataCountsOnNextRecord;
    private final ThreadPool.Cancellable persistDataCountsDatafeedAction;

    private DataStreamDiagnostics diagnostics;

    public DataCountsReporter(ThreadPool threadPool, Settings settings, Job job, DataCounts counts,
            JobDataCountsPersister dataCountsPersister) {

        super(settings);

        this.job = job;
        this.dataCountsPersister = dataCountsPersister;

        totalRecordStats = counts;
        incrementalRecordStats = new DataCounts(job.getId());
        diagnostics = new DataStreamDiagnostics(job);

        acceptablePercentDateParseErrors = ACCEPTABLE_PERCENTAGE_DATE_PARSE_ERRORS_SETTING.get(settings);
        acceptablePercentOutOfOrderErrors = ACCEPTABLE_PERCENTAGE_OUT_OF_ORDER_ERRORS_SETTING.get(settings);

        reportingBoundaryFunction = this::reportEvery100Records;

        persistDataCountsDatafeedAction = threadPool.scheduleWithFixedDelay(() -> persistDataCountsOnNextRecord = true,
                PERSIST_INTERVAL, ThreadPool.Names.GENERIC);
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
     */
    public void reportRecordWritten(long inputFieldCount, long recordTimeMs) {
        Date recordDate = new Date(recordTimeMs);

        totalRecordStats.incrementInputFieldCount(inputFieldCount);
        totalRecordStats.incrementProcessedRecordCount(1);
        totalRecordStats.setLatestRecordTimeStamp(recordDate);

        incrementalRecordStats.incrementInputFieldCount(inputFieldCount);
        incrementalRecordStats.incrementProcessedRecordCount(1);
        incrementalRecordStats.setLatestRecordTimeStamp(recordDate);

        boolean isFirstReport = totalRecordStats.getEarliestRecordTimeStamp() == null;
        if (isFirstReport) {
            totalRecordStats.setEarliestRecordTimeStamp(recordDate);
            incrementalRecordStats.setEarliestRecordTimeStamp(recordDate);
        }

        // report at various boundaries
        long totalRecords = getInputRecordCount();
        if (reportingBoundaryFunction.apply(totalRecords)) {
            logStatus(totalRecords);
        }

        if (persistDataCountsOnNextRecord) {
            retrieveDiagnosticsIntermediateResults();
            
            DataCounts copy = new DataCounts(runningTotalStats());
            dataCountsPersister.persistDataCounts(job.getId(), copy, new LoggingActionListener());
            persistDataCountsOnNextRecord = false;
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

    public int getAcceptablePercentDateParseErrors() {
        return acceptablePercentDateParseErrors;
    }

    public int getAcceptablePercentOutOfOrderErrors() {
        return acceptablePercentOutOfOrderErrors;
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
        dataCountsPersister.persistDataCounts(job.getId(), runningTotalStats(), new LoggingActionListener());
    }

    /**
     * Log the status.  This is done progressively less frequently as the job
     * processes more data.  Logging every 10000 records when the data rate is
     * 40000 per second quickly rolls the logs.
     */
    protected void logStatus(long totalRecords) {
        if (++logCount % logEvery != 0) {
            return;
        }

        String status = String.format(Locale.ROOT,
                "[%s] %d records written to autodetect; missingFieldCount=%d, invalidDateCount=%d, outOfOrderCount=%d", job.getId(),
                getProcessedRecordCount(), getMissingFieldErrorCount(), getDateParseErrorsCount(), getOutOfOrderRecordCount());

        logger.info(status);

        int log10TotalRecords = (int) Math.floor(Math.log10(totalRecords));
        // Start reducing the logging rate after 10 million records have been seen
        if (log10TotalRecords > 6) {
            logEvery = (int) Math.pow(10.0, log10TotalRecords - 6);
            logCount = 0;
        }
    }

    private boolean reportEvery100Records(long totalRecords) {
        if (totalRecords > 1000) {
            lastRecordCountQuotient = totalRecords / 1000;
            reportingBoundaryFunction = this::reportEvery1000Records;
            return false;
        }

        long quotient = totalRecords / 100;
        if (quotient > lastRecordCountQuotient) {
            lastRecordCountQuotient = quotient;
            return true;
        }

        return false;
    }

    private boolean reportEvery1000Records(long totalRecords) {

        if (totalRecords > 20000) {
            lastRecordCountQuotient = totalRecords / 10000;
            reportingBoundaryFunction = this::reportEvery10000Records;
            return false;
        }

        long quotient = totalRecords / 1000;
        if (quotient > lastRecordCountQuotient) {
            lastRecordCountQuotient = quotient;
            return true;
        }

        return false;
    }

    private boolean reportEvery10000Records(long totalRecords) {
        long quotient = totalRecords / 10000;
        if (quotient > lastRecordCountQuotient) {
            lastRecordCountQuotient = quotient;
            return true;
        }

        return false;
    }

    public void startNewIncrementalCount() {
        incrementalRecordStats = new DataCounts(job.getId());
        retrieveDiagnosticsIntermediateResults();
        diagnostics.resetCounts();
    }

    public DataCounts incrementalStats() {
        incrementalRecordStats.calcProcessedFieldCount(getAnalysedFieldsPerRecord());
        return incrementalRecordStats;
    }

    public synchronized DataCounts runningTotalStats() {
        totalRecordStats.calcProcessedFieldCount(getAnalysedFieldsPerRecord());
        return totalRecordStats;
    }

    @Override
    public void close() {
        persistDataCountsDatafeedAction.cancel();
    }

    private void retrieveDiagnosticsIntermediateResults() {
        totalRecordStats.incrementBucketCount(diagnostics.getEmptyBucketCount());
        totalRecordStats.incrementBucketCount(diagnostics.getBucketCount());
        totalRecordStats.incrementSparseBucketCount(diagnostics.getSparseBucketCount());
        totalRecordStats.updateLatestEmptyBucketTimeStamp(diagnostics.getLatestEmptyBucketTime());
        totalRecordStats.updateLatestSparseBucketTimeStamp(diagnostics.getLatestSparseBucketTime());

        incrementalRecordStats.incrementEmptyBucketCount(diagnostics.getEmptyBucketCount());
        incrementalRecordStats.incrementBucketCount(diagnostics.getBucketCount());
        incrementalRecordStats.incrementSparseBucketCount(diagnostics.getSparseBucketCount());
        incrementalRecordStats.updateLatestEmptyBucketTimeStamp(diagnostics.getLatestEmptyBucketTime());
        incrementalRecordStats.updateLatestSparseBucketTimeStamp(diagnostics.getLatestSparseBucketTime());

        diagnostics.resetCounts();
    }

    /**
     * Log success/error
     */
    private class LoggingActionListener implements ActionListener<Boolean> {
        @Override
        public void onResponse(Boolean aBoolean) {
            logger.trace("[{}] Persisted DataCounts", job.getId());
        }

        @Override
        public void onFailure(Exception e) {
            logger.debug(new ParameterizedMessage("[{}] Error persisting DataCounts stats", job.getId()), e);
        }
    }
}
