/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Bucket Result POJO
 */
public class Bucket implements ToXContentObject {

    public static final ParseField ANOMALY_SCORE = new ParseField("anomaly_score");
    public static final ParseField INITIAL_ANOMALY_SCORE = new ParseField("initial_anomaly_score");
    public static final ParseField EVENT_COUNT = new ParseField("event_count");
    public static final ParseField RECORDS = new ParseField("records");
    public static final ParseField BUCKET_INFLUENCERS = new ParseField("bucket_influencers");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField PROCESSING_TIME_MS = new ParseField("processing_time_ms");
    public static final ParseField SCHEDULED_EVENTS = new ParseField("scheduled_events");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("buckets");

    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "bucket";
    public static final ParseField RESULT_TYPE_FIELD = new ParseField(RESULT_TYPE_VALUE);

    public static final ConstructingObjectParser<Bucket, Void> PARSER =
        new ConstructingObjectParser<>(RESULT_TYPE_VALUE, true, a -> new Bucket((String) a[0], (Date) a[1], (long) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p) -> TimeUtil.parseTimeField(p, Result.TIMESTAMP.getPreferredName()), Result.TIMESTAMP, ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        PARSER.declareDouble(Bucket::setAnomalyScore, ANOMALY_SCORE);
        PARSER.declareDouble(Bucket::setInitialAnomalyScore, INITIAL_ANOMALY_SCORE);
        PARSER.declareBoolean(Bucket::setInterim, Result.IS_INTERIM);
        PARSER.declareLong(Bucket::setEventCount, EVENT_COUNT);
        PARSER.declareObjectArray(Bucket::setRecords, AnomalyRecord.PARSER, RECORDS);
        PARSER.declareObjectArray(Bucket::setBucketInfluencers, BucketInfluencer.PARSER, BUCKET_INFLUENCERS);
        PARSER.declareLong(Bucket::setProcessingTimeMs, PROCESSING_TIME_MS);
        PARSER.declareString((bucket, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareStringArray(Bucket::setScheduledEvents, SCHEDULED_EVENTS);
    }

    private final String jobId;
    private final Date timestamp;
    private final long bucketSpan;
    private double anomalyScore;
    private double initialAnomalyScore;
    private List<AnomalyRecord> records = new ArrayList<>();
    private long eventCount;
    private boolean isInterim;
    private List<BucketInfluencer> bucketInfluencers = new ArrayList<>(); // Can't use emptyList as might be appended to
    private long processingTimeMs;
    private List<String> scheduledEvents = Collections.emptyList();

    Bucket(String jobId, Date timestamp, long bucketSpan) {
        this.jobId = jobId;
        this.timestamp = Objects.requireNonNull(timestamp);
        this.bucketSpan = bucketSpan;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.timeField(Result.TIMESTAMP.getPreferredName(), Result.TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
        builder.field(ANOMALY_SCORE.getPreferredName(), anomalyScore);
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(INITIAL_ANOMALY_SCORE.getPreferredName(), initialAnomalyScore);
        if (records.isEmpty() == false) {
            builder.field(RECORDS.getPreferredName(), records);
        }
        builder.field(EVENT_COUNT.getPreferredName(), eventCount);
        builder.field(Result.IS_INTERIM.getPreferredName(), isInterim);
        builder.field(BUCKET_INFLUENCERS.getPreferredName(), bucketInfluencers);
        builder.field(PROCESSING_TIME_MS.getPreferredName(), processingTimeMs);
        if (scheduledEvents.isEmpty() == false) {
            builder.field(SCHEDULED_EVENTS.getPreferredName(), scheduledEvents);
        }
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * Bucketspan expressed in seconds
     */
    public long getBucketSpan() {
        return bucketSpan;
    }

    public double getAnomalyScore() {
        return anomalyScore;
    }

    void setAnomalyScore(double anomalyScore) {
        this.anomalyScore = anomalyScore;
    }

    public double getInitialAnomalyScore() {
        return initialAnomalyScore;
    }

    void setInitialAnomalyScore(double initialAnomalyScore) {
        this.initialAnomalyScore = initialAnomalyScore;
    }

    /**
     * Get all the anomaly records associated with this bucket.
     * The records are not part of the bucket document. They will
     * only be present when the bucket was retrieved and expanded
     * to contain the associated records.
     *
     * @return the anomaly records for the bucket IF the bucket was expanded.
     */
    public List<AnomalyRecord> getRecords() {
        return records;
    }

    void setRecords(List<AnomalyRecord> records) {
        this.records = Collections.unmodifiableList(records);
    }

    /**
     * The number of records (events) actually processed in this bucket.
     */
    public long getEventCount() {
        return eventCount;
    }

    void setEventCount(long value) {
        eventCount = value;
    }

    public boolean isInterim() {
        return isInterim;
    }

    void setInterim(boolean isInterim) {
        this.isInterim = isInterim;
    }

    public long getProcessingTimeMs() {
        return processingTimeMs;
    }

    void setProcessingTimeMs(long timeMs) {
        processingTimeMs = timeMs;
    }

    public List<BucketInfluencer> getBucketInfluencers() {
        return bucketInfluencers;
    }

    void setBucketInfluencers(List<BucketInfluencer> bucketInfluencers) {
        this.bucketInfluencers = Collections.unmodifiableList(bucketInfluencers);
    }

    public List<String> getScheduledEvents() {
        return scheduledEvents;
    }

    void setScheduledEvents(List<String> scheduledEvents) {
        this.scheduledEvents = Collections.unmodifiableList(scheduledEvents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, eventCount, initialAnomalyScore, anomalyScore, records,
                isInterim, bucketSpan, bucketInfluencers, processingTimeMs, scheduledEvents);
    }

    /**
     * Compare all the fields and embedded anomaly records (if any)
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        Bucket that = (Bucket) other;

        return Objects.equals(this.jobId, that.jobId) && Objects.equals(this.timestamp, that.timestamp)
                && (this.eventCount == that.eventCount) && (this.bucketSpan == that.bucketSpan)
                && (this.anomalyScore == that.anomalyScore) && (this.initialAnomalyScore == that.initialAnomalyScore)
                && Objects.equals(this.records, that.records) && Objects.equals(this.isInterim, that.isInterim)
                && Objects.equals(this.bucketInfluencers, that.bucketInfluencers)
                && (this.processingTimeMs == that.processingTimeMs)
                && Objects.equals(this.scheduledEvents, that.scheduledEvents);
    }
}
