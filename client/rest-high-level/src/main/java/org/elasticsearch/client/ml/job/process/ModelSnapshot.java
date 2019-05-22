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
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.Version;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.util.TimeUtil;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

/**
 * ModelSnapshot Result POJO
 */
public class ModelSnapshot implements ToXContentObject {
    /**
     * Field Names
     */
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField SNAPSHOT_DOC_COUNT = new ParseField("snapshot_doc_count");
    public static final ParseField LATEST_RECORD_TIME = new ParseField("latest_record_time_stamp");
    public static final ParseField LATEST_RESULT_TIME = new ParseField("latest_result_time_stamp");
    public static final ParseField QUANTILES = new ParseField("quantiles");
    public static final ParseField RETAIN = new ParseField("retain");
    public static final ParseField MIN_VERSION = new ParseField("min_version");
    public static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("model_snapshot", true, Builder::new);

    static {
        PARSER.declareString(Builder::setJobId, Job.ID);
        PARSER.declareString(Builder::setMinVersion, MIN_VERSION);
        PARSER.declareField(Builder::setTimestamp,
            (p) -> TimeUtil.parseTimeField(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ValueType.VALUE);
        PARSER.declareString(Builder::setDescription, DESCRIPTION);
        PARSER.declareString(Builder::setSnapshotId, SNAPSHOT_ID);
        PARSER.declareInt(Builder::setSnapshotDocCount, SNAPSHOT_DOC_COUNT);
        PARSER.declareObject(Builder::setModelSizeStats, ModelSizeStats.PARSER,
            ModelSizeStats.RESULT_TYPE_FIELD);
        PARSER.declareField(Builder::setLatestRecordTimeStamp,
            (p) -> TimeUtil.parseTimeField(p, LATEST_RECORD_TIME.getPreferredName()),
            LATEST_RECORD_TIME,
            ValueType.VALUE);
        PARSER.declareField(Builder::setLatestResultTimeStamp,
            (p) -> TimeUtil.parseTimeField(p, LATEST_RESULT_TIME.getPreferredName()),
            LATEST_RESULT_TIME,
            ValueType.VALUE);
        PARSER.declareObject(Builder::setQuantiles, Quantiles.PARSER, QUANTILES);
        PARSER.declareBoolean(Builder::setRetain, RETAIN);
    }


    private final String jobId;

    /**
     * The minimum version a node should have to be able
     * to read this model snapshot.
     */
    private final Version minVersion;

    private final Date timestamp;
    private final String description;
    private final String snapshotId;
    private final int snapshotDocCount;
    private final ModelSizeStats modelSizeStats;
    private final Date latestRecordTimeStamp;
    private final Date latestResultTimeStamp;
    private final Quantiles quantiles;
    private final boolean retain;


    private ModelSnapshot(String jobId, Version minVersion, Date timestamp, String description, String snapshotId, int snapshotDocCount,
                          ModelSizeStats modelSizeStats, Date latestRecordTimeStamp, Date latestResultTimeStamp, Quantiles quantiles,
                          boolean retain) {
        this.jobId = jobId;
        this.minVersion = minVersion;
        this.timestamp = timestamp;
        this.description = description;
        this.snapshotId = snapshotId;
        this.snapshotDocCount = snapshotDocCount;
        this.modelSizeStats = modelSizeStats;
        this.latestRecordTimeStamp = latestRecordTimeStamp;
        this.latestResultTimeStamp = latestResultTimeStamp;
        this.quantiles = quantiles;
        this.retain = retain;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(MIN_VERSION.getPreferredName(), minVersion);
        if (timestamp != null) {
            builder.timeField(TIMESTAMP.getPreferredName(), TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
        }
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        if (snapshotId != null) {
            builder.field(SNAPSHOT_ID.getPreferredName(), snapshotId);
        }
        builder.field(SNAPSHOT_DOC_COUNT.getPreferredName(), snapshotDocCount);
        if (modelSizeStats != null) {
            builder.field(ModelSizeStats.RESULT_TYPE_FIELD.getPreferredName(), modelSizeStats);
        }
        if (latestRecordTimeStamp != null) {
            builder.timeField(LATEST_RECORD_TIME.getPreferredName(), LATEST_RECORD_TIME.getPreferredName() + "_string",
                latestRecordTimeStamp.getTime());
        }
        if (latestResultTimeStamp != null) {
            builder.timeField(LATEST_RESULT_TIME.getPreferredName(), LATEST_RESULT_TIME.getPreferredName() + "_string",
                latestResultTimeStamp.getTime());
        }
        if (quantiles != null) {
            builder.field(QUANTILES.getPreferredName(), quantiles);
        }
        builder.field(RETAIN.getPreferredName(), retain);
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public Version getMinVersion() {
        return minVersion;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getDescription() {
        return description;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public int getSnapshotDocCount() {
        return snapshotDocCount;
    }

    public ModelSizeStats getModelSizeStats() {
        return modelSizeStats;
    }

    public Quantiles getQuantiles() {
        return quantiles;
    }

    public boolean getRetain() {
        return retain;
    }

    public Date getLatestRecordTimeStamp() {
        return latestRecordTimeStamp;
    }

    public Date getLatestResultTimeStamp() {
        return latestResultTimeStamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, minVersion, timestamp, description, snapshotId, quantiles, snapshotDocCount, modelSizeStats,
            latestRecordTimeStamp, latestResultTimeStamp, retain);
    }

    /**
     * Compare all the fields.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        ModelSnapshot that = (ModelSnapshot) other;

        return Objects.equals(this.jobId, that.jobId)
            && Objects.equals(this.minVersion, that.minVersion)
            && Objects.equals(this.timestamp, that.timestamp)
            && Objects.equals(this.description, that.description)
            && Objects.equals(this.snapshotId, that.snapshotId)
            && this.snapshotDocCount == that.snapshotDocCount
            && Objects.equals(this.modelSizeStats, that.modelSizeStats)
            && Objects.equals(this.quantiles, that.quantiles)
            && Objects.equals(this.latestRecordTimeStamp, that.latestRecordTimeStamp)
            && Objects.equals(this.latestResultTimeStamp, that.latestResultTimeStamp)
            && this.retain == that.retain;
    }

    public static class Builder {
        private String jobId;

        // Stored snapshot documents created prior to 6.3.0 will have no value for min_version.
        private Version minVersion = Version.fromString("6.3.0");

        private Date timestamp;
        private String description;
        private String snapshotId;
        private int snapshotDocCount;
        private ModelSizeStats modelSizeStats;
        private Date latestRecordTimeStamp;
        private Date latestResultTimeStamp;
        private Quantiles quantiles;
        private boolean retain;


        public Builder() {
        }

        public Builder(String jobId) {
            this.jobId = jobId;
        }

        public Builder(ModelSnapshot modelSnapshot) {
            this.jobId = modelSnapshot.jobId;
            this.timestamp = modelSnapshot.timestamp;
            this.description = modelSnapshot.description;
            this.snapshotId = modelSnapshot.snapshotId;
            this.snapshotDocCount = modelSnapshot.snapshotDocCount;
            this.modelSizeStats = modelSnapshot.modelSizeStats;
            this.latestRecordTimeStamp = modelSnapshot.latestRecordTimeStamp;
            this.latestResultTimeStamp = modelSnapshot.latestResultTimeStamp;
            this.quantiles = modelSnapshot.quantiles;
            this.retain = modelSnapshot.retain;
            this.minVersion = modelSnapshot.minVersion;
        }

        public Builder setJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        Builder setMinVersion(Version minVersion) {
            this.minVersion = minVersion;
            return this;
        }

        Builder setMinVersion(String minVersion) {
            this.minVersion = Version.fromString(minVersion);
            return this;
        }

        public Builder setTimestamp(Date timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setSnapshotId(String snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder setSnapshotDocCount(int snapshotDocCount) {
            this.snapshotDocCount = snapshotDocCount;
            return this;
        }

        public Builder setModelSizeStats(ModelSizeStats.Builder modelSizeStats) {
            this.modelSizeStats = modelSizeStats.build();
            return this;
        }

        public Builder setModelSizeStats(ModelSizeStats modelSizeStats) {
            this.modelSizeStats = modelSizeStats;
            return this;
        }

        public Builder setLatestRecordTimeStamp(Date latestRecordTimeStamp) {
            this.latestRecordTimeStamp = latestRecordTimeStamp;
            return this;
        }

        public Builder setLatestResultTimeStamp(Date latestResultTimeStamp) {
            this.latestResultTimeStamp = latestResultTimeStamp;
            return this;
        }

        public Builder setQuantiles(Quantiles quantiles) {
            this.quantiles = quantiles;
            return this;
        }

        public Builder setRetain(boolean value) {
            this.retain = value;
            return this;
        }

        public ModelSnapshot build() {
            return new ModelSnapshot(jobId, minVersion, timestamp, description, snapshotId, snapshotDocCount, modelSizeStats,
                latestRecordTimeStamp, latestResultTimeStamp, quantiles, retain);
        }
    }
}
