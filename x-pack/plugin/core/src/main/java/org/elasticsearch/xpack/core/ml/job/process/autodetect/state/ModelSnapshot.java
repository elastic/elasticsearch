/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * ModelSnapshot Result POJO
 */
public class ModelSnapshot implements ToXContentObject, Writeable {
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

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("model_snapshots");

    /**
     * Legacy type, now used only as a discriminant in the document ID
     */
    public static final ParseField TYPE = new ParseField("model_snapshot");

    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(TYPE.getPreferredName(), ignoreUnknownFields, Builder::new);

        parser.declareString(Builder::setJobId, Job.ID);
        parser.declareString(Builder::setMinVersion, MIN_VERSION);
        parser.declareField(
            Builder::setTimestamp,
            p -> TimeUtils.parseTimeField(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ValueType.VALUE
        );
        parser.declareString(Builder::setDescription, DESCRIPTION);
        parser.declareString(Builder::setSnapshotId, ModelSnapshotField.SNAPSHOT_ID);
        parser.declareInt(Builder::setSnapshotDocCount, SNAPSHOT_DOC_COUNT);
        parser.declareObject(
            Builder::setModelSizeStats,
            ignoreUnknownFields ? ModelSizeStats.LENIENT_PARSER : ModelSizeStats.STRICT_PARSER,
            ModelSizeStats.RESULT_TYPE_FIELD
        );
        parser.declareField(
            Builder::setLatestRecordTimeStamp,
            p -> TimeUtils.parseTimeField(p, LATEST_RECORD_TIME.getPreferredName()),
            LATEST_RECORD_TIME,
            ValueType.VALUE
        );
        parser.declareField(
            Builder::setLatestResultTimeStamp,
            p -> TimeUtils.parseTimeField(p, LATEST_RESULT_TIME.getPreferredName()),
            LATEST_RESULT_TIME,
            ValueType.VALUE
        );
        parser.declareObject(Builder::setQuantiles, ignoreUnknownFields ? Quantiles.LENIENT_PARSER : Quantiles.STRICT_PARSER, QUANTILES);
        parser.declareBoolean(Builder::setRetain, RETAIN);

        return parser;
    }

    public static String EMPTY_SNAPSHOT_ID = "empty";

    private final String jobId;

    /**
     * The minimum version a node should have to be able
     * to read this model snapshot.
     */
    private final MlConfigVersion minVersion;

    /**
     * This is model snapshot's creation wall clock time.
     * Use {@code latestResultTimeStamp} if you need model time instead.
     */
    private final Date timestamp;
    private final String description;
    private final String snapshotId;
    private final int snapshotDocCount;
    private final ModelSizeStats modelSizeStats;
    private final Date latestRecordTimeStamp;
    private final Date latestResultTimeStamp;
    private final Quantiles quantiles;
    private final boolean retain;

    private ModelSnapshot(
        String jobId,
        MlConfigVersion minVersion,
        Date timestamp,
        String description,
        String snapshotId,
        int snapshotDocCount,
        ModelSizeStats modelSizeStats,
        Date latestRecordTimeStamp,
        Date latestResultTimeStamp,
        Quantiles quantiles,
        boolean retain
    ) {
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

    public ModelSnapshot(StreamInput in) throws IOException {
        jobId = in.readString();
        minVersion = MlConfigVersion.readVersion(in);
        timestamp = in.readBoolean() ? new Date(in.readVLong()) : null;
        description = in.readOptionalString();
        snapshotId = in.readOptionalString();
        snapshotDocCount = in.readInt();
        modelSizeStats = in.readOptionalWriteable(ModelSizeStats::new);
        latestRecordTimeStamp = in.readBoolean() ? new Date(in.readVLong()) : null;
        latestResultTimeStamp = in.readBoolean() ? new Date(in.readVLong()) : null;
        quantiles = in.readOptionalWriteable(Quantiles::new);
        retain = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        MlConfigVersion.writeVersion(minVersion, out);
        if (timestamp != null) {
            out.writeBoolean(true);
            out.writeVLong(timestamp.getTime());
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(description);
        out.writeOptionalString(snapshotId);
        out.writeInt(snapshotDocCount);
        out.writeOptionalWriteable(modelSizeStats);
        if (latestRecordTimeStamp != null) {
            out.writeBoolean(true);
            out.writeVLong(latestRecordTimeStamp.getTime());
        } else {
            out.writeBoolean(false);
        }
        if (latestResultTimeStamp != null) {
            out.writeBoolean(true);
            out.writeVLong(latestResultTimeStamp.getTime());
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalWriteable(quantiles);
        out.writeBoolean(retain);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(MIN_VERSION.getPreferredName(), minVersion);
        if (timestamp != null) {
            builder.timestampFieldsFromUnixEpochMillis(
                TIMESTAMP.getPreferredName(),
                TIMESTAMP.getPreferredName() + "_string",
                timestamp.getTime()
            );
        }
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        if (snapshotId != null) {
            builder.field(ModelSnapshotField.SNAPSHOT_ID.getPreferredName(), snapshotId);
        }
        builder.field(SNAPSHOT_DOC_COUNT.getPreferredName(), snapshotDocCount);
        if (modelSizeStats != null) {
            builder.field(ModelSizeStats.RESULT_TYPE_FIELD.getPreferredName(), modelSizeStats);
        }
        if (latestRecordTimeStamp != null) {
            builder.timestampFieldsFromUnixEpochMillis(
                LATEST_RECORD_TIME.getPreferredName(),
                LATEST_RECORD_TIME.getPreferredName() + "_string",
                latestRecordTimeStamp.getTime()
            );
        }
        if (latestResultTimeStamp != null) {
            builder.timestampFieldsFromUnixEpochMillis(
                LATEST_RESULT_TIME.getPreferredName(),
                LATEST_RESULT_TIME.getPreferredName() + "_string",
                latestResultTimeStamp.getTime()
            );
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

    public MlConfigVersion getMinVersion() {
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

    public Date getLatestRecordTimeStamp() {
        return latestRecordTimeStamp;
    }

    public Date getLatestResultTimeStamp() {
        return latestResultTimeStamp;
    }

    public boolean isRetain() {
        return retain;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            minVersion,
            timestamp,
            description,
            snapshotId,
            quantiles,
            snapshotDocCount,
            modelSizeStats,
            latestRecordTimeStamp,
            latestResultTimeStamp,
            retain
        );
    }

    /**
     * Compare all the fields.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof ModelSnapshot == false) {
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

    public List<String> stateDocumentIds() {
        List<String> stateDocumentIds = new ArrayList<>(snapshotDocCount);
        // The state documents count suffices are 1-based
        for (int i = 1; i <= snapshotDocCount; i++) {
            stateDocumentIds.add(ModelState.documentId(jobId, snapshotId, i));
        }
        return stateDocumentIds;
    }

    public boolean isTheEmptySnapshot() {
        return isTheEmptySnapshot(snapshotId);
    }

    public static boolean isTheEmptySnapshot(String snapshotId) {
        return EMPTY_SNAPSHOT_ID.equals(snapshotId);
    }

    public static String documentIdPrefix(String jobId) {
        return jobId + "_" + TYPE + "_";
    }

    public static String annotationDocumentId(ModelSnapshot snapshot) {
        return "annotation_for_" + documentId(snapshot);
    }

    public static String documentId(ModelSnapshot snapshot) {
        return documentId(snapshot.getJobId(), snapshot.getSnapshotId());
    }

    /**
     * This is how the IDs were formed in v5.4
     */
    public static String v54DocumentId(ModelSnapshot snapshot) {
        return v54DocumentId(snapshot.getJobId(), snapshot.getSnapshotId());
    }

    public static String documentId(String jobId, String snapshotId) {
        return documentIdPrefix(jobId) + snapshotId;
    }

    /**
     * This is how the IDs were formed in v5.4
     */
    public static String v54DocumentId(String jobId, String snapshotId) {
        return jobId + "-" + snapshotId;
    }

    public static ModelSnapshot fromJson(BytesReference bytesReference) {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                bytesReference,
                XContentType.JSON
            )
        ) {
            return LENIENT_PARSER.apply(parser, null).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse modelSnapshot", e);
        }
    }

    public static class Builder {
        private String jobId;

        // Stored snapshot documents created prior to 6.3.0 will have no
        // value for min_version.
        private MlConfigVersion minVersion = MlConfigVersion.fromString("6.3.0");

        private Date timestamp;
        private String description;
        private String snapshotId;
        private int snapshotDocCount;
        private ModelSizeStats modelSizeStats;
        private Date latestRecordTimeStamp;
        private Date latestResultTimeStamp;
        private Quantiles quantiles;
        private boolean retain;

        public Builder() {}

        public Builder(String jobId) {
            this();
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

        public Builder setMinVersion(MlConfigVersion minVersion) {
            this.minVersion = minVersion;
            return this;
        }

        public Builder setMinVersion(String minVersion) {
            this.minVersion = MlConfigVersion.fromString(minVersion);
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
            return new ModelSnapshot(
                jobId,
                minVersion,
                timestamp,
                description,
                snapshotId,
                snapshotDocCount,
                modelSizeStats,
                latestRecordTimeStamp,
                latestResultTimeStamp,
                quantiles,
                retain
            );
        }
    }

    public static ModelSnapshot emptySnapshot(String jobId) {
        return new ModelSnapshot(
            jobId,
            MlConfigVersion.CURRENT,
            new Date(),
            "empty snapshot",
            EMPTY_SNAPSHOT_ID,
            0,
            new ModelSizeStats.Builder(jobId).build(),
            null,
            null,
            null,
            false
        );
    }
}
