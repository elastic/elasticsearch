/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Anomaly Record POJO.
 * Uses the object wrappers Boolean and Double so <code>null</code> values
 * can be returned if the members have not been set.
 */
public class AnomalyRecord implements ToXContentObject, Writeable {
    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "record";
    /**
     * Result fields (all detector types)
     */
    public static final ParseField PROBABILITY = new ParseField("probability");
    public static final ParseField MULTI_BUCKET_IMPACT = new ParseField("multi_bucket_impact");
    public static final ParseField BY_FIELD_NAME = new ParseField("by_field_name");
    public static final ParseField BY_FIELD_VALUE = new ParseField("by_field_value");
    public static final ParseField CORRELATED_BY_FIELD_VALUE = new ParseField("correlated_by_field_value");
    public static final ParseField PARTITION_FIELD_NAME = new ParseField("partition_field_name");
    public static final ParseField PARTITION_FIELD_VALUE = new ParseField("partition_field_value");
    public static final ParseField FUNCTION = new ParseField("function");
    public static final ParseField FUNCTION_DESCRIPTION = new ParseField("function_description");
    public static final ParseField TYPICAL = new ParseField("typical");
    public static final ParseField ACTUAL = new ParseField("actual");
    public static final ParseField INFLUENCERS = new ParseField("influencers");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField GEO_RESULTS = new ParseField("geo_results");
    public static final ParseField ANOMALY_SCORE_EXPLANATION = new ParseField("anomaly_score_explanation");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("records");

    /**
     * Metric Results (including population metrics)
     */
    public static final ParseField FIELD_NAME = new ParseField("field_name");

    /**
     * Population results
     */
    public static final ParseField OVER_FIELD_NAME = new ParseField("over_field_name");
    public static final ParseField OVER_FIELD_VALUE = new ParseField("over_field_value");
    public static final ParseField CAUSES = new ParseField("causes");

    /**
     * Normalization
     */
    public static final ParseField RECORD_SCORE = new ParseField("record_score");
    public static final ParseField INITIAL_RECORD_SCORE = new ParseField("initial_record_score");

    public static final ConstructingObjectParser<AnomalyRecord, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<AnomalyRecord, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<AnomalyRecord, Void> createParser(boolean ignoreUnknownFields) {
        // As a record contains fields named after the data fields, the parser for the record should always ignore unknown fields.
        // However, it makes sense to offer strict/lenient parsing for other members, e.g. influences, anomaly causes, etc.
        ConstructingObjectParser<AnomalyRecord, Void> parser = new ConstructingObjectParser<>(
            RESULT_TYPE_VALUE,
            true,
            a -> new AnomalyRecord((String) a[0], (Date) a[1], (long) a[2])
        );

        parser.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        parser.declareField(
            ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeField(p, Result.TIMESTAMP.getPreferredName()),
            Result.TIMESTAMP,
            ValueType.VALUE
        );
        parser.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        parser.declareString((anomalyRecord, s) -> {}, Result.RESULT_TYPE);
        parser.declareDouble(AnomalyRecord::setProbability, PROBABILITY);
        parser.declareDouble(AnomalyRecord::setMultiBucketImpact, MULTI_BUCKET_IMPACT);
        parser.declareDouble(AnomalyRecord::setRecordScore, RECORD_SCORE);
        parser.declareDouble(AnomalyRecord::setInitialRecordScore, INITIAL_RECORD_SCORE);
        parser.declareInt(AnomalyRecord::setDetectorIndex, Detector.DETECTOR_INDEX);
        parser.declareBoolean(AnomalyRecord::setInterim, Result.IS_INTERIM);
        parser.declareString(AnomalyRecord::setByFieldName, BY_FIELD_NAME);
        parser.declareString(AnomalyRecord::setByFieldValue, BY_FIELD_VALUE);
        parser.declareString(AnomalyRecord::setCorrelatedByFieldValue, CORRELATED_BY_FIELD_VALUE);
        parser.declareString(AnomalyRecord::setPartitionFieldName, PARTITION_FIELD_NAME);
        parser.declareString(AnomalyRecord::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        parser.declareString(AnomalyRecord::setFunction, FUNCTION);
        parser.declareString(AnomalyRecord::setFunctionDescription, FUNCTION_DESCRIPTION);
        parser.declareDoubleArray(AnomalyRecord::setTypical, TYPICAL);
        parser.declareDoubleArray(AnomalyRecord::setActual, ACTUAL);
        parser.declareString(AnomalyRecord::setFieldName, FIELD_NAME);
        parser.declareString(AnomalyRecord::setOverFieldName, OVER_FIELD_NAME);
        parser.declareString(AnomalyRecord::setOverFieldValue, OVER_FIELD_VALUE);
        parser.declareObjectArray(
            AnomalyRecord::setCauses,
            ignoreUnknownFields ? AnomalyCause.LENIENT_PARSER : AnomalyCause.STRICT_PARSER,
            CAUSES
        );
        parser.declareObjectArray(
            AnomalyRecord::setInfluencers,
            ignoreUnknownFields ? Influence.LENIENT_PARSER : Influence.STRICT_PARSER,
            INFLUENCERS
        );
        parser.declareObject(
            AnomalyRecord::setGeoResults,
            ignoreUnknownFields ? GeoResults.LENIENT_PARSER : GeoResults.STRICT_PARSER,
            GEO_RESULTS
        );
        parser.declareObject(
            AnomalyRecord::setAnomalyScoreExplanation,
            ignoreUnknownFields ? AnomalyScoreExplanation.LENIENT_PARSER : AnomalyScoreExplanation.STRICT_PARSER,
            ANOMALY_SCORE_EXPLANATION
        );

        return parser;
    }

    private final String jobId;
    private int detectorIndex;
    private double probability;
    private Double multiBucketImpact;
    private String byFieldName;
    private String byFieldValue;
    private String correlatedByFieldValue;
    private String partitionFieldName;
    private String partitionFieldValue;
    private String function;
    private String functionDescription;
    private List<Double> typical;
    private List<Double> actual;
    private boolean isInterim;
    private GeoResults geoResults;

    private AnomalyScoreExplanation anomalyScoreExplanation;
    private String fieldName;

    private String overFieldName;
    private String overFieldValue;
    private List<AnomalyCause> causes;

    private double recordScore;

    private double initialRecordScore;

    private final Date timestamp;
    private final long bucketSpan;

    private List<Influence> influences;

    public AnomalyRecord(String jobId, Date timestamp, long bucketSpan) {
        this.jobId = jobId;
        this.timestamp = ExceptionsHelper.requireNonNull(timestamp, Result.TIMESTAMP.getPreferredName());
        this.bucketSpan = bucketSpan;
    }

    @SuppressWarnings("unchecked")
    public AnomalyRecord(StreamInput in) throws IOException {
        jobId = in.readString();
        detectorIndex = in.readInt();
        probability = in.readDouble();

        multiBucketImpact = in.readOptionalDouble();
        byFieldName = in.readOptionalString();
        byFieldValue = in.readOptionalString();
        correlatedByFieldValue = in.readOptionalString();
        partitionFieldName = in.readOptionalString();
        partitionFieldValue = in.readOptionalString();
        function = in.readOptionalString();
        functionDescription = in.readOptionalString();
        fieldName = in.readOptionalString();
        overFieldName = in.readOptionalString();
        overFieldValue = in.readOptionalString();
        if (in.readBoolean()) {
            typical = (List<Double>) in.readGenericValue();
        }
        if (in.readBoolean()) {
            actual = (List<Double>) in.readGenericValue();
        }
        isInterim = in.readBoolean();
        if (in.readBoolean()) {
            causes = in.readList(AnomalyCause::new);
        }
        recordScore = in.readDouble();
        initialRecordScore = in.readDouble();
        timestamp = new Date(in.readLong());
        bucketSpan = in.readLong();
        if (in.readBoolean()) {
            influences = in.readList(Influence::new);
        }
        geoResults = in.readOptionalWriteable(GeoResults::new);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            anomalyScoreExplanation = in.readOptionalWriteable(AnomalyScoreExplanation::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeInt(detectorIndex);
        out.writeDouble(probability);
        out.writeOptionalDouble(multiBucketImpact);
        out.writeOptionalString(byFieldName);
        out.writeOptionalString(byFieldValue);
        out.writeOptionalString(correlatedByFieldValue);
        out.writeOptionalString(partitionFieldName);
        out.writeOptionalString(partitionFieldValue);
        out.writeOptionalString(function);
        out.writeOptionalString(functionDescription);
        out.writeOptionalString(fieldName);
        out.writeOptionalString(overFieldName);
        out.writeOptionalString(overFieldValue);
        boolean hasTypical = typical != null;
        out.writeBoolean(hasTypical);
        if (hasTypical) {
            out.writeGenericValue(typical);
        }
        boolean hasActual = actual != null;
        out.writeBoolean(hasActual);
        if (hasActual) {
            out.writeGenericValue(actual);
        }
        out.writeBoolean(isInterim);
        boolean hasCauses = causes != null;
        out.writeBoolean(hasCauses);
        if (hasCauses) {
            out.writeList(causes);
        }
        out.writeDouble(recordScore);
        out.writeDouble(initialRecordScore);
        out.writeLong(timestamp.getTime());
        out.writeLong(bucketSpan);
        boolean hasInfluencers = influences != null;
        out.writeBoolean(hasInfluencers);
        if (hasInfluencers) {
            out.writeList(influences);
        }
        out.writeOptionalWriteable(geoResults);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            out.writeOptionalWriteable(anomalyScoreExplanation);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(PROBABILITY.getPreferredName(), probability);
        if (multiBucketImpact != null) {
            builder.field(MULTI_BUCKET_IMPACT.getPreferredName(), multiBucketImpact);
        }
        builder.field(RECORD_SCORE.getPreferredName(), recordScore);
        builder.field(INITIAL_RECORD_SCORE.getPreferredName(), initialRecordScore);
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(Detector.DETECTOR_INDEX.getPreferredName(), detectorIndex);
        builder.field(Result.IS_INTERIM.getPreferredName(), isInterim);
        builder.timeField(Result.TIMESTAMP.getPreferredName(), Result.TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
        if (byFieldName != null) {
            builder.field(BY_FIELD_NAME.getPreferredName(), byFieldName);
        }
        if (byFieldValue != null) {
            builder.field(BY_FIELD_VALUE.getPreferredName(), byFieldValue);
        }
        if (correlatedByFieldValue != null) {
            builder.field(CORRELATED_BY_FIELD_VALUE.getPreferredName(), correlatedByFieldValue);
        }
        if (partitionFieldName != null) {
            builder.field(PARTITION_FIELD_NAME.getPreferredName(), partitionFieldName);
        }
        if (partitionFieldValue != null) {
            builder.field(PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue);
        }
        if (function != null) {
            builder.field(FUNCTION.getPreferredName(), function);
        }
        if (functionDescription != null) {
            builder.field(FUNCTION_DESCRIPTION.getPreferredName(), functionDescription);
        }
        if (typical != null) {
            builder.field(TYPICAL.getPreferredName(), typical);
        }
        if (actual != null) {
            builder.field(ACTUAL.getPreferredName(), actual);
        }
        if (fieldName != null) {
            builder.field(FIELD_NAME.getPreferredName(), fieldName);
        }
        if (overFieldName != null) {
            builder.field(OVER_FIELD_NAME.getPreferredName(), overFieldName);
        }
        if (overFieldValue != null) {
            builder.field(OVER_FIELD_VALUE.getPreferredName(), overFieldValue);
        }
        if (causes != null) {
            builder.field(CAUSES.getPreferredName(), causes);
        }
        if (influences != null) {
            builder.field(INFLUENCERS.getPreferredName(), influences);
        }
        if (geoResults != null) {
            builder.field(GEO_RESULTS.getPreferredName(), geoResults);
        }
        if (anomalyScoreExplanation != null) {
            builder.field(ANOMALY_SCORE_EXPLANATION.getPreferredName(), anomalyScoreExplanation);
        }

        Map<String, LinkedHashSet<String>> inputFields = inputFieldMap();
        for (String inputFieldName : inputFields.keySet()) {
            builder.field(inputFieldName, inputFields.get(inputFieldName));
        }
        builder.endObject();
        return builder;
    }

    private Map<String, LinkedHashSet<String>> inputFieldMap() {
        // LinkedHashSet preserves insertion order when iterating entries
        Map<String, LinkedHashSet<String>> result = new HashMap<>();

        addInputFieldsToMap(result, byFieldName, byFieldValue);
        addInputFieldsToMap(result, overFieldName, overFieldValue);
        addInputFieldsToMap(result, partitionFieldName, partitionFieldValue);

        if (influences != null) {
            for (Influence inf : influences) {
                String influencerFieldName = inf.getInfluencerFieldName();
                for (String fieldValue : inf.getInfluencerFieldValues()) {
                    addInputFieldsToMap(result, influencerFieldName, fieldValue);
                }
            }
        }
        return result;
    }

    private static void addInputFieldsToMap(Map<String, LinkedHashSet<String>> inputFields, String inputFieldName, String fieldValue) {
        if (Strings.isNullOrEmpty(inputFieldName) == false && fieldValue != null) {
            if (ReservedFieldNames.isValidFieldName(inputFieldName)) {
                inputFields.computeIfAbsent(inputFieldName, k -> new LinkedHashSet<>()).add(fieldValue);
            }
        }
    }

    public String getJobId() {
        return this.jobId;
    }

    /**
     * Data store ID of this record.
     */
    public String getId() {
        return buildId(jobId, timestamp, bucketSpan, detectorIndex, byFieldValue, overFieldValue, partitionFieldValue);
    }

    static String buildId(
        String jobId,
        Date timestamp,
        long bucketSpan,
        int detectorIndex,
        String byFieldValue,
        String overFieldValue,
        String partitionFieldValue
    ) {
        return jobId
            + "_record_"
            + timestamp.getTime()
            + "_"
            + bucketSpan
            + "_"
            + detectorIndex
            + "_"
            + MachineLearningField.valuesToId(byFieldValue, overFieldValue, partitionFieldValue);
    }

    public int getDetectorIndex() {
        return detectorIndex;
    }

    public void setDetectorIndex(int detectorIndex) {
        this.detectorIndex = detectorIndex;
    }

    public double getRecordScore() {
        return recordScore;
    }

    public void setRecordScore(double recordScore) {
        this.recordScore = recordScore;
    }

    public double getInitialRecordScore() {
        return initialRecordScore;
    }

    public void setInitialRecordScore(double initialRecordScore) {
        this.initialRecordScore = initialRecordScore;
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

    public double getProbability() {
        return probability;
    }

    public void setProbability(double value) {
        probability = value;
    }

    public double getMultiBucketImpact() {
        return multiBucketImpact;
    }

    public void setMultiBucketImpact(double value) {
        multiBucketImpact = value;
    }

    public String getByFieldName() {
        return byFieldName;
    }

    public void setByFieldName(String value) {
        byFieldName = value.intern();
    }

    public String getByFieldValue() {
        return byFieldValue;
    }

    public void setByFieldValue(String value) {
        byFieldValue = value.intern();
    }

    public String getCorrelatedByFieldValue() {
        return correlatedByFieldValue;
    }

    public void setCorrelatedByFieldValue(String value) {
        correlatedByFieldValue = value.intern();
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    public void setPartitionFieldName(String field) {
        partitionFieldName = field.intern();
    }

    public String getPartitionFieldValue() {
        return partitionFieldValue;
    }

    public void setPartitionFieldValue(String value) {
        partitionFieldValue = value.intern();
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String name) {
        function = name.intern();
    }

    public String getFunctionDescription() {
        return functionDescription;
    }

    public void setFunctionDescription(String functionDescription) {
        this.functionDescription = functionDescription.intern();
    }

    public List<Double> getTypical() {
        return typical;
    }

    public void setTypical(List<Double> typical) {
        this.typical = typical;
    }

    public List<Double> getActual() {
        return actual;
    }

    public void setActual(List<Double> actual) {
        this.actual = actual;
    }

    public boolean isInterim() {
        return isInterim;
    }

    public void setInterim(boolean interim) {
        this.isInterim = interim;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String field) {
        fieldName = field.intern();
    }

    public String getOverFieldName() {
        return overFieldName;
    }

    public void setOverFieldName(String name) {
        overFieldName = name.intern();
    }

    public String getOverFieldValue() {
        return overFieldValue;
    }

    public void setOverFieldValue(String value) {
        overFieldValue = value.intern();
    }

    public List<AnomalyCause> getCauses() {
        return causes;
    }

    public void setCauses(List<AnomalyCause> causes) {
        this.causes = causes;
    }

    public void addCause(AnomalyCause cause) {
        if (causes == null) {
            causes = new ArrayList<>();
        }
        causes.add(cause);
    }

    public List<Influence> getInfluencers() {
        return influences;
    }

    public void setInfluencers(List<Influence> influencers) {
        this.influences = influencers;
    }

    public GeoResults getGeoResults() {
        return geoResults;
    }

    public void setGeoResults(GeoResults geoResults) {
        this.geoResults = geoResults;
    }

    public AnomalyScoreExplanation getAnomalyScoreExplanation() {
        return anomalyScoreExplanation;
    }

    public void setAnomalyScoreExplanation(AnomalyScoreExplanation anomalyScoreExplanation) {
        this.anomalyScoreExplanation = anomalyScoreExplanation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            detectorIndex,
            bucketSpan,
            probability,
            multiBucketImpact,
            recordScore,
            initialRecordScore,
            typical,
            actual,
            function,
            functionDescription,
            fieldName,
            byFieldName,
            byFieldValue,
            correlatedByFieldValue,
            partitionFieldName,
            partitionFieldValue,
            overFieldName,
            overFieldValue,
            timestamp,
            isInterim,
            causes,
            influences,
            jobId,
            geoResults,
            anomalyScoreExplanation
        );
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof AnomalyRecord == false) {
            return false;
        }

        AnomalyRecord that = (AnomalyRecord) other;

        return Objects.equals(this.jobId, that.jobId)
            && this.detectorIndex == that.detectorIndex
            && this.bucketSpan == that.bucketSpan
            && this.probability == that.probability
            && Objects.equals(this.multiBucketImpact, that.multiBucketImpact)
            && this.recordScore == that.recordScore
            && this.initialRecordScore == that.initialRecordScore
            && Objects.deepEquals(this.typical, that.typical)
            && Objects.deepEquals(this.actual, that.actual)
            && Objects.equals(this.function, that.function)
            && Objects.equals(this.functionDescription, that.functionDescription)
            && Objects.equals(this.fieldName, that.fieldName)
            && Objects.equals(this.byFieldName, that.byFieldName)
            && Objects.equals(this.byFieldValue, that.byFieldValue)
            && Objects.equals(this.correlatedByFieldValue, that.correlatedByFieldValue)
            && Objects.equals(this.partitionFieldName, that.partitionFieldName)
            && Objects.equals(this.partitionFieldValue, that.partitionFieldValue)
            && Objects.equals(this.overFieldName, that.overFieldName)
            && Objects.equals(this.overFieldValue, that.overFieldValue)
            && Objects.equals(this.timestamp, that.timestamp)
            && Objects.equals(this.isInterim, that.isInterim)
            && Objects.equals(this.causes, that.causes)
            && Objects.equals(this.geoResults, that.geoResults)
            && Objects.equals(this.anomalyScoreExplanation, that.anomalyScoreExplanation)
            && Objects.equals(this.influences, that.influences);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
