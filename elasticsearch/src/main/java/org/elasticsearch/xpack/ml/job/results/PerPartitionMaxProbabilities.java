/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * When per-partition normalization is enabled this class represents
 * the max anomalous probabilities of each partition per bucket. These values
 * calculated from the bucket's anomaly records.
 */
public class PerPartitionMaxProbabilities extends ToXContentToBytes implements Writeable {

    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "partition_normalized_probs";

    /*
     * Field Names
     */
    public static final ParseField PER_PARTITION_MAX_PROBABILITIES = new ParseField("per_partition_max_probabilities");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<PerPartitionMaxProbabilities, Void> PARSER =
            new ConstructingObjectParser<>(RESULT_TYPE_VALUE, a ->
                    new PerPartitionMaxProbabilities((String) a[0], (Date) a[1], (long) a[2], (List<PartitionProbability>) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return new Date(p.longValue());
            } else if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return new Date(TimeUtils.dateStringToEpoch(p.text()));
            }
            throw new IllegalArgumentException(
                    "unexpected token [" + p.currentToken() + "] for [" + Bucket.TIMESTAMP.getPreferredName() + "]");
        }, Bucket.TIMESTAMP, ObjectParser.ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), Bucket.BUCKET_SPAN);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), PartitionProbability.PARSER, PER_PARTITION_MAX_PROBABILITIES);
        PARSER.declareString((p, s) -> {}, Result.RESULT_TYPE);
    }

    private final String jobId;
    private final Date timestamp;
    private final long bucketSpan;
    private final List<PartitionProbability> perPartitionMaxProbabilities;

    public PerPartitionMaxProbabilities(String jobId, Date timestamp, long bucketSpan,
                                        List<PartitionProbability> partitionProbabilities) {
        this.jobId = jobId;
        this.timestamp = timestamp;
        this.bucketSpan = bucketSpan;
        this.perPartitionMaxProbabilities = partitionProbabilities;
    }

    public PerPartitionMaxProbabilities(List<AnomalyRecord> records) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("PerPartitionMaxProbabilities cannot be created from an empty list of records");
        }
        this.jobId = records.get(0).getJobId();
        this.timestamp = records.get(0).getTimestamp();
        this.bucketSpan = records.get(0).getBucketSpan();
        this.perPartitionMaxProbabilities = calcMaxNormalizedProbabilityPerPartition(records);
    }

    public PerPartitionMaxProbabilities(StreamInput in) throws IOException {
        jobId = in.readString();
        timestamp = new Date(in.readLong());
        bucketSpan = in.readLong();
        perPartitionMaxProbabilities = in.readList(PartitionProbability::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeLong(timestamp.getTime());
        out.writeLong(bucketSpan);
        out.writeList(perPartitionMaxProbabilities);
    }

    public String getJobId() {
        return jobId;
    }

    public String getId() {
        return jobId + "_" + timestamp.getTime() + "_" + bucketSpan + "_" + RESULT_TYPE_VALUE;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public List<PartitionProbability> getPerPartitionMaxProbabilities() {
        return perPartitionMaxProbabilities;
    }

    public double getMaxProbabilityForPartition(String partitionValue) {
        Optional<PartitionProbability> first =
                perPartitionMaxProbabilities.stream().filter(pp -> partitionValue.equals(pp.getPartitionValue())).findFirst();

        return first.isPresent() ? first.get().getMaxNormalizedProbability() : 0.0;
    }

    /**
     * Box class for the stream collector function below
     */
    private final class DoubleMaxBox {
        private double value = 0.0;

        public DoubleMaxBox() {
        }

        public void accept(double d) {
            if (d > value) {
                value = d;
            }
        }

        public DoubleMaxBox combine(DoubleMaxBox other) {
            return (this.value > other.value) ? this : other;
        }

        public Double value() {
            return this.value;
        }
    }

    private List<PartitionProbability> calcMaxNormalizedProbabilityPerPartition(List<AnomalyRecord> anomalyRecords) {
        Map<String, Double> maxValueByPartition = anomalyRecords.stream().collect(
                Collectors.groupingBy(AnomalyRecord::getPartitionFieldValue,
                Collector.of(DoubleMaxBox::new, (m, ar) -> m.accept(ar.getNormalizedProbability()),
                        DoubleMaxBox::combine, DoubleMaxBox::value)));

        List<PartitionProbability> pProbs = new ArrayList<>();
        for (Map.Entry<String, Double> entry : maxValueByPartition.entrySet()) {
            pProbs.add(new PartitionProbability(entry.getKey(), entry.getValue()));
        }

        return pProbs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Bucket.TIMESTAMP.getPreferredName(), timestamp.getTime());
        builder.field(Bucket.BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(PER_PARTITION_MAX_PROBABILITIES.getPreferredName(), perPartitionMaxProbabilities);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, perPartitionMaxProbabilities, bucketSpan);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof PerPartitionMaxProbabilities == false) {
            return false;
        }

        PerPartitionMaxProbabilities that = (PerPartitionMaxProbabilities) other;

        return Objects.equals(this.jobId, that.jobId)
                && Objects.equals(this.timestamp, that.timestamp)
                && this.bucketSpan == that.bucketSpan
                && Objects.equals(this.perPartitionMaxProbabilities, that.perPartitionMaxProbabilities);
    }

    /**
     * Class for partitionValue, maxNormalizedProb pairs
     */
    public static class PartitionProbability extends ToXContentToBytes implements Writeable  {

        public static final ConstructingObjectParser<PartitionProbability, Void> PARSER =
                new ConstructingObjectParser<>("partitionProbability",
                        a -> new PartitionProbability((String) a[0], (double) a[1]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), AnomalyRecord.PARTITION_FIELD_VALUE);
            PARSER.declareDouble(ConstructingObjectParser.constructorArg(), Bucket.MAX_NORMALIZED_PROBABILITY);
        }

        private final String partitionValue;
        private final double maxNormalizedProbability;

        PartitionProbability(String partitionName, double maxNormalizedProbability) {
            this.partitionValue = partitionName;
            this.maxNormalizedProbability = maxNormalizedProbability;
        }

        public PartitionProbability(StreamInput in) throws IOException {
            partitionValue = in.readString();
            maxNormalizedProbability = in.readDouble();
        }

        public String getPartitionValue() {
            return partitionValue;
        }

        public double getMaxNormalizedProbability() {
            return maxNormalizedProbability;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(partitionValue);
            out.writeDouble(maxNormalizedProbability);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject()
                    .field(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(), partitionValue)
                    .field(Bucket.MAX_NORMALIZED_PROBABILITY.getPreferredName(), maxNormalizedProbability)
                    .endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionValue, maxNormalizedProbability);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other instanceof PartitionProbability == false) {
                return false;
            }

            PartitionProbability that = (PartitionProbability) other;

            return Objects.equals(this.partitionValue, that.partitionValue)
                    && this.maxNormalizedProbability == that.maxNormalizedProbability;
        }
    }
}


