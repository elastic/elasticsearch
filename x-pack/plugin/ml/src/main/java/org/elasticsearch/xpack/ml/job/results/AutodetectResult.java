/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class AutodetectResult implements ToXContentObject, Writeable {

    public static final ParseField TYPE = new ParseField("autodetect_result");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<AutodetectResult, Void> PARSER = new ConstructingObjectParser<>(
            TYPE.getPreferredName(), a -> new AutodetectResult((Bucket) a[0], (List<AnomalyRecord>) a[1], (List<Influencer>) a[2],
                    (Quantiles) a[3], a[4] == null ? null : ((ModelSnapshot.Builder) a[4]).build(),
                    a[5] == null ? null : ((ModelSizeStats.Builder) a[5]).build(), (ModelPlot) a[6],
                    (Forecast) a[7], (ForecastRequestStats) a[8], (CategoryDefinition) a[9], (FlushAcknowledgement) a[10]));

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Bucket.STRICT_PARSER, Bucket.RESULT_TYPE_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), AnomalyRecord.STRICT_PARSER,
                AnomalyRecord.RESULTS_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), Influencer.LENIENT_PARSER, Influencer.RESULTS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Quantiles.STRICT_PARSER, Quantiles.TYPE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ModelSnapshot.STRICT_PARSER, ModelSnapshot.TYPE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ModelSizeStats.STRICT_PARSER,
                ModelSizeStats.RESULT_TYPE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ModelPlot.STRICT_PARSER, ModelPlot.RESULTS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Forecast.STRICT_PARSER, Forecast.RESULTS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ForecastRequestStats.STRICT_PARSER,
                ForecastRequestStats.RESULTS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), CategoryDefinition.STRICT_PARSER, CategoryDefinition.TYPE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), FlushAcknowledgement.PARSER, FlushAcknowledgement.TYPE);
    }

    private final Bucket bucket;
    private final List<AnomalyRecord> records;
    private final List<Influencer> influencers;
    private final Quantiles quantiles;
    private final ModelSnapshot modelSnapshot;
    private final ModelSizeStats modelSizeStats;
    private final ModelPlot modelPlot;
    private final Forecast forecast;
    private final ForecastRequestStats forecastRequestStats;
    private final CategoryDefinition categoryDefinition;
    private final FlushAcknowledgement flushAcknowledgement;

    public AutodetectResult(Bucket bucket, List<AnomalyRecord> records, List<Influencer> influencers, Quantiles quantiles,
            ModelSnapshot modelSnapshot, ModelSizeStats modelSizeStats, ModelPlot modelPlot, Forecast forecast,
            ForecastRequestStats forecastRequestStats, CategoryDefinition categoryDefinition, FlushAcknowledgement flushAcknowledgement) {
        this.bucket = bucket;
        this.records = records;
        this.influencers = influencers;
        this.quantiles = quantiles;
        this.modelSnapshot = modelSnapshot;
        this.modelSizeStats = modelSizeStats;
        this.modelPlot = modelPlot;
        this.forecast = forecast;
        this.forecastRequestStats = forecastRequestStats;
        this.categoryDefinition = categoryDefinition;
        this.flushAcknowledgement = flushAcknowledgement;
    }

    public AutodetectResult(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            this.bucket = new Bucket(in);
        } else {
            this.bucket = null;
        }
        if (in.readBoolean()) {
            this.records = in.readList(AnomalyRecord::new);
        } else {
            this.records = null;
        }
        if (in.readBoolean()) {
            this.influencers = in.readList(Influencer::new);
        } else {
            this.influencers = null;
        }
        if (in.readBoolean()) {
            this.quantiles = new Quantiles(in);
        } else {
            this.quantiles = null;
        }
        if (in.readBoolean()) {
            this.modelSnapshot = new ModelSnapshot(in);
        } else {
            this.modelSnapshot = null;
        }
        if (in.readBoolean()) {
            this.modelSizeStats = new ModelSizeStats(in);
        } else {
            this.modelSizeStats = null;
        }
        if (in.readBoolean()) {
            this.modelPlot = new ModelPlot(in);
        } else {
            this.modelPlot = null;
        }
        if (in.readBoolean()) {
            this.categoryDefinition = new CategoryDefinition(in);
        } else {
            this.categoryDefinition = null;
        }
        if (in.readBoolean()) {
            this.flushAcknowledgement = new FlushAcknowledgement(in);
        } else {
            this.flushAcknowledgement = null;
        }

        if (in.readBoolean()) {
            this.forecast = new Forecast(in);
        } else {
            this.forecast = null;
        }
        if (in.readBoolean()) {
            this.forecastRequestStats = new ForecastRequestStats(in);
        } else {
            this.forecastRequestStats = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeNullable(bucket, out);
        writeNullable(records, out);
        writeNullable(influencers, out);
        writeNullable(quantiles, out);
        writeNullable(modelSnapshot, out);
        writeNullable(modelSizeStats, out);
        writeNullable(modelPlot, out);
        writeNullable(categoryDefinition, out);
        writeNullable(flushAcknowledgement, out);
        writeNullable(forecast, out);
        writeNullable(forecastRequestStats, out);
    }

    private static void writeNullable(Writeable writeable, StreamOutput out) throws IOException {
        boolean isPresent = writeable != null;
        out.writeBoolean(isPresent);
        if (isPresent) {
            writeable.writeTo(out);
        }
    }

    private static void writeNullable(List<? extends Writeable> writeables, StreamOutput out) throws IOException {
        boolean isPresent = writeables != null;
        out.writeBoolean(isPresent);
        if (isPresent) {
            out.writeList(writeables);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        addNullableField(Bucket.RESULT_TYPE_FIELD, bucket, builder);
        addNullableField(AnomalyRecord.RESULTS_FIELD, records, builder);
        addNullableField(Influencer.RESULTS_FIELD, influencers, builder);
        addNullableField(Quantiles.TYPE, quantiles, builder);
        addNullableField(ModelSnapshot.TYPE, modelSnapshot, builder);
        addNullableField(ModelSizeStats.RESULT_TYPE_FIELD, modelSizeStats, builder);
        addNullableField(ModelPlot.RESULTS_FIELD, modelPlot, builder);
        addNullableField(Forecast.RESULTS_FIELD, forecast, builder);
        addNullableField(ForecastRequestStats.RESULTS_FIELD, forecastRequestStats, builder);
        addNullableField(CategoryDefinition.TYPE, categoryDefinition, builder);
        addNullableField(FlushAcknowledgement.TYPE, flushAcknowledgement, builder);
        builder.endObject();
        return builder;
    }

    private static void addNullableField(ParseField field, ToXContent value, XContentBuilder builder) throws IOException {
        if (value != null) {
            builder.field(field.getPreferredName(), value);
        }
    }

    private static void addNullableField(ParseField field, List<? extends ToXContent> values, XContentBuilder builder) throws IOException {
        if (values != null) {
            builder.field(field.getPreferredName(), values);
        }
    }

    public Bucket getBucket() {
        return bucket;
    }

    public List<AnomalyRecord> getRecords() {
        return records;
    }

    public List<Influencer> getInfluencers() {
        return influencers;
    }

    public Quantiles getQuantiles() {
        return quantiles;
    }

    public ModelSnapshot getModelSnapshot() {
        return modelSnapshot;
    }

    public ModelSizeStats getModelSizeStats() {
        return modelSizeStats;
    }

    public ModelPlot getModelPlot() {
        return modelPlot;
    }

    public Forecast getForecast() {
        return forecast;
    }

    public ForecastRequestStats getForecastRequestStats() {
        return forecastRequestStats;
    }

    public CategoryDefinition getCategoryDefinition() {
        return categoryDefinition;
    }

    public FlushAcknowledgement getFlushAcknowledgement() {
        return flushAcknowledgement;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, records, influencers, categoryDefinition, flushAcknowledgement, modelPlot, forecast, 
                forecastRequestStats, modelSizeStats, modelSnapshot, quantiles);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AutodetectResult other = (AutodetectResult) obj;
        return Objects.equals(bucket, other.bucket) &&
                Objects.equals(records, other.records) &&
                Objects.equals(influencers, other.influencers) &&
                Objects.equals(categoryDefinition, other.categoryDefinition) &&
                Objects.equals(flushAcknowledgement, other.flushAcknowledgement) &&
                Objects.equals(modelPlot, other.modelPlot) &&
                Objects.equals(forecast, other.forecast) &&
                Objects.equals(forecastRequestStats, other.forecastRequestStats) &&
                Objects.equals(modelSizeStats, other.modelSizeStats) &&
                Objects.equals(modelSnapshot, other.modelSnapshot) &&
                Objects.equals(quantiles, other.quantiles);
    }

}
