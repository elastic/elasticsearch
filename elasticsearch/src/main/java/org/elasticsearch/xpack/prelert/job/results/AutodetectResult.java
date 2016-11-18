/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.results;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;

import java.io.IOException;
import java.util.Objects;

public class AutodetectResult extends ToXContentToBytes implements Writeable {

    public static final ParseField TYPE = new ParseField("autodetect_result");

    public static final ConstructingObjectParser<AutodetectResult, ParseFieldMatcherSupplier> PARSER = new ConstructingObjectParser<>(
            TYPE.getPreferredName(), a -> new AutodetectResult((Bucket) a[0], (Quantiles) a[1], (ModelSnapshot) a[2],
                    a[3] == null ? null : ((ModelSizeStats.Builder) a[3]).build(), (ModelDebugOutput) a[4], (CategoryDefinition) a[5],
                    (FlushAcknowledgement) a[6]));

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Bucket.PARSER, Bucket.TYPE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Quantiles.PARSER, Quantiles.TYPE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ModelSnapshot.PARSER, ModelSnapshot.TYPE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ModelSizeStats.PARSER, ModelSizeStats.TYPE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ModelDebugOutput.PARSER, ModelDebugOutput.TYPE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), CategoryDefinition.PARSER, CategoryDefinition.TYPE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), FlushAcknowledgement.PARSER, FlushAcknowledgement.TYPE);
    }

    private final Bucket bucket;
    private final Quantiles quantiles;
    private final ModelSnapshot modelSnapshot;
    private final ModelSizeStats modelSizeStats;
    private final ModelDebugOutput modelDebugOutput;
    private final CategoryDefinition categoryDefinition;
    private final FlushAcknowledgement flushAcknowledgement;

    public AutodetectResult(Bucket bucket, Quantiles quantiles, ModelSnapshot modelSnapshot, ModelSizeStats modelSizeStats,
            ModelDebugOutput modelDebugOutput, CategoryDefinition categoryDefinition, FlushAcknowledgement flushAcknowledgement) {
        this.bucket = bucket;
        this.quantiles = quantiles;
        this.modelSnapshot = modelSnapshot;
        this.modelSizeStats = modelSizeStats;
        this.modelDebugOutput = modelDebugOutput;
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
            this.modelDebugOutput = new ModelDebugOutput(in);
        } else {
            this.modelDebugOutput = null;
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        boolean hasBucket = bucket != null;
        out.writeBoolean(hasBucket);
        if (hasBucket) {
            bucket.writeTo(out);
        }
        boolean hasQuantiles = quantiles != null;
        out.writeBoolean(hasQuantiles);
        if (hasQuantiles) {
            quantiles.writeTo(out);
        }
        boolean hasModelSnapshot = modelSnapshot != null;
        out.writeBoolean(hasModelSnapshot);
        if (hasModelSnapshot) {
            modelSnapshot.writeTo(out);
        }
        boolean hasModelSizeStats = modelSizeStats != null;
        out.writeBoolean(hasModelSizeStats);
        if (hasModelSizeStats) {
            modelSizeStats.writeTo(out);
        }
        boolean hasModelDebugOutput = modelDebugOutput != null;
        out.writeBoolean(hasModelDebugOutput);
        if (hasModelDebugOutput) {
            modelDebugOutput.writeTo(out);
        }
        boolean hasCategoryDefinition = categoryDefinition != null;
        out.writeBoolean(hasCategoryDefinition);
        if (hasCategoryDefinition) {
            categoryDefinition.writeTo(out);
        }
        boolean hasFlushAcknowledgement = flushAcknowledgement != null;
        out.writeBoolean(hasFlushAcknowledgement);
        if (hasFlushAcknowledgement) {
            flushAcknowledgement.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (bucket != null) {
            builder.field(Bucket.TYPE.getPreferredName(), bucket);
        }
        if (quantiles != null) {
            builder.field(Quantiles.TYPE.getPreferredName(), quantiles);
        }
        if (modelSnapshot != null) {
            builder.field(ModelSnapshot.TYPE.getPreferredName(), modelSnapshot);
        }
        if (modelSizeStats != null) {
            builder.field(ModelSizeStats.TYPE.getPreferredName(), modelSizeStats);
        }
        if (modelDebugOutput != null) {
            builder.field(ModelDebugOutput.TYPE.getPreferredName(), modelDebugOutput);
        }
        if (categoryDefinition != null) {
            builder.field(CategoryDefinition.TYPE.getPreferredName(), categoryDefinition);
        }
        if (flushAcknowledgement != null) {
            builder.field(FlushAcknowledgement.TYPE.getPreferredName(), flushAcknowledgement);
        }
        builder.endObject();
        return builder;
    }

    public Bucket getBucket() {
        return bucket;
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

    public ModelDebugOutput getModelDebugOutput() {
        return modelDebugOutput;
    }

    public CategoryDefinition getCategoryDefinition() {
        return categoryDefinition;
    }

    public FlushAcknowledgement getFlushAcknowledgement() {
        return flushAcknowledgement;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, categoryDefinition, flushAcknowledgement, modelDebugOutput, modelSizeStats, modelSnapshot, quantiles);
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
                Objects.equals(categoryDefinition, other.categoryDefinition) &&
                Objects.equals(flushAcknowledgement, other.flushAcknowledgement) &&
                Objects.equals(modelDebugOutput, other.modelDebugOutput) &&
                Objects.equals(modelSizeStats, other.modelSizeStats) &&
                Objects.equals(modelSnapshot, other.modelSnapshot) &&
                Objects.equals(quantiles, other.quantiles);
    }

}
