/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

public class InferenceRescorerBuilder extends RescorerBuilder<InferenceRescorerBuilder> {

    public static final String NAME = "inference";
    private static final ParseField MODEL = new ParseField("model_id");
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME, false, Builder::new);
    static {
        PARSER.declareString(Builder::setModelId, MODEL);
    }

    public static InferenceRescorerBuilder fromXContent(XContentParser parser, Supplier<ModelLoadingService> modelLoadingServiceSupplier) {
        return PARSER.apply(parser, null).build(modelLoadingServiceSupplier);
    }

    private final String modelId;
    private final LocalModel inferenceDefinition;
    private final Supplier<LocalModel> inferenceDefinitionSupplier;
    private final Supplier<ModelLoadingService> modelLoadingServiceSupplier;
    private boolean rescoreOccurred;

    public InferenceRescorerBuilder(String modelId, Supplier<ModelLoadingService> modelLoadingServiceSupplier) {
        this.modelId = Objects.requireNonNull(modelId);
        this.modelLoadingServiceSupplier = modelLoadingServiceSupplier;
        this.inferenceDefinition = null;
        this.inferenceDefinitionSupplier = null;
    }

    InferenceRescorerBuilder(String modelId, LocalModel inferenceDefinition) {
        this.modelId = Objects.requireNonNull(modelId);
        this.inferenceDefinition = Objects.requireNonNull(inferenceDefinition);
        this.inferenceDefinitionSupplier = null;
        this.modelLoadingServiceSupplier = null;
    }

    private InferenceRescorerBuilder(
        String modelId,
        Supplier<ModelLoadingService> modelLoadingServiceSupplier,
        Supplier<LocalModel> inferenceDefinitionSupplier
    ) {
        this.modelId = modelId;
        this.inferenceDefinition = null;
        this.inferenceDefinitionSupplier = inferenceDefinitionSupplier;
        this.modelLoadingServiceSupplier = modelLoadingServiceSupplier;
    }

    public InferenceRescorerBuilder(StreamInput input, Supplier<ModelLoadingService> modelLoadingServiceSupplier) throws IOException {
        super(input);
        this.modelId = input.readString();
        this.inferenceDefinitionSupplier = null;
        this.inferenceDefinition = null;
        this.modelLoadingServiceSupplier = modelLoadingServiceSupplier;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * should be updated once {@link InferenceRescorerFeature} is removed
     */
    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // TODO: update transport version when released!
        return TransportVersion.current();
    }

    @Override
    public RescorerBuilder<InferenceRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
        if (inferenceDefinition != null) {
            return this;
        }
        if (inferenceDefinitionSupplier != null) {
            if (inferenceDefinitionSupplier.get() == null) {
                return this;
            }
            LocalModel inferenceDefinition = inferenceDefinitionSupplier.get();
            InferenceRescorerBuilder builder = new InferenceRescorerBuilder(modelId, inferenceDefinition);
            if (windowSize() != null) {
                builder.windowSize(windowSize());
            }
            return builder;
        }
        // We don't want to rewrite on the coordinator as that doesn't make sense for this rescorer
        if (ctx.convertToDataRewriteContext() != null) {
            if (modelLoadingServiceSupplier == null || modelLoadingServiceSupplier.get() == null) {
                throw new IllegalStateException("Model loading service must be available");
            }
            SetOnce<LocalModel> inferenceDefinitionSetOnce = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> modelLoadingServiceSupplier.get().getModelForSearch(modelId, ActionListener.wrap(lm -> {
                inferenceDefinitionSetOnce.set(lm);
                l.onResponse(null);
            }, l::onFailure)));
            InferenceRescorerBuilder builder = new InferenceRescorerBuilder(
                modelId,
                modelLoadingServiceSupplier,
                inferenceDefinitionSetOnce::get
            );
            if (windowSize() != null) {
                builder.windowSize(windowSize());
            }
            return builder;
        }
        return this;
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (inferenceDefinitionSupplier != null) {
            throw new IllegalStateException("supplier must be null, missing a rewriteAndFetch?");
        }
        assert inferenceDefinition == null || rescoreOccurred : "Unnecessarily populated local model object";
        out.writeString(modelId);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(MODEL.getPreferredName(), modelId);
        builder.endObject();
    }

    @Override
    protected InferenceRescorerContext innerBuildContext(int windowSize, SearchExecutionContext context) {
        rescoreOccurred = true;
        return new InferenceRescorerContext(windowSize, InferenceRescorer.INSTANCE, inferenceDefinition, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        InferenceRescorerBuilder that = (InferenceRescorerBuilder) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(inferenceDefinition, that.inferenceDefinition)
            && Objects.equals(inferenceDefinitionSupplier, that.inferenceDefinitionSupplier)
            && Objects.equals(modelLoadingServiceSupplier, that.modelLoadingServiceSupplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), modelId, inferenceDefinition, inferenceDefinitionSupplier, modelLoadingServiceSupplier);
    }

    private static class Builder {
        private String modelId;

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        InferenceRescorerBuilder build(Supplier<ModelLoadingService> modelLoadingServiceSupplier) {
            return new InferenceRescorerBuilder(modelId, modelLoadingServiceSupplier);
        }
    }
}
