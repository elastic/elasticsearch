/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.services.TextEmbedding;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.MlNodeDeployedServiceSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.TransportVersions.ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;
import static org.elasticsearch.xpack.inference.services.TextEmbedding.TextEmbeddingService.MULTILINGUAL_E5_SMALL_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.TextEmbedding.TextEmbeddingService.MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86;

public class MultilingualE5SmallServiceSettings extends TextEmbeddingServiceSettings {

    public static final String NAME = "multilingual_e5_small_service_settings";
    public static final List<String> MODEL_VARIANTS = List.of(MULTILINGUAL_E5_SMALL_MODEL_ID, MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86);

    public MultilingualE5SmallServiceSettings(int numAllocations, int numThreads, String modelVariant) {
        super(numAllocations, numThreads, modelVariant);
    }

    /**
     * Parse the MultilingualE5SmallServiceSettings from map and validate the setting values.
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @return The {@code MultilingualE5SmallServiceSettings} builder
     */
    public static MultilingualE5SmallServiceSettings.Builder fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        Integer numAllocations = ServiceUtils.removeAsType(map, NUM_ALLOCATIONS, Integer.class);
        Integer numThreads = ServiceUtils.removeAsType(map, NUM_THREADS, Integer.class);

        validateParameters(numAllocations, validationException, numThreads);

        String version = ServiceUtils.removeAsType(map, MODEL_VERSION, String.class);
        if (version != null) {
            if (MODEL_VARIANTS.contains(version) == false) {
                validationException.addValidationError("unknown Multilingual-E5-Small model version [" + version + "]");
            }
        } else {
            version = MULTILINGUAL_E5_SMALL_MODEL_ID;
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        var builder = new MlNodeDeployedServiceSettings.Builder() {
            @Override
            public MultilingualE5SmallServiceSettings build() {
                return new MultilingualE5SmallServiceSettings(getNumAllocations(), getNumThreads(), getModelVariant());
            }
        };
        builder.setNumAllocations(numAllocations);
        builder.setNumThreads(numThreads);
        builder.setModelVariant(version);
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return super.toXContent(builder, params);
    }

    @Override
    public boolean isFragment() {
        return super.isFragment();
    }

    @Override
    public String getWriteableName() {
        return MultilingualE5SmallServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
