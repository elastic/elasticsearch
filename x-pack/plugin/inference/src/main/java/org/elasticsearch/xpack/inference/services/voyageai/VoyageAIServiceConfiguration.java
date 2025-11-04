/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;

/**
 * Composite pattern implementation for VoyageAI service configuration.
 * This class encapsulates the complex configuration parsing logic and provides
 * a unified interface for different service setting types.
 */
public class VoyageAIServiceConfiguration {
    
    private final VoyageAIServiceSettings commonSettings;
    private final RateLimitSettings rateLimitSettings;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final boolean dimensionsSetByUser;

    private VoyageAIServiceConfiguration(Builder builder) {
        this.commonSettings = builder.commonSettings;
        this.rateLimitSettings = builder.rateLimitSettings;
        this.similarity = builder.similarity;
        this.dimensions = builder.dimensions;
        this.maxInputTokens = builder.maxInputTokens;
        this.dimensionsSetByUser = builder.dimensionsSetByUser;
    }

    /**
     * Builder pattern for creating VoyageAI service configurations.
     * This provides a fluent interface for configuration creation while handling validation.
     */
    public static class Builder {
        private VoyageAIServiceSettings commonSettings;
        private RateLimitSettings rateLimitSettings;
        private SimilarityMeasure similarity;
        private Integer dimensions;
        private Integer maxInputTokens;
        private boolean dimensionsSetByUser = false;
        private final ValidationException validationException = new ValidationException();

        /**
         * Creates a builder from a configuration map.
         */
        public static Builder fromMap(Map<String, Object> map, ConfigurationParseContext context) {
            Builder builder = new Builder();
            builder.commonSettings = VoyageAIServiceSettings.fromMap(map, context);
            return builder;
        }

        /**
         * Sets the common service settings.
         */
        public Builder withCommonSettings(VoyageAIServiceSettings commonSettings) {
            this.commonSettings = commonSettings;
            return this;
        }

        /**
         * Sets the rate limit settings.
         */
        public Builder withRateLimitSettings(@Nullable RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
            return this;
        }

        /**
         * Sets the similarity measure.
         */
        public Builder withSimilarity(@Nullable SimilarityMeasure similarity) {
            this.similarity = similarity;
            return this;
        }

        /**
         * Sets the dimensions.
         */
        public Builder withDimensions(@Nullable Integer dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        /**
         * Sets the maximum input tokens.
         */
        public Builder withMaxInputTokens(@Nullable Integer maxInputTokens) {
            this.maxInputTokens = maxInputTokens;
            return this;
        }

        /**
         * Sets whether dimensions were set by user.
         */
        public Builder withDimensionsSetByUser(boolean dimensionsSetByUser) {
            this.dimensionsSetByUser = dimensionsSetByUser;
            return this;
        }

        /**
         * Adds a validation error.
         */
        public Builder addValidationError(String error) {
            validationException.addValidationError(error);
            return this;
        }

        /**
         * Validates the configuration and throws if there are errors.
         */
        public Builder validate() {
            if (commonSettings == null) {
                validationException.addValidationError("Common settings are required");
            }
            
            if (validationException.validationErrors().isEmpty() == false) {
                throw validationException;
            }
            return this;
        }

        /**
         * Builds the configuration.
         */
        public VoyageAIServiceConfiguration build() {
            validate();
            return new VoyageAIServiceConfiguration(this);
        }
    }

    // Getters
    public VoyageAIServiceSettings getCommonSettings() {
        return commonSettings;
    }

    public RateLimitSettings getRateLimitSettings() {
        return rateLimitSettings;
    }

    public SimilarityMeasure getSimilarity() {
        return similarity;
    }

    public Integer getDimensions() {
        return dimensions;
    }

    public Integer getMaxInputTokens() {
        return maxInputTokens;
    }

    public boolean isDimensionsSetByUser() {
        return dimensionsSetByUser;
    }

    /**
     * Template method for creating specific service settings.
     * This demonstrates how the configuration can be used to create different types of settings.
     */
    public <T> T createServiceSettings(ServiceSettingsFactory<T> factory) {
        return factory.create(this);
    }

    /**
     * Functional interface for creating service settings from configuration.
     */
    @FunctionalInterface
    public interface ServiceSettingsFactory<T> {
        T create(VoyageAIServiceConfiguration config);
    }
}