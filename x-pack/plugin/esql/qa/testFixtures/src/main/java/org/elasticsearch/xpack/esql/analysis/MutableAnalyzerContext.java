/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Map;

/**
 * A mutable version of AnalyzerContext that allows temporarily changing the transport version.
 * This is useful for testing scenarios where different transport versions need to be tested.
 */
public class MutableAnalyzerContext extends AnalyzerContext {
    private TransportVersion currentVersion;
    private boolean currentUseAggregateMetricDoubleWhenNotSupported;
    private boolean currentUseDenseVectorWhenNotSupported;

    public MutableAnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        Map<IndexPattern, IndexResolution> indexResolution,
        Map<String, IndexResolution> lookupResolution,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution,
        TransportVersion minimumVersion,
        UnmappedResolution unmappedResolution
    ) {
        super(
            configuration,
            functionRegistry,
            indexResolution,
            lookupResolution,
            enrichResolution,
            inferenceResolution,
            minimumVersion,
            unmappedResolution
        );
        this.currentVersion = minimumVersion;
        this.currentUseAggregateMetricDoubleWhenNotSupported = false;
        this.currentUseDenseVectorWhenNotSupported = false;
    }

    @Override
    public TransportVersion minimumVersion() {
        return currentVersion;
    }

    @Override
    public boolean useAggregateMetricDoubleWhenNotSupported() {
        return currentUseAggregateMetricDoubleWhenNotSupported;
    }

    @Override
    public boolean useDenseVectorWhenNotSupported() {
        return currentUseDenseVectorWhenNotSupported;
    }

    /**
     * Temporarily set the transport version to a random version between the passed-in version and the latest,
     * and return an AutoCloseable to restore it.
     * Usage:
     * try (var restore = context.setTemporaryTransportVersionOnOrAfter(minVersion)) {...}
     */
    public RestoreTransportVersion setTemporaryTransportVersionOnOrAfter(TransportVersion minVersion) {
        TransportVersion oldVersion = this.currentVersion;
        // Set to a random version between minVersion and current
        this.currentVersion = TransportVersionUtils.randomVersionSupporting(minVersion);
        return new RestoreTransportVersion(
            oldVersion,
            this.currentUseAggregateMetricDoubleWhenNotSupported,
            this.currentUseDenseVectorWhenNotSupported
        );
    }

    public RestoreTransportVersion setTemporaryTransportVersion(
        TransportVersion transportVersion,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported
    ) {
        TransportVersion originalVersion = this.currentVersion;
        boolean originalUseAggregateMetricDoubleWhenNotSupported = this.currentUseAggregateMetricDoubleWhenNotSupported;
        boolean originalUseDenseVectorWhenNotSupported = this.currentUseDenseVectorWhenNotSupported;

        this.currentVersion = transportVersion;
        this.currentUseAggregateMetricDoubleWhenNotSupported = useAggregateMetricDoubleWhenNotSupported;
        this.currentUseDenseVectorWhenNotSupported = useDenseVectorWhenNotSupported;

        return new RestoreTransportVersion(
            originalVersion,
            originalUseAggregateMetricDoubleWhenNotSupported,
            originalUseDenseVectorWhenNotSupported
        );
    }

    /**
     * AutoCloseable that restores the original transport version when closed.
     */
    public class RestoreTransportVersion implements AutoCloseable {
        private final TransportVersion originalVersion;
        private final boolean originalUseAggregateMetricDoubleWhenNotSupported;
        private final boolean originalUseDenseVectorWhenNotSupported;

        private RestoreTransportVersion(
            TransportVersion originalVersion,
            boolean originalUseAggregateMetricDoubleWhenNotSupported,
            boolean originalUseDenseVectorWhenNotSupported
        ) {
            this.originalVersion = originalVersion;
            this.originalUseAggregateMetricDoubleWhenNotSupported = originalUseAggregateMetricDoubleWhenNotSupported;
            this.originalUseDenseVectorWhenNotSupported = originalUseDenseVectorWhenNotSupported;
        }

        @Override
        public void close() {
            MutableAnalyzerContext.this.currentVersion = originalVersion;
            MutableAnalyzerContext.this.currentUseAggregateMetricDoubleWhenNotSupported = originalUseAggregateMetricDoubleWhenNotSupported;
            MutableAnalyzerContext.this.currentUseDenseVectorWhenNotSupported = originalUseDenseVectorWhenNotSupported;
        }
    }
}
