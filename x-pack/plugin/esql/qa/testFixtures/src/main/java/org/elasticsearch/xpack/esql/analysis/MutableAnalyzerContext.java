/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
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

    public MutableAnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        Map<IndexPattern, IndexResolution> indexResolution,
        Map<String, IndexResolution> lookupResolution,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution,
        TransportVersion minimumVersion
    ) {
        super(configuration, functionRegistry, indexResolution, lookupResolution, enrichResolution, inferenceResolution, minimumVersion);
        this.currentVersion = minimumVersion;
    }

    @Override
    public TransportVersion minimumVersion() {
        return currentVersion;
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
        this.currentVersion = TransportVersionUtils.randomVersionBetween(ESTestCase.random(), minVersion, TransportVersion.current());
        return new RestoreTransportVersion(oldVersion);
    }

    /**
     * AutoCloseable that restores the original transport version when closed.
     */
    public class RestoreTransportVersion implements AutoCloseable {
        private final TransportVersion originalVersion;

        private RestoreTransportVersion(TransportVersion originalVersion) {
            this.originalVersion = originalVersion;
        }

        @Override
        public void close() {
            MutableAnalyzerContext.this.currentVersion = originalVersion;
        }
    }
}
