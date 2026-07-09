/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class AnalyzerContext {
    private final Configuration configuration;
    private final EsqlFunctionRegistry functionRegistry;
    private final PromqlFunctionRegistry promqlFunctionRegistry;
    private final AnalysisRegistry analysisRegistry;
    private final Map<IndexPattern, IndexResolution> indexResolution;
    private final Map<String, IndexResolution> lookupResolution;
    private final Map<LinkedIndexPattern, IndexResolution> linkedResolution; // CPS-specific resolution for remote indexes matching local
                                                                             // views
    private final EnrichResolution enrichResolution;
    private final InferenceResolution inferenceResolution;
    private final ExternalSourceResolution externalSourceResolution;
    private final TransportVersion minimumVersion;
    private final ProjectMetadata projectMetadata;
    private Boolean hasRemoteIndices;
    private final UnmappedResolution unmappedResolution;
    private final Set<String> deferredHeaderWarnings = new LinkedHashSet<>();
    private final TimestampBounds timestampBounds;
    private final IpLocationResolution ipLocationResolution;

    public AnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        PromqlFunctionRegistry promqlFunctionRegistry,
        AnalysisRegistry analysisRegistry,
        ProjectMetadata projectMetadata,
        Map<IndexPattern, IndexResolution> indexResolution,
        Map<String, IndexResolution> lookupResolution,
        Map<LinkedIndexPattern, IndexResolution> linkedResolution,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution,
        ExternalSourceResolution externalSourceResolution,
        TransportVersion minimumVersion,
        UnmappedResolution unmappedResolution,
        @Nullable TimestampBounds timestampBounds,
        IpLocationResolution ipLocationResolution
    ) {
        this.configuration = configuration;
        this.functionRegistry = functionRegistry;
        this.promqlFunctionRegistry = promqlFunctionRegistry;
        this.analysisRegistry = analysisRegistry;
        this.projectMetadata = projectMetadata;
        this.indexResolution = indexResolution;
        this.lookupResolution = lookupResolution;
        this.linkedResolution = linkedResolution;
        this.enrichResolution = enrichResolution;
        this.inferenceResolution = inferenceResolution;
        this.externalSourceResolution = externalSourceResolution;
        this.minimumVersion = minimumVersion;
        this.unmappedResolution = unmappedResolution;
        this.timestampBounds = timestampBounds;
        this.ipLocationResolution = ipLocationResolution;

        assert minimumVersion != null : "AnalyzerContext must have a minimum transport version";
        assert TransportVersion.current().supports(minimumVersion)
            : "AnalyzerContext [" + minimumVersion + "] is not on or before current transport version [" + TransportVersion.current() + "]";
    }

    // for testing only
    public AnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        PromqlFunctionRegistry promqlFunctionRegistry,
        AnalysisRegistry analysisRegistry,
        Map<IndexPattern, IndexResolution> indexResolution,
        Map<String, IndexResolution> lookupResolution,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution,
        TransportVersion minimumVersion,
        UnmappedResolution unmappedResolution
    ) {
        this(
            configuration,
            functionRegistry,
            promqlFunctionRegistry,
            analysisRegistry,
            null,
            indexResolution,
            lookupResolution,
            Map.of(),
            enrichResolution,
            inferenceResolution,
            ExternalSourceResolution.EMPTY,
            minimumVersion,
            unmappedResolution,
            null,
            IpLocationResolution.SERVICE_UNAVAILABLE
        );
    }

    public Configuration configuration() {
        return configuration;
    }

    public EsqlFunctionRegistry functionRegistry() {
        return functionRegistry;
    }

    public PromqlFunctionRegistry promqlFunctionRegistry() {
        return promqlFunctionRegistry;
    }

    /**
     * Node-level analyzer registry.
     */
    public AnalysisRegistry analysisRegistry() {
        return analysisRegistry;
    }

    public Map<IndexPattern, IndexResolution> indexResolution() {
        return indexResolution;
    }

    public Map<String, IndexResolution> lookupResolution() {
        return lookupResolution;
    }

    /**
     * Contains resolution for optional linked patterns. Such patterns include linked indices (if exist) that shadow local views.
     */
    public Map<LinkedIndexPattern, IndexResolution> linkedResolution() {
        return linkedResolution;
    }

    public EnrichResolution enrichResolution() {
        return enrichResolution;
    }

    public InferenceResolution inferenceResolution() {
        return inferenceResolution;
    }

    public ExternalSourceResolution externalSourceResolution() {
        return externalSourceResolution;
    }

    public TransportVersion minimumVersion() {
        return minimumVersion;
    }

    public ProjectMetadata projectMetadata() {
        return projectMetadata;
    }

    public boolean includesRemoteIndices() {
        assert indexResolution != null;
        if (hasRemoteIndices == null) {
            hasRemoteIndices = indexResolution.values().stream().anyMatch(IndexResolution::includesRemoteIndices);
        }
        return hasRemoteIndices;
    }

    public UnmappedResolution unmappedResolution() {
        return unmappedResolution;
    }

    /**
     * Header warnings collected during analysis but emitted only once the {@code Verifier} has passed, so a query that fails
     * verification produces no warnings.
     */
    public Set<String> deferredHeaderWarnings() {
        return deferredHeaderWarnings;
    }

    /**
     * Returns the {@code @timestamp} bounds extracted from the query DSL filter, or {@code null} if not available.
     */
    @Nullable
    public TimestampBounds timestampBounds() {
        return timestampBounds;
    }

    /**
     * The pre-fetched IP database metadata used by the {@code ResolveIpLocation} analyzer rule to resolve {@code IP_LOCATION}
     * output columns. When the service was unavailable while building the context, this is
     * {@link IpLocationResolution#SERVICE_UNAVAILABLE} and resolution fails verification.
     */
    public IpLocationResolution ipLocationResolution() {
        return ipLocationResolution;
    }

    public Set<String> allowedTags() {
        Set<String> result = new HashSet<>();
        result.addAll(MetadataAttribute.ATTRIBUTES_MAP.keySet());
        if (projectMetadata() != null) {
            projectMetadata().customs()
                .values()
                .stream()
                .filter(Metadata.TaggedProjectCustom.class::isInstance)
                .map(Metadata.TaggedProjectCustom.class::cast)
                .forEach(x -> {
                    Set<String> tagNames = x.tags().tags().keySet();
                    for (String tagName : tagNames) {
                        result.add(x.tagPrefix() + tagName);
                    }
                });
        }
        // TODO it would be good to cache this, but some tags can change over time (eg. tags on linked projects)
        return Collections.unmodifiableSet(result);
    }

    public AnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        PromqlFunctionRegistry promqlFunctionRegistry,
        AnalysisRegistry analysisRegistry,
        UnmappedResolution unmappedResolution,
        ProjectMetadata projectMetadata,
        EsqlSession.PreAnalysisResult result,
        @Nullable TimestampBounds timestampBounds,
        IpLocationResolution ipLocationResolution
    ) {
        this(
            configuration,
            functionRegistry,
            promqlFunctionRegistry,
            analysisRegistry,
            projectMetadata,
            result.indexResolution(),
            result.lookupIndices(),
            result.linkedResolution(),
            result.enrichResolution(),
            result.inferenceResolution(),
            result.externalSourceResolution(),
            result.minimumTransportVersion(),
            unmappedResolution,
            timestampBounds,
            ipLocationResolution
        );
    }
}
