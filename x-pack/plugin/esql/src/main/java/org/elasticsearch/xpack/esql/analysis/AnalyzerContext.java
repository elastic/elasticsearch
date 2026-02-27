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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AnalyzerContext {
    private final Configuration configuration;
    private final EsqlFunctionRegistry functionRegistry;
    private final Map<IndexPattern, IndexResolution> indexResolution;
    private final Map<String, IndexResolution> lookupResolution;
    private final EnrichResolution enrichResolution;
    private final InferenceResolution inferenceResolution;
    private final ExternalSourceResolution externalSourceResolution;
    private final TransportVersion minimumVersion;
    private final ProjectMetadata projectMetadata;
    private Boolean hasRemoteIndices;
    private final UnmappedResolution unmappedResolution;

    public AnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        ProjectMetadata projectMetadata,
        Map<IndexPattern, IndexResolution> indexResolution,
        Map<String, IndexResolution> lookupResolution,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution,
        ExternalSourceResolution externalSourceResolution,
        TransportVersion minimumVersion,
        UnmappedResolution unmappedResolution
    ) {
        this.configuration = configuration;
        this.functionRegistry = functionRegistry;
        this.projectMetadata = projectMetadata;
        this.indexResolution = indexResolution;
        this.lookupResolution = lookupResolution;
        this.enrichResolution = enrichResolution;
        this.inferenceResolution = inferenceResolution;
        this.externalSourceResolution = externalSourceResolution;
        this.minimumVersion = minimumVersion;
        this.unmappedResolution = unmappedResolution;

        assert minimumVersion != null : "AnalyzerContext must have a minimum transport version";
        assert TransportVersion.current().supports(minimumVersion)
            : "AnalyzerContext [" + minimumVersion + "] is not on or before current transport version [" + TransportVersion.current() + "]";
    }

    // for testing only
    public AnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
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
            null,
            indexResolution,
            lookupResolution,
            enrichResolution,
            inferenceResolution,
            ExternalSourceResolution.EMPTY,
            minimumVersion,
            unmappedResolution
        );
    }

    public Configuration configuration() {
        return configuration;
    }

    public EsqlFunctionRegistry functionRegistry() {
        return functionRegistry;
    }

    public Map<IndexPattern, IndexResolution> indexResolution() {
        return indexResolution;
    }

    public Map<String, IndexResolution> lookupResolution() {
        return lookupResolution;
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
        UnmappedResolution unmappedResolution,
        ProjectMetadata projectMetadata,
        EsqlSession.PreAnalysisResult result
    ) {
        this(
            configuration,
            functionRegistry,
            projectMetadata,
            result.indexResolution(),
            result.lookupIndices(),
            result.enrichResolution(),
            result.inferenceResolution(),
            result.externalSourceResolution(),
            result.minimumTransportVersion(),
            unmappedResolution
        );
    }
}
