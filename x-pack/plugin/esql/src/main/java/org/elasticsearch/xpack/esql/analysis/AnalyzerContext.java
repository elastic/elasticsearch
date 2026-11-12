/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.regex.Regex;
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
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.view.ViewQuerySettings;

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
    // Merged field-caps retained for EQL source patterns (empty otherwise), so ResolveEqlRelation can hand them to the
    // EQL delegate for reuse instead of a second _field_caps round trip.
    private final Map<IndexPattern, FieldCapabilitiesResponse> eqlFieldCaps;
    private final Map<String, IndexResolution> lookupResolution;
    private final Map<LinkedIndexPattern, IndexResolution> linkedResolution; // CPS-specific resolution for remote indexes matching local
                                                                             // views
    private final EnrichResolution enrichResolution;
    private final InferenceResolution inferenceResolution;
    private final ExternalSourceResolution externalSourceResolution;
    private final TransportVersion minimumVersion;
    private final ProjectMetadata projectMetadata;
    private final UnmappedResolution unmappedResolution;
    private final Set<String> deferredHeaderWarnings;
    private final Set<String> subqueryNonLoadableNullFills;
    private final Set<ViewBoundaryNullFill> viewBoundaryNullFills;
    private final TimestampBounds timestampBounds;
    private final IpLocationResolution ipLocationResolution;
    private final boolean preserveViewBoundaries;
    /** Per-view SET settings from each resolved view's definition, keyed by view name; see {@link ViewQuerySettings}. */
    private final Map<String, ViewQuerySettings> viewSettings;
    private final EsqlFlags flags;
    private final Set<String> fieldNames;

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
        IpLocationResolution ipLocationResolution,
        boolean preserveViewBoundaries,
        Map<String, ViewQuerySettings> viewSettings,
        EsqlFlags flags,
        Map<IndexPattern, FieldCapabilitiesResponse> eqlFieldCaps
    ) {
        this(
            configuration,
            functionRegistry,
            promqlFunctionRegistry,
            analysisRegistry,
            projectMetadata,
            indexResolution,
            lookupResolution,
            linkedResolution,
            enrichResolution,
            inferenceResolution,
            externalSourceResolution,
            minimumVersion,
            unmappedResolution,
            timestampBounds,
            ipLocationResolution,
            preserveViewBoundaries,
            viewSettings,
            flags,
            eqlFieldCaps,
            IndexResolver.ALL_FIELDS
        );
    }

    /**
     * @param fieldNames the field names the field caps requests ask for, as {@link IndexResolver#ALL_FIELDS} when every field is asked
     *                   for. A field no request asks for is not in the mapping of a relation, whether or not the indices map it.
     */
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
        IpLocationResolution ipLocationResolution,
        boolean preserveViewBoundaries,
        Map<String, ViewQuerySettings> viewSettings,
        EsqlFlags flags,
        Map<IndexPattern, FieldCapabilitiesResponse> eqlFieldCaps,
        Set<String> fieldNames
    ) {
        this.fieldNames = fieldNames;
        this.configuration = configuration;
        this.functionRegistry = functionRegistry;
        this.promqlFunctionRegistry = promqlFunctionRegistry;
        this.analysisRegistry = analysisRegistry;
        this.projectMetadata = projectMetadata;
        this.indexResolution = indexResolution;
        this.eqlFieldCaps = eqlFieldCaps;
        this.lookupResolution = lookupResolution;
        this.linkedResolution = linkedResolution;
        this.enrichResolution = enrichResolution;
        this.inferenceResolution = inferenceResolution;
        this.externalSourceResolution = externalSourceResolution;
        this.minimumVersion = minimumVersion;
        this.unmappedResolution = unmappedResolution;
        this.deferredHeaderWarnings = new LinkedHashSet<>();
        this.subqueryNonLoadableNullFills = new LinkedHashSet<>();
        this.viewBoundaryNullFills = new LinkedHashSet<>();
        this.timestampBounds = timestampBounds;
        this.ipLocationResolution = ipLocationResolution;
        this.preserveViewBoundaries = preserveViewBoundaries;
        this.viewSettings = viewSettings;
        this.flags = flags;

        assert minimumVersion != null : "AnalyzerContext must have a minimum transport version";
        assert flags != null : "AnalyzerContext must have flags";
        assert TransportVersion.current().supports(minimumVersion)
            : "AnalyzerContext [" + minimumVersion + "] is not on or before current transport version [" + TransportVersion.current() + "]";
    }

    /**
     * A context that resolves names with {@code unmappedResolution} and otherwise shares this one, including the warning sets.
     * Warnings recorded while resolving a view branch stay visible on the context that started analysis.
     */
    AnalyzerContext withUnmappedResolution(UnmappedResolution unmappedResolution) {
        return new AnalyzerContext(this, unmappedResolution);
    }

    private AnalyzerContext(AnalyzerContext other, UnmappedResolution unmappedResolution) {
        this.configuration = other.configuration;
        this.functionRegistry = other.functionRegistry;
        this.promqlFunctionRegistry = other.promqlFunctionRegistry;
        this.analysisRegistry = other.analysisRegistry;
        this.indexResolution = other.indexResolution;
        this.eqlFieldCaps = other.eqlFieldCaps;
        this.lookupResolution = other.lookupResolution;
        this.linkedResolution = other.linkedResolution;
        this.enrichResolution = other.enrichResolution;
        this.inferenceResolution = other.inferenceResolution;
        this.externalSourceResolution = other.externalSourceResolution;
        this.minimumVersion = other.minimumVersion;
        this.projectMetadata = other.projectMetadata;
        this.unmappedResolution = unmappedResolution;
        this.deferredHeaderWarnings = other.deferredHeaderWarnings;
        this.subqueryNonLoadableNullFills = other.subqueryNonLoadableNullFills;
        this.viewBoundaryNullFills = other.viewBoundaryNullFills;
        this.timestampBounds = other.timestampBounds;
        this.ipLocationResolution = other.ipLocationResolution;
        this.preserveViewBoundaries = other.preserveViewBoundaries;
        this.viewSettings = other.viewSettings;
        this.flags = other.flags;
        this.fieldNames = other.fieldNames;
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
            IpLocationResolution.SERVICE_UNAVAILABLE,
            false,
            Map.of(),
            EsqlFlags.DEFAULTS,
            Map.of()
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

    /** Merged field-caps retained for EQL source patterns (empty for non-EQL queries). */
    Map<IndexPattern, FieldCapabilitiesResponse> eqlFieldCaps() {
        return eqlFieldCaps;
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

    public UnmappedResolution unmappedResolution() {
        return unmappedResolution;
    }

    /** Per-view SET settings from each resolved view's definition, keyed by view name. */
    public Map<String, ViewQuerySettings> viewSettings() {
        return viewSettings;
    }

    /**
     * Header warnings collected during analysis but emitted only once the {@code Verifier} has passed, so a query that fails
     * verification produces no warnings.
     */
    public Set<String> deferredHeaderWarnings() {
        return deferredHeaderWarnings;
    }

    /**
     * LOAD_ALL subquery fields null-filled because the sibling type has no implicit KEYWORD cast.
     * Warnings are emitted later, and only if the field is observed.
     */
    public Set<String> subqueryNonLoadableNullFills() {
        return subqueryNonLoadableNullFills;
    }

    /**
     * Outer references null-filled at a view's output under a loading {@code unmapped_fields} mode that the view would have
     * surfaced had the mode applied inside it. Warned about once analysis succeeds, since a user who knows the view's contents
     * may expect the enclosing {@code SET} to reach them.
     */
    public Set<ViewBoundaryNullFill> viewBoundaryNullFills() {
        return viewBoundaryNullFills;
    }

    /**
     * Where a loading {@code unmapped_fields} mode stopped: {@code resolution} was set by the caller's query, or by the
     * definition of {@code settingView} if not {@code null}, and does not apply inside view {@code viewName}.
     * {@code enclosingView} names the view that reads {@code viewName} and whose output the null-fill was placed at, when
     * {@code viewName} was merged into it. {@code viewLoadsUnmappedFields} says whether {@code viewName}'s own definition
     * already SETs a loading mode, in which case it only has to reference the fields to load them.
     */
    public record ViewBoundary(
        UnmappedResolution resolution,
        @Nullable String settingView,
        String viewName,
        @Nullable String enclosingView,
        boolean viewLoadsUnmappedFields
    ) {}

    /** {@code fieldName} was answered with null at the output of {@code boundary}'s view. */
    public record ViewBoundaryNullFill(ViewBoundary boundary, String fieldName) {}

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
                    for (String tagName : x.allowedTagsNames()) {
                        result.add(x.tagPrefix() + tagName);
                    }
                });
        }
        // TODO it would be good to cache this, but some tags can change over time (eg. tags on linked projects)
        return Collections.unmodifiableSet(result);
    }

    /**
     * Whether the current request carries a DSL filter that must be applied at view-output
     * boundaries. When {@code true}, {@link org.elasticsearch.xpack.esql.view.ViewCompaction}
     * preserves {@link org.elasticsearch.xpack.esql.plan.logical.BranchPlan} wrappers around
     * view branches so that
     * {@link org.elasticsearch.xpack.esql.dsltranslate.ViewRequestFilterRewriter} can apply the
     * filter to the view's output rather than pushing it to the Lucene scan layer.
     */
    public boolean preserveViewBoundaries() {
        return preserveViewBoundaries;
    }

    /** Cluster flags for this request. Tests that omit them get {@link EsqlFlags#DEFAULTS}. */
    /**
     * Whether the field caps requests of this query ask for the field {@code name}. A relation that does not have a column of that name
     * does not map it, or was not asked about it, and the query does not read the column if it was not asked about.
     */
    public boolean fieldCapsAsksFor(String name) {
        if (IndexResolver.ALL_FIELDS.equals(fieldNames)) {
            return true;
        }
        for (String pattern : fieldNames) {
            if (Regex.simpleMatch(pattern, name)) {
                return true;
            }
        }
        return false;
    }

    public EsqlFlags flags() {
        return flags;
    }

    /** Resolved {@code max_branch_count_per_merge}: a set query pragma overrides {@link EsqlFlags#maxBranchCountPerMerge()}. */
    public int maxBranchCountPerMerge() {
        QueryPragmas pragmas = configuration == null ? QueryPragmas.EMPTY : configuration.pragmas();
        return pragmas.maxBranchCountPerMerge(flags.maxBranchCountPerMerge());
    }

    /** Label for whichever knob supplied {@link #maxBranchCountPerMerge()}. */
    public String maxBranchCountPerMergeLimitSource() {
        QueryPragmas pragmas = configuration == null ? QueryPragmas.EMPTY : configuration.pragmas();
        return pragmas.maxBranchCountPerMergeLimitSource(EsqlFlags.ESQL_MAX_BRANCH_COUNT_PER_MERGE.getKey());
    }

    /**
     * Resolved {@code max_branch_count}: a set query pragma overrides {@link EsqlFlags#maxBranchCount()}.
     * View compaction uses this as the flatten budget for nested view unions.
     */
    public int maxBranchCount() {
        QueryPragmas pragmas = configuration == null ? QueryPragmas.EMPTY : configuration.pragmas();
        return pragmas.maxBranchCount(flags.maxBranchCount());
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
        IpLocationResolution ipLocationResolution,
        boolean preserveViewBoundaries,
        Map<String, ViewQuerySettings> viewSettings,
        EsqlFlags flags
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
            ipLocationResolution,
            preserveViewBoundaries,
            viewSettings,
            flags,
            result.eqlFieldCaps(),
            result.fieldNames()
        );
    }
}
