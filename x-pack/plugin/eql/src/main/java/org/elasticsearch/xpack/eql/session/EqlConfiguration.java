/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class EqlConfiguration extends org.elasticsearch.xpack.ql.session.Configuration {

    private final String[] indices;
    private final String[] originalIndices;
    private final TimeValue requestTimeout;
    private final String clientId;
    private final IndicesOptions indicesOptions;
    private final TaskId taskId;
    private final EqlSearchTask task;
    private final int fetchSize;
    private final int maxSamplesPerKey;
    private final boolean allowPartialSearchResults;
    private final boolean allowPartialSequenceResults;
    private final String projectRouting;
    private final boolean crossProjectEnabled;
    private final ResolvedIndexExpressions resolvedIndexExpressions;
    // A merged field-caps response the caller already resolved for these indices (e.g. the ES|QL EQL source command),
    // letting the engine plan against it instead of issuing its own _field_caps. Coordinator-local like
    // EqlSearchRequest.resolvedTargetProjects: never serialized, because a caller's resolution describes the caller's
    // view of the mapping — a cross-cluster-proxied request must re-resolve on the executing cluster.
    private final transient FieldCapabilitiesResponse preResolvedFieldCaps;

    @Nullable
    private final QueryBuilder filter;
    @Nullable
    private final List<FieldAndFormat> fetchFields;
    @Nullable
    private Map<String, Object> runtimeMappings;

    // for test only
    public EqlConfiguration(
        String[] indices,
        ZoneId zi,
        String username,
        String clusterName,
        QueryBuilder filter,
        Map<String, Object> runtimeMappings,
        List<FieldAndFormat> fetchFields,
        TimeValue requestTimeout,
        IndicesOptions indicesOptions,
        int fetchSize,
        int maxSamplesPerKey,
        boolean allowPartialSearchResults,
        boolean allowPartialSequenceResults,
        String projectRouting,
        String clientId,
        TaskId taskId,
        EqlSearchTask task
    ) {
        this(
            indices,
            indices,
            zi,
            username,
            clusterName,
            filter,
            runtimeMappings,
            fetchFields,
            requestTimeout,
            indicesOptions,
            fetchSize,
            maxSamplesPerKey,
            allowPartialSearchResults,
            allowPartialSequenceResults,
            projectRouting,
            clientId,
            taskId,
            task,
            false,
            null,
            null
        );
    }

    public EqlConfiguration(
        String[] indices,
        String[] originalIndices,
        ZoneId zi,
        String username,
        String clusterName,
        QueryBuilder filter,
        Map<String, Object> runtimeMappings,
        List<FieldAndFormat> fetchFields,
        TimeValue requestTimeout,
        IndicesOptions indicesOptions,
        int fetchSize,
        int maxSamplesPerKey,
        boolean allowPartialSearchResults,
        boolean allowPartialSequenceResults,
        String projectRouting,
        String clientId,
        TaskId taskId,
        EqlSearchTask task,
        boolean crossProjectEnabled,
        ResolvedIndexExpressions resolvedIndexExpressions,
        @Nullable FieldCapabilitiesResponse preResolvedFieldCaps
    ) {
        super(zi, username, clusterName);

        this.indices = indices;
        this.originalIndices = originalIndices;
        this.filter = filter;
        this.runtimeMappings = runtimeMappings;
        this.fetchFields = fetchFields;
        this.requestTimeout = requestTimeout;
        this.clientId = clientId;
        this.indicesOptions = indicesOptions;
        this.taskId = taskId;
        this.task = task;
        this.fetchSize = fetchSize;
        this.maxSamplesPerKey = maxSamplesPerKey;
        this.allowPartialSearchResults = allowPartialSearchResults;
        this.allowPartialSequenceResults = allowPartialSequenceResults;
        this.projectRouting = projectRouting;
        this.crossProjectEnabled = crossProjectEnabled;
        this.resolvedIndexExpressions = resolvedIndexExpressions;
        this.preResolvedFieldCaps = preResolvedFieldCaps;
    }

    /** The caller-supplied merged field-caps for these indices, or {@code null} if the engine should self-resolve. */
    @Nullable
    FieldCapabilitiesResponse preResolvedFieldCaps() {
        return preResolvedFieldCaps;
    }

    public boolean crossProjectEnabled() {
        return crossProjectEnabled;
    }

    public ResolvedIndexExpressions resolvedIndexExpressions() {
        return resolvedIndexExpressions;
    }

    public String projectRouting() {
        return projectRouting;
    }

    public String[] indices() {
        return indices;
    }

    public String indexAsWildcard() {
        return Strings.arrayToCommaDelimitedString(originalIndices);
    }

    public TimeValue requestTimeout() {
        return requestTimeout;
    }

    public int fetchSize() {
        return fetchSize;
    }

    public int maxSamplesPerKey() {
        return maxSamplesPerKey;
    }

    public boolean allowPartialSearchResults() {
        return allowPartialSearchResults;
    }

    public boolean allowPartialSequenceResults() {
        return allowPartialSequenceResults;
    }

    public QueryBuilder filter() {
        return filter;
    }

    public Map<String, Object> runtimeMappings() {
        return runtimeMappings;
    }

    public List<FieldAndFormat> fetchFields() {
        return fetchFields;
    }

    public String clientId() {
        return clientId;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public boolean isCancelled() {
        return task.isCancelled();
    }

    public TaskId getTaskId() {
        return taskId;
    }

}
