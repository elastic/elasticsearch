/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import joptsimple.internal.Strings;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.action.ActionLoggerContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;

import java.util.Arrays;

public class SearchLogContext extends ActionLoggerContext {
    private static final String TYPE = "search";
    private final SearchRequest request;
    private final @Nullable SearchResponse response;
    private String[] indexNames = null;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final ProjectState projectState;

    public SearchLogContext(
        Task task,
        NamedWriteableRegistry namedWriteableRegistry,
        ProjectState projectState,
        SearchRequest request,
        SearchResponse response
    ) {
        super(task, TYPE, response.getTook().nanos());
        this.request = request;
        this.response = response;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.projectState = projectState;
    }

    SearchLogContext(
        Task task,
        NamedWriteableRegistry namedWriteableRegistry,
        ProjectState projectState,
        SearchRequest request,
        long tookInNanos,
        Exception error
    ) {
        super(task, TYPE, tookInNanos, error);
        this.request = request;
        this.response = null;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.projectState = projectState;
    }

    String getQuery() {
        var source = request.source();
        if (source == null) {
            return "{}";
        } else {
            return source.toString(FORMAT_PARAMS);
        }
    }

    long getHits() {
        if (response == null || response.getHits() == null || response.getHits().getTotalHits() == null) {
            return 0;
        }
        return response.getHits().getTotalHits().value();
    }

    String[] getIndexNames() {
        if (indexNames == null) {
            if (request.pointInTimeBuilder() == null) {
                indexNames = request.indices();
            } else {
                final SearchContextId searchContextId = request.pointInTimeBuilder().getSearchContextId(namedWriteableRegistry);
                indexNames = searchContextId.getActualIndices();
            }
        }
        return indexNames;
    }

    boolean isSystemIndex(String index) {
        ProjectMetadata metadata = projectState.metadata();

        if (metadata.hasAlias(index) && metadata.aliasedIndices(index).stream().allMatch(i -> metadata.index(i).isSystem())) {
            return true;
        }

        if (metadata.hasIndex(index) && metadata.index(index).isSystem()) {
            return true;
        }

        if (metadata.indexIsADataStream(index)) {
            DataStream ds = metadata.dataStreams().get(index);
            return ds != null && ds.isSystem();
        }

        return false;
    }

    boolean isSystemSearch() {
        String opaqueId = getOpaqueId();
        // Kibana task manager queries
        // TODO: refine the check
        if (opaqueId != null && (opaqueId.startsWith("kibana:") || opaqueId.contains(";kibana:"))) {
            return true;
        }
        String[] indices = getIndexNames();
        // Request that only asks for system indices is system search
        if (indices.length > 0 && Arrays.stream(indices).allMatch(this::isSystemIndex)) {
            return true;
        }
        return false;
    }

    public String getIndices() {
        return Strings.join(getIndexNames(), ",");
    }
}
