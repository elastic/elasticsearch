/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Reports explicit shard decisions. A shard can participate in more than one candidate search. */
public final class SequenceKeyFilterProfile {
    public static final ProfileSchema<SequenceKeyFilterProfile> PROFILE_SCHEMA = ProfileSchema.of(
        SequenceKeyFilterProfile.class,
        (builder, value, params) -> builder.map(value.snapshot())
    )
        .required("built_searches", ProfileSchema.integer())
        .required("applied_shard_searches", ProfileSchema.integer())
        .required("coordinator_fallbacks", ProfileSchema.dynamicObject(ProfileSchema.integer()))
        .required("rejected_shard_searches", ProfileSchema.dynamicObject(ProfileSchema.integer()))
        .required("index_modes", ProfileSchema.dynamicObject(ProfileSchema.integer()))
        .build();

    private long builtSearches;
    private long appliedShardSearches;
    private final Map<String, Long> coordinatorFallbacks = new HashMap<>();
    private final Map<String, Long> rejectedShardSearches = new HashMap<>();
    private final Map<String, Long> indexModes = new HashMap<>();

    synchronized void builtSearch() {
        builtSearches = plus(builtSearches, 1);
    }

    synchronized void coordinatorFallback(String reason) {
        coordinatorFallbacks.merge(reason, 1L, SequenceKeyFilterProfile::plus);
    }

    synchronized void recordShardSearches(SearchResponse response) {
        var shardResults = response.getSearchProfileShardResults();
        if (shardResults == null) {
            return;
        }
        for (var shard : shardResults.values()) {
            recordShardSearch(shard.getQueryProfileResults());
        }
    }

    private void recordShardSearch(List<QueryProfileShardResult> phases) {
        for (var phase : phases) {
            for (var result : phase.getQueryResults()) {
                var debug = result.getDebugInfo();
                if (debug != null
                    && debug.get(SequenceKeyFilterShardProfile.DEBUG_KEY) instanceof Map<?, ?> state
                    && state.get("decision") instanceof String decision
                    && state.get("index_mode") instanceof String mode) {
                    indexModes.merge(mode, 1L, SequenceKeyFilterProfile::plus);
                    if ("applied".equals(decision)) {
                        appliedShardSearches = plus(appliedShardSearches, 1);
                    } else {
                        rejectedShardSearches.merge(decision, 1L, SequenceKeyFilterProfile::plus);
                    }
                    return;
                }
            }
        }
    }

    @Nullable
    public synchronized Map<String, Object> snapshot() {
        if (builtSearches == 0 && coordinatorFallbacks.isEmpty()) {
            return null;
        }
        return Map.of(
            "built_searches",
            builtSearches,
            "applied_shard_searches",
            appliedShardSearches,
            "coordinator_fallbacks",
            Map.copyOf(coordinatorFallbacks),
            "rejected_shard_searches",
            Map.copyOf(rejectedShardSearches),
            "index_modes",
            Map.copyOf(indexModes)
        );
    }

    private static long plus(long a, long b) {
        return a > Long.MAX_VALUE - b ? Long.MAX_VALUE : a + b;
    }
}
