/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileQueryPhaseResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** Carries a compiled decision in the existing shard query profile, even when the shard returns no hits. */
public final class SequenceKeyFilterShardProfile implements SearchOperationListener {
    public static final String REQUEST_HEADER = "_esql_sequence_key_filter_profile";
    public static final String DEBUG_KEY = "esql_sequence_key_filter";
    public static final String APPLIED = "applied";

    private static final String DECISIONS = "esql.sequence.key_filter.decisions";
    private final Supplier<ThreadContext> threadContext;

    public SequenceKeyFilterShardProfile(Supplier<ThreadContext> threadContext) {
        this.threadContext = threadContext;
    }

    static boolean enabled(@Nullable ThreadContext context) {
        return context != null && "1".equals(context.getHeader(REQUEST_HEADER));
    }

    static void record(ThreadContext context, SearchExecutionContext execution, String decision) {
        PendingDecisions pending = context.getTransient(DECISIONS);
        if (pending == null) {
            pending = new PendingDecisions();
            context.putTransient(DECISIONS, pending);
        }
        // Only the context that actually runs the query may consume this decision. Rewrite clones and
        // other compilation contexts cannot contribute a decision to its profile.
        pending.values.put(execution, new Decision(decision, execution.getIndexSettings().getMode().getName()));
    }

    @Override
    public void onQueryPhase(SearchContext context, long tookInNanos) {
        Decision decision = takeDecision(context);
        var result = context.queryResult();
        if (decision == null || result.hasProfileResults() == false) {
            return;
        }
        var previous = result.consumeProfileResult();
        SearchProfileQueryPhaseResult updated = previous;
        try {
            List<QueryProfileShardResult> phases = new ArrayList<>(previous.getQueryProfileResults());
            if (phases.isEmpty()) {
                return;
            }
            var phase = phases.getFirst();
            List<ProfileResult> nodes = new ArrayList<>(phase.getQueryResults());
            nodes.add(
                new ProfileResult(
                    "SequenceKeyFilter",
                    "SEQUENCE key filter construction",
                    Map.of(),
                    Map.of(DEBUG_KEY, Map.of("decision", decision.outcome, "index_mode", decision.indexMode)),
                    0L,
                    List.of()
                )
            );
            phases.set(
                0,
                new QueryProfileShardResult(nodes, phase.getRewriteTime(), phase.getCollectorResult(), phase.getVectorOperationsCount())
            );
            var replacement = new SearchProfileQueryPhaseResult(
                phases,
                previous.getAggregationProfileResults(),
                previous.getRescoreProfileResults()
            );
            replacement.setSearchProfileDfsPhaseResult(previous.getSearchProfileDfsPhaseResult());
            updated = replacement;
        } finally {
            result.profileResults(updated);
        }
    }

    @Override
    public void onFailedQueryPhase(SearchContext context) {
        takeDecision(context);
    }

    @Nullable
    private Decision takeDecision(SearchContext search) {
        ThreadContext context = threadContext.get();
        if (enabled(context) == false) {
            return null;
        }
        PendingDecisions pending = context.getTransient(DECISIONS);
        return pending == null ? null : pending.values.remove(search.getSearchExecutionContext());
    }

    private record Decision(String outcome, String indexMode) {}

    private static final class PendingDecisions {
        private final Map<SearchExecutionContext, Decision> values = Collections.synchronizedMap(new IdentityHashMap<>());
    }
}
