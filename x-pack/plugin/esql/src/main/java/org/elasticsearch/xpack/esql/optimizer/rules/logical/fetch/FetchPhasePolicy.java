/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags.FetchPhaseMode;

/**
 * Decides whether the fetch phase may be planned for a query, and in which mode, before any plan is inspected.
 * <p>
 * Three switches, read in this order:
 * <ol>
 *     <li>the feature flag: a build without it never plans a fetch phase, whatever the other switches say,</li>
 *     <li>the {@code fetch_phase} pragma, which overrides the mode of the cluster for one query: {@code true} plans the
 *     fetch for every plan that can use it, {@link FetchPhaseMode#ALWAYS}, and {@code false} for none,</li>
 *     <li>the minimum transport version of the cluster: every node must be able to read the plans and the requests
 *     of the fetch phase. Otherwise the query falls back to loading every column eagerly.</li>
 * </ol>
 * The feature flag reaches the policy as the mode of the cluster, see {@link EsqlFlags#fetchPhaseMode()}. Whether a plan
 * can use the fetch phase once the switches allow it is decided by {@link PlanFetch}.
 *
 * @param clusterMode the mode the build gives, or the one a test pins
 * @param pragma      the {@code fetch_phase} pragma, {@code null} when the query does not set it
 */
public record FetchPhasePolicy(FetchPhaseMode clusterMode, @Nullable Boolean pragma, TransportVersion minimumVersion) {

    /**
     * The transport version every node needs before a coordinator plans a fetch phase. It names the newest wire format
     * the fetch phase depends on: the fetch request. Every node that reads it also reads the detached {@code _doc} of the
     * exchange and keeps the reader contexts a data node request asks for.
     */
    public static final TransportVersion PLANNER_MINIMUM = TransportVersion.fromName("esql_fetch_action");

    /**
     * Whether the switches allow the fetch phase for a query, and why not.
     */
    public enum Outcome {
        /** The switches allow the fetch phase. */
        ENABLED,
        /** This build does not carry the feature. */
        DISABLED_FEATURE_FLAG,
        /** The query turned the fetch phase off with the {@code fetch_phase} pragma. */
        DISABLED_PRAGMA,
        /** The mode of the cluster is off and the query does not turn it on with the {@code fetch_phase} pragma. */
        DISABLED_CLUSTER,
        /** Some node in the cluster cannot read the plans or requests of the fetch phase. */
        MIXED_VERSION_FALLBACK
    }

    public static FetchPhasePolicy from(LogicalOptimizerContext context) {
        return new FetchPhasePolicy(
            context.flags().fetchPhaseMode(),
            context.configuration().pragmas().fetchPhase(),
            context.minimumVersion()
        );
    }

    /**
     * {@link Outcome#ENABLED} when the rule may plan the fetch phase in {@link #mode()}, otherwise why it may not.
     */
    public Outcome decide() {
        if (clusterMode == FetchPhaseMode.UNAVAILABLE) {
            // the pragma cannot turn on what the build does not carry
            return Outcome.DISABLED_FEATURE_FLAG;
        }
        if (mode() == FetchPhaseMode.DISABLED) {
            return pragma != null ? Outcome.DISABLED_PRAGMA : Outcome.DISABLED_CLUSTER;
        }
        return minimumVersion.supports(PLANNER_MINIMUM) ? Outcome.ENABLED : Outcome.MIXED_VERSION_FALLBACK;
    }

    /**
     * The mode the rule plans with when {@link #decide()} allows it: the pragma's when the query sets it, otherwise the
     * cluster's. Never {@link FetchPhaseMode#UNAVAILABLE}.
     */
    public FetchPhaseMode mode() {
        if (clusterMode == FetchPhaseMode.UNAVAILABLE) {
            return FetchPhaseMode.DISABLED;
        }
        if (pragma != null) {
            return pragma ? FetchPhaseMode.ALWAYS : FetchPhaseMode.DISABLED;
        }
        return clusterMode;
    }
}
