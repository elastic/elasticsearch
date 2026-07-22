/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;

import java.util.function.Function;

/**
 * Kill switch for the ES|QL federation feature (external data sources and datasets). The feature is
 * registered by default; an operator suppresses it by setting the system property
 * {@value #REGISTER_PROPERTY} to {@code false}.
 *
 * <p>This is a deliberately coarse, static lever, not a dynamic setting: the value is read once at
 * class initialization (forced at node startup by {@code EsqlPlugin}), so changing it requires
 * restarting the node. That trade-off is intentional for an emergency lever that is expected to be
 * used rarely, and it keeps the mechanism simple (a dynamic enabler would be considerably more
 * complex). Cloud/GovCloud can set system properties on any deployment.
 *
 * <p>When suppressed, the goal is that the feature looks like it never existed rather than being
 * present-but-forbidden:
 * <ul>
 *   <li>{@code EsqlPlugin} does not register the federation REST handlers or transport actions
 *       (create/get/delete data source and dataset, plus the {@code FROM <dataset>} resolve action),
 *       so their endpoints return the framework's standard {@code no handler found for uri}
 *       ({@code 400}), identical to a feature that was never shipped.</li>
 *   <li>{@code DatasetResolver} skips the {@code FROM <dataset>} rewrite entirely, so a dataset name
 *       falls through to normal index resolution and errors as {@code Unknown index}, the same error
 *       a nonexistent index gives.</li>
 *   <li>Every node keeps a backstop at the physical external-source operator build
 *       ({@code LocalExecutionPlanner.planExternalSource}) that throws {@link #notAvailableException()}.
 *       This closes the data-node execution path: an already-rewritten {@code ExternalSourceExec}
 *       shipped from an enabled coordinator (in CCS/CPS, or during a rolling restart that has not yet
 *       reached this node) is refused before it can build a scanning operator, so a disabled node runs
 *       no external scan. Coordinator-side work (the {@code FROM <dataset>} rewrite) is closed
 *       separately by the {@code DatasetResolver} gate above; the snapshot-only inline {@code EXTERNAL}
 *       command bypasses that gate and is only stopped here at operator build, so on a disabled
 *       coordinator its planning-time source resolution and split discovery can still touch external
 *       storage before this backstop fires.</li>
 * </ul>
 *
 * <p>Because any node can be the coordinating node for a query and any node can receive a data
 * source / dataset create request, the property must be set on <em>all</em> nodes for a complete
 * kill.
 */
public final class Federation {

    private static final Logger logger = LogManager.getLogger(Federation.class);

    public static final String REGISTER_PROPERTY = "es.esql.register_federation_feature";

    private static final boolean ENABLED = readEnabled(System::getProperty);

    static {
        // Mirror FeatureFlag: surface the effective state in the node log so an operator can confirm
        // the switch after a bounce. Only log the exceptional (disabled) state to avoid noise. Because
        // the read and this log run in the static initializer, EsqlPlugin forces class initialization
        // at boot rather than deferring it to the first federation operation.
        if (ENABLED == false) {
            logger.info("ES|QL federation (external data sources) is not registered ([{}]=false)", REGISTER_PROPERTY);
        }
    }

    private Federation() {}

    /**
     * Parses the enabled state from the given property source. Defaults to enabled when the property
     * is absent; an unparseable value fails fast (matching {@code FeatureFlag}).
     */
    static boolean readEnabled(Function<String, String> getProperty) {
        final String value = getProperty.apply(REGISTER_PROPERTY);
        try {
            return Booleans.parseBoolean(value, true);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value [" + value + "] for system property [" + REGISTER_PROPERTY + "]", e);
        }
    }

    /**
     * Whether the federation feature is available on this node. Read by {@code EsqlPlugin} at startup to
     * decide whether to register the federation REST handlers and transport actions, and by
     * {@code DatasetResolver} to decide whether to attempt the {@code FROM <dataset>} rewrite.
     */
    public static boolean isAvailable() {
        return ENABLED;
    }

    /** No-op when federation is available; throws {@link #notAvailableException()} when the kill switch is engaged. */
    public static void ensureEnabled() {
        ensureEnabled(isAvailable());
    }

    static void ensureEnabled(boolean enabled) {
        if (enabled == false) {
            throw notAvailableException();
        }
    }

    /**
     * The {@code 400} thrown by the data-node backstop when an external-source plan reaches a node that has
     * federation suppressed. The message deliberately omits the property name so it reads as a plain
     * "feature not present" error rather than a configuration hint.
     */
    static ElasticsearchStatusException notAvailableException() {
        return new ElasticsearchStatusException("external data sources are not available", RestStatus.BAD_REQUEST);
    }
}
