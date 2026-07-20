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
 * Kill switch for the ES|QL federation feature (external data sources and datasets). Federation is
 * enabled by default; an operator disables it by setting the system property
 * {@value #ENABLED_PROPERTY} to {@code false}.
 *
 * <p>This is a deliberately coarse, static lever, not a dynamic setting: the value is read once at
 * class initialization (forced at node startup via {@link #init()}), so changing it requires
 * restarting the node. That trade-off is intentional for an emergency lever that is expected to be
 * used rarely, and it keeps the mechanism simple (a dynamic enabler would be considerably more
 * complex). Cloud/GovCloud can set system properties on any deployment.
 *
 * <p>Because any node can be the coordinating node for a query and any node can receive a data
 * source / dataset create request, the property must be set on <em>all</em> nodes for a complete
 * kill. Enforcement is layered so that any single disabled node is sufficient to stop its own share
 * of the work:
 * <ul>
 *   <li>The coordinator gates create requests ({@code DataSourceService}, {@code DatasetService})
 *       and the {@code FROM <dataset>} rewrite ({@code DatasetResolver}), giving a clean early 403.</li>
 *   <li>Every node gates the physical external-source operator build
 *       ({@code LocalExecutionPlanner.planExternalSource}). This is the backstop that guarantees no
 *       external I/O on a disabled node even when the already-rewritten plan arrives from elsewhere:
 *       an enabled coordinator, a remote cluster in CCS/CPS, an enabled coordinator during a rolling
 *       restart, or the inline {@code EXTERNAL} command (which bypasses the coordinator rewrite).</li>
 * </ul>
 *
 * <p>Note this only gates the federation abstraction (create data source, create dataset, and
 * external-source execution). It intentionally does not gate GET/DELETE of data sources and
 * datasets, so an operator can still inspect and clean up while the switch is engaged.
 */
public final class Federation {

    private static final Logger logger = LogManager.getLogger(Federation.class);

    public static final String ENABLED_PROPERTY = "es.esql.federation.enabled";

    private static final boolean ENABLED = readEnabled(System::getProperty);

    static {
        // Mirror FeatureFlag: surface the effective state in the node log so an operator can confirm
        // the switch after a bounce. Only log the exceptional (disabled) state to avoid noise. Because
        // the read and this log run in the static initializer, plugin startup calls init() to force
        // class initialization at boot rather than deferring it to the first federation operation.
        if (ENABLED == false) {
            logger.info("ES|QL federation (external data sources) is disabled ([{}]=false)", ENABLED_PROPERTY);
        }
    }

    private Federation() {}

    /**
     * Forces this class to initialize now, so the property is read (failing fast on an invalid value) and
     * the disabled state is logged at node startup rather than on the first federation operation. Intended
     * to be called once during plugin component creation; it does nothing beyond triggering the static
     * initializer.
     */
    public static void init() {}

    /**
     * Parses the enabled state from the given property source. Defaults to enabled when the property
     * is absent; an unparseable value fails fast (matching {@code FeatureFlag}).
     */
    static boolean readEnabled(Function<String, String> getProperty) {
        final String value = getProperty.apply(ENABLED_PROPERTY);
        try {
            return Booleans.parseBoolean(value, true);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value [" + value + "] for system property [" + ENABLED_PROPERTY + "]", e);
        }
    }

    /** Whether the federation feature is enabled on this node. */
    private static boolean enabled() {
        return ENABLED;
    }

    /** No-op when federation is enabled; throws {@link #disabledException()} when the kill switch is engaged. */
    public static void ensureEnabled() {
        ensureEnabled(enabled());
    }

    static void ensureEnabled(boolean enabled) {
        if (enabled == false) {
            throw disabledException();
        }
    }

    static ElasticsearchStatusException disabledException() {
        return new ElasticsearchStatusException(
            "ES|QL federation (external data sources) is disabled on this node "
                + "(system property ["
                + ENABLED_PROPERTY
                + "] is set to false)",
            RestStatus.FORBIDDEN
        );
    }
}
