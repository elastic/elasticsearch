/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheSettings;
import org.elasticsearch.xpack.esql.datasources.dataset.DatasetService;
import org.elasticsearch.xpack.esql.datasources.datasource.DataSourceService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * The availability gate for the ES|QL federation feature (external data sources and datasets). Two
 * levers decide it, and the feature is available only when both agree:
 * <ul>
 *   <li>the system property {@value #REGISTER_PROPERTY} decides whether the feature is
 *       <em>registered</em> on this node at all. It defaults to {@code true}, and an operator
 *       suppresses the feature by setting it to {@code false}. Cloud/GovCloud can set system
 *       properties on any deployment.</li>
 *   <li>the setting {@link #FEDERATION_ENABLED} decides whether the registered feature is
 *       <em>enabled</em>. Its default follows the build: on in a snapshot build, so a development or
 *       test deployment gets the feature without configuring anything, and off in a release build,
 *       where turning it on is a deliberate act. A value in {@code elasticsearch.yml} wins over the
 *       default either way.</li>
 * </ul>
 *
 * <p>The two levers are not symmetric. An unregistered feature has no settings at all: {@link #settings()}
 * returns nothing, so neither {@link #FEDERATION_ENABLED} nor any external data source knob is registered and a
 * node whose {@code elasticsearch.yml} carries one of those keys fails to start with the framework's standard
 * {@code unknown setting} error. Configuring a feature an operator removed is a misconfiguration, not something
 * to tolerate, and the resulting error is the same one a misspelled key gives.
 *
 * <p>Both levers are deliberately coarse and static rather than dynamic: the property is read once at
 * class initialization and the setting is read from the node's settings, so changing either requires
 * restarting the node. The gates below include REST handler registration, which happens once at
 * startup, so a dynamic value could not take effect without a restart anyway.
 *
 * <p>When federation is not available, the goal is that the feature looks like it never existed rather
 * than being present-but-forbidden:
 * <ul>
 *   <li>{@code EsqlPlugin} does not register the federation REST handlers (create/get/delete data
 *       source and dataset), so their endpoints return the framework's standard
 *       {@code no handler found for uri} ({@code 400}), identical to a feature that was never
 *       shipped. The corresponding transport actions stay registered, so a caller with transport
 *       privileges can still read and write data source and dataset cluster state.</li>
 *   <li>{@code DatasetResolver} skips the {@code FROM <dataset>} rewrite entirely, so a dataset name
 *       falls through to normal index resolution and errors as {@code Unknown index}, the same error
 *       a nonexistent index gives.</li>
 *   <li>{@code EsqlResolveFieldsAction} reports no datasets for an incoming remote field-caps request
 *       and does not ask its own remotes to resolve datasets, so {@code FROM <remote>:<name>} falls
 *       through to normal remote index resolution in both directions instead of failing with a
 *       {@code RemoteDatasetNotSupportedException} that names datasets.</li>
 *   <li>A data node refuses an external request on arrival
 *       ({@code DataNodeComputeHandler.handleExternalSourceRequest}), before local planning runs. This closes
 *       the data-node execution path: work shipped from an enabled coordinator (in CCS/CPS, or during a
 *       rolling restart that has not yet reached this node) is refused whatever the plan turns into, including
 *       an ungrouped {@code COUNT}/{@code MIN}/{@code MAX} that {@code PushStatsToExternalSource} would answer
 *       from split stats without ever building a scanning operator.</li>
 *   <li>Every node keeps a backstop at the physical external-source operator build
 *       ({@code LocalExecutionPlanner.planExternalSource}) that throws {@link #notAvailableException()}, which
 *       also covers plans built outside the data-node request path above. Coordinator-side work (the
 *       {@code FROM <dataset>} rewrite) is closed separately by the {@code DatasetResolver} gate above; the
 *       snapshot-only inline {@code EXTERNAL} command bypasses that gate and is only stopped here at operator
 *       build, so on a coordinator without federation its planning-time source resolution and split discovery
 *       can still touch external storage before this backstop fires.</li>
 * </ul>
 *
 * <p>Because any node can be the coordinating node for a query and any node can receive a data
 * source / dataset create request, both levers must agree across <em>all</em> nodes for a consistent
 * result.
 */
public final class Federation {

    private static final Logger logger = LogManager.getLogger(Federation.class);

    public static final String REGISTER_PROPERTY = "es.esql.register_federation_feature";

    /**
     * Enables the ES|QL federation feature (external data sources and datasets) on this node. The default
     * follows the build: a snapshot build has it on, so development and test deployments exercise the
     * feature without configuring it, and a release build has it off. An explicit value wins over the
     * default. The value is read from the node's settings rather than from cluster state and gates REST
     * handler registration, so a change takes effect only after a restart. It is registered only when the
     * feature is registered, so on a node where an operator set {@value #REGISTER_PROPERTY} to
     * {@code false} this key is unknown and rejected at startup.
     */
    public static final Setting<Boolean> FEDERATION_ENABLED = Setting.boolSetting(
        "esql.federation.enabled",
        Build.current().isSnapshot(),
        Setting.Property.NodeScope
    );

    private static final boolean REGISTERED = readRegistered(System::getProperty);

    private Federation() {}

    /**
     * Whether the feature is registered on this node, i.e. whether an operator left {@value #REGISTER_PROPERTY}
     * alone. Callers that only need to know if federation can be used should use {@link #isAvailable(Settings)};
     * this is for the registration-time decisions that cannot consult a setting, because the settings themselves
     * are what is being registered.
     */
    public static boolean isRegistered() {
        return REGISTERED;
    }

    /**
     * The node settings that exist only while the feature is registered: the {@link #FEDERATION_ENABLED} gate, every
     * external data source and dataset knob, and the ceilings on how many data sources and datasets a project may
     * hold. A plugin must add these to its own {@code Plugin#getSettings()} rather than registering the groups
     * directly, so that unregistering the feature takes its whole configuration surface with it.
     */
    public static List<Setting<?>> settings() {
        return settings(REGISTERED);
    }

    static List<Setting<?>> settings(boolean registered) {
        if (registered == false) {
            return List.of();
        }
        List<Setting<?>> settings = new ArrayList<>();
        settings.add(FEDERATION_ENABLED);
        settings.addAll(ExternalSourceSettings.settings());
        settings.addAll(ExternalSourceCacheSettings.settings());
        settings.add(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING);
        settings.add(DatasetService.MAX_DATASETS_COUNT_SETTING);
        return List.copyOf(settings);
    }

    /**
     * Parses the registered state from the given property source. Defaults to registered when the
     * property is absent; an unparseable value fails fast (matching {@code FeatureFlag}).
     */
    static boolean readRegistered(Function<String, String> getProperty) {
        final String value = getProperty.apply(REGISTER_PROPERTY);
        try {
            return Booleans.parseBoolean(value, true);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value [" + value + "] for system property [" + REGISTER_PROPERTY + "]", e);
        }
    }

    /**
     * Whether the federation feature is available on this node, which requires both that it is registered
     * and that {@link #FEDERATION_ENABLED} is on. Takes the node settings rather than caching an
     * effective value, because settings are per-node state and several nodes share one JVM in tests.
     */
    public static boolean isAvailable(Settings settings) {
        return REGISTERED && FEDERATION_ENABLED.get(settings);
    }

    /** No-op when federation is available on this node; throws {@link #notAvailableException()} otherwise. */
    public static void ensureEnabled(Settings settings) {
        ensureEnabled(isAvailable(settings));
    }

    static void ensureEnabled(boolean enabled) {
        if (enabled == false) {
            throw notAvailableException();
        }
    }

    /**
     * Surfaces the effective state in the node log at startup so an operator can confirm both levers after a
     * bounce. The states that change what the node does reach {@code INFO}: an enabled node serves external
     * data sources, and an unregistered one has lost a whole configuration surface. A registered node with
     * the setting off is inert, so it is logged at {@code DEBUG} and confirming it takes raising the level
     * for this class. An unregistered node cannot also have the setting on, because it does not accept the
     * setting at all, so there is no combination to warn about.
     */
    public static void logEffectiveState(Settings settings) {
        logEffectiveState(REGISTERED, FEDERATION_ENABLED.get(settings));
    }

    static void logEffectiveState(boolean registered, boolean enabled) {
        if (registered == false) {
            logger.info("ES|QL federation (external data sources) is not registered ([{}]=[false])", REGISTER_PROPERTY);
        } else if (enabled) {
            logger.info("ES|QL federation (external data sources) is enabled ([{}]=[true])", FEDERATION_ENABLED.getKey());
        } else {
            logger.debug("ES|QL federation (external data sources) is disabled ([{}]=[false])", FEDERATION_ENABLED.getKey());
        }
    }

    /**
     * The {@code 400} raised when external-source work reaches a node that does not have federation available,
     * either at the data node's external-request entry point or at the operator-build backstop. The message
     * deliberately omits the property and setting names so it reads as a plain "feature not present" error
     * rather than a configuration hint.
     */
    public static ElasticsearchStatusException notAvailableException() {
        return new ElasticsearchStatusException("external data sources are not available", RestStatus.BAD_REQUEST);
    }
}
