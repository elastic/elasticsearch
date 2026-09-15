/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.Constants;
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
 *       properties on any deployment. <strong>On Windows the default is {@code false}</strong>, so a
 *       Windows node has external data sources off unless something explicitly asks for them — and on
 *       a Windows <em>release</em> build nothing can, because the feature is not {@link #SUPPORTED}
 *       there and the property is rejected outright (see {@link #resolveRegistered}).</li>
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
 *   <li>A dataset on another cluster is unaffected, because it is invisible across a cluster boundary
 *       whether or not federation is available here: {@code EsqlResolveFieldsAction} never asks a remote to
 *       resolve datasets and clears the option on an incoming request, so {@code FROM <remote>:<name>}
 *       always falls through to normal remote index resolution.</li>
 *   <li>A data node refuses an external request on arrival
 *       ({@code DataNodeComputeHandler.handleExternalSourceRequest}), before local planning runs. This closes
 *       the data-node execution path: work shipped from an enabled coordinator (in CCS/CPS, or during a
 *       rolling restart that has not yet reached this node) is refused whatever the plan turns into, including
 *       an ungrouped {@code COUNT}/{@code MIN}/{@code MAX} that {@code PushStatsToExternalSource} would answer
 *       from split stats without ever building a scanning operator.</li>
 *   <li>The snapshot-only inline {@code EXTERNAL} command is refused by the parser
 *       ({@code LogicalPlanBuilder.visitExternalCommand}) with {@link #externalNotSupportedMessage()}. It does
 *       not go through the {@code DatasetResolver} gate above, so without this it would reach the operator-build
 *       backstop only after planning-time source resolution and split discovery had already touched external
 *       storage.</li>
 *   <li>Every node keeps a backstop at the physical external-source operator build
 *       ({@code LocalExecutionPlanner.planExternalSource}) that throws {@link #notAvailableException()}, which
 *       also covers plans built outside the data-node request path above — an {@code ExternalSourceExec}
 *       deserialized from an enabled coordinator never passes through this node's parser.</li>
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

    /**
     * Whether this node is capable of federation at all. External data sources are not shipped for Windows, so a
     * Windows release build cannot run them and cannot be made to: {@link #REGISTER_PROPERTY} is rejected rather
     * than ignored, and no federation setting exists to turn on. A Windows snapshot build is capable, so the
     * feature's own tests can opt in there, but it still defaults to off (see {@link #defaultRegistered}).
     *
     * <p>Code that <em>configures</em> a node — test cluster builders, and anything else deciding whether to write
     * a federation setting or property — should branch on this. Code that merely needs to know whether federation
     * can be used right now wants {@link #isRegistered()} or {@link #isAvailable(Settings)} instead.
     */
    public static final boolean SUPPORTED = supported(Constants.WINDOWS, Build.current().isSnapshot());

    /**
     * Whether a node that leaves {@value #REGISTER_PROPERTY} alone ends up with the feature registered. False on
     * Windows, where it must be asked for. Test infrastructure that needs to know whether a cluster it did not
     * configure will have federation should read this rather than inspecting the platform itself.
     */
    public static final boolean DEFAULT_REGISTERED = defaultRegistered(Constants.WINDOWS);

    private static final boolean REGISTERED = resolveRegistered(System::getProperty, Constants.WINDOWS, Build.current().isSnapshot());

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
     * Parses the registered state from the given property source, falling back to {@code defaultValue} when the
     * property is absent or blank; an unparseable value fails fast (matching {@code FeatureFlag}). The default is a
     * parameter because it is platform-dependent — see {@link #defaultRegistered}.
     */
    static boolean readRegistered(Function<String, String> getProperty, boolean defaultValue) {
        final String value = getProperty.apply(REGISTER_PROPERTY);
        try {
            return Booleans.parseBoolean(value, defaultValue);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value [" + value + "] for system property [" + REGISTER_PROPERTY + "]", e);
        }
    }

    /**
     * Whether federation can exist on the given platform and build. Only a Windows release build is incapable:
     * external data sources are not shipped for it. A Windows snapshot build is capable so that the feature's own
     * tests can run there.
     */
    static boolean supported(boolean windows, boolean snapshot) {
        return windows == false || snapshot;
    }

    /**
     * The registered state when {@value #REGISTER_PROPERTY} is not set. Off on Windows, so that a Windows node
     * behaves out of the box like the release build does — a test that wants the feature there opts in explicitly.
     * Unchanged (on) everywhere else.
     */
    static boolean defaultRegistered(boolean windows) {
        return windows == false;
    }

    /**
     * The registered state, combining the platform capability with {@value #REGISTER_PROPERTY}.
     *
     * <p>Where federation is not {@link #supported}, it cannot be turned on by any means: an explicit opt-in is
     * refused rather than ignored, failing the node at startup exactly as an unparseable value does. Every way of
     * setting the property (a {@code -D} flag, {@code ES_JAVA_OPTS}, {@code jvm.options}) arrives here as the same
     * system property, so this one check covers all of them.
     *
     * <p>Where it is supported, the property decides, over a platform-dependent default: on as before, except on
     * Windows, where it is off until something explicitly asks for it.
     *
     * <p>Platform and build arrive as parameters rather than being read from {@link Constants}/{@link Build} here,
     * so that every combination is unit-testable on any host.
     */
    static boolean resolveRegistered(Function<String, String> getProperty, boolean windows, boolean snapshot) {
        if (supported(windows, snapshot)) {
            return readRegistered(getProperty, defaultRegistered(windows));
        }
        // Parsing with a false default distinguishes an explicit opt-in from an absent one: null, empty and
        // whitespace-only all yield false, so leaving the property alone is not mistaken for setting it.
        if (Booleans.parseBoolean(getProperty.apply(REGISTER_PROPERTY), false)) {
            throw new IllegalArgumentException(
                "System property ["
                    + REGISTER_PROPERTY
                    + "] cannot be set to [true] on this platform: ES|QL federation (external data sources) is not "
                    + "supported on Windows"
            );
        }
        return false;
    }

    /**
     * The message for refusing an inline {@code EXTERNAL} command before it resolves anything. A node that could
     * never run the feature says so; one that merely has it switched off reuses the wording of
     * {@link #notAvailableException()}, so the two entry points read the same and existing assertions hold.
     */
    public static String externalNotSupportedMessage() {
        return SUPPORTED ? "external data sources are not available" : "External data sources are not supported on Windows";
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
