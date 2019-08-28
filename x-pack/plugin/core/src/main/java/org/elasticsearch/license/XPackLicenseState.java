/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.monitoring.MonitoringField;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

/**
 * A holder for the current state of the license for all xpack features.
 */
public class XPackLicenseState {

    /** Messages for each feature which are printed when the license expires. */
    static final Map<String, String[]> EXPIRATION_MESSAGES;
    static {
        Map<String, String[]> messages = new LinkedHashMap<>();
        messages.put(XPackField.SECURITY, new String[] {
            "Cluster health, cluster stats and indices stats operations are blocked",
            "All data operations (read and write) continue to work"
        });
        messages.put(XPackField.WATCHER, new String[] {
            "PUT / GET watch APIs are disabled, DELETE watch API continues to work",
            "Watches execute and write to the history",
            "The actions of the watches don't execute"
        });
        messages.put(XPackField.MONITORING, new String[] {
            "The agent will stop collecting cluster and indices metrics",
            "The agent will stop automatically cleaning indices older than [xpack.monitoring.history.duration]"
        });
        messages.put(XPackField.GRAPH, new String[] {
            "Graph explore APIs are disabled"
        });
        messages.put(XPackField.MACHINE_LEARNING, new String[] {
            "Machine learning APIs are disabled"
        });
        messages.put(XPackField.LOGSTASH, new String[] {
            "Logstash will continue to poll centrally-managed pipelines"
        });
        messages.put(XPackField.BEATS, new String[] {
            "Beats will continue to poll centrally-managed configuration"
        });
        messages.put(XPackField.DEPRECATION, new String[] {
            "Deprecation APIs are disabled"
        });
        messages.put(XPackField.UPGRADE, new String[] {
            "Upgrade API is disabled"
        });
        messages.put(XPackField.SQL, new String[] {
            "SQL support is disabled"
        });
        messages.put(XPackField.ROLLUP, new String[] {
            "Creating and Starting rollup jobs will no longer be allowed.",
            "Stopping/Deleting existing jobs, RollupCaps API and RollupSearch continue to function."
        });
        messages.put(XPackField.ANALYTICS, new String[] {
            "Aggregations provided by Data Science plugin are no longer usable."
        });
        EXPIRATION_MESSAGES = Collections.unmodifiableMap(messages);
    }

    /**
     * Messages for each feature which are printed when the license type changes.
     * The value is a function taking the old and new license type, and returns the messages for that feature.
     */
    static final Map<String, BiFunction<OperationMode, OperationMode, String[]>> ACKNOWLEDGMENT_MESSAGES;
    static {
        Map<String, BiFunction<OperationMode, OperationMode, String[]>> messages = new LinkedHashMap<>();
        messages.put(XPackField.SECURITY, XPackLicenseState::securityAcknowledgementMessages);
        messages.put(XPackField.WATCHER, XPackLicenseState::watcherAcknowledgementMessages);
        messages.put(XPackField.MONITORING, XPackLicenseState::monitoringAcknowledgementMessages);
        messages.put(XPackField.GRAPH, XPackLicenseState::graphAcknowledgementMessages);
        messages.put(XPackField.MACHINE_LEARNING, XPackLicenseState::machineLearningAcknowledgementMessages);
        messages.put(XPackField.LOGSTASH, XPackLicenseState::logstashAcknowledgementMessages);
        messages.put(XPackField.BEATS, XPackLicenseState::beatsAcknowledgementMessages);
        messages.put(XPackField.SQL, XPackLicenseState::sqlAcknowledgementMessages);
        ACKNOWLEDGMENT_MESSAGES = Collections.unmodifiableMap(messages);
    }

    private static String[] securityAcknowledgementMessages(OperationMode currentMode, OperationMode newMode) {
        switch (newMode) {
            case BASIC:
                switch (currentMode) {
                    case STANDARD:
                        return new String[] {
                            "Security will default to disabled (set " + XPackSettings.SECURITY_ENABLED.getKey() + " to enable security).",
                        };
                    case TRIAL:
                    case GOLD:
                    case PLATINUM:
                        return new String[] {
                            "Security will default to disabled (set " + XPackSettings.SECURITY_ENABLED.getKey() + " to enable security).",
                            "Authentication will be limited to the native and file realms.",
                            "Security tokens and API keys will not be supported.",
                            "IP filtering and auditing will be disabled.",
                            "Field and document level access control will be disabled.",
                            "Custom realms will be ignored.",
                            "A custom authorization engine will be ignored."
                        };
                }
                break;
            case GOLD:
                switch (currentMode) {
                    case BASIC:
                    case STANDARD:
                        // ^^ though technically it was already disabled, it's not bad to remind them
                    case TRIAL:
                    case PLATINUM:
                        return new String[] {
                            "Field and document level access control will be disabled.",
                            "Custom realms will be ignored.",
                            "A custom authorization engine will be ignored."
                        };
                }
                break;
            case STANDARD:
                switch (currentMode) {
                    case BASIC:
                        // ^^ though technically it doesn't change the feature set, it's not bad to remind them
                    case GOLD:
                    case PLATINUM:
                    case TRIAL:
                        return new String[] {
                            "Authentication will be limited to the native realms.",
                            "IP filtering and auditing will be disabled.",
                            "Field and document level access control will be disabled.",
                            "Custom realms will be ignored.",
                            "A custom authorization engine will be ignored."
                        };
                }
        }
        return Strings.EMPTY_ARRAY;
    }

    private static String[] watcherAcknowledgementMessages(OperationMode currentMode, OperationMode newMode) {
        switch (newMode) {
            case BASIC:
                switch (currentMode) {
                    case TRIAL:
                    case STANDARD:
                    case GOLD:
                    case PLATINUM:
                        return new String[] { "Watcher will be disabled" };
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }

    private static String[] monitoringAcknowledgementMessages(OperationMode currentMode, OperationMode newMode) {
        switch (newMode) {
            case BASIC:
                switch (currentMode) {
                    case TRIAL:
                    case STANDARD:
                    case GOLD:
                    case PLATINUM:
                        return new String[] {
                            LoggerMessageFormat.format(
                                "Multi-cluster support is disabled for clusters with [{}] license. If you are\n" +
                                    "running multiple clusters, users won't be able to access the clusters with\n" +
                                    "[{}] licenses from within a single X-Pack Kibana instance. You will have to deploy a\n" +
                                    "separate and dedicated X-pack Kibana instance for each [{}] cluster you wish to monitor.",
                                newMode, newMode, newMode),
                            LoggerMessageFormat.format(
                                "Automatic index cleanup is locked to {} days for clusters with [{}] license.",
                                MonitoringField.HISTORY_DURATION.getDefault(Settings.EMPTY).days(), newMode)
                        };
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }

    private static String[] graphAcknowledgementMessages(OperationMode currentMode, OperationMode newMode) {
        switch (newMode) {
            case BASIC:
            case STANDARD:
            case GOLD:
                switch (currentMode) {
                    case TRIAL:
                    case PLATINUM:
                        return new String[] { "Graph will be disabled" };
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }

    private static String[] machineLearningAcknowledgementMessages(OperationMode currentMode, OperationMode newMode) {
        switch (newMode) {
            case BASIC:
            case STANDARD:
            case GOLD:
                switch (currentMode) {
                    case TRIAL:
                    case PLATINUM:
                        return new String[] { "Machine learning will be disabled" };
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }

    private static String[] logstashAcknowledgementMessages(OperationMode currentMode, OperationMode newMode) {
        switch (newMode) {
            case BASIC:
                if (isBasic(currentMode) == false) {
                    return new String[] { "Logstash will no longer poll for centrally-managed pipelines" };
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }

    private static String[] beatsAcknowledgementMessages(OperationMode currentMode, OperationMode newMode) {
        switch (newMode) {
            case BASIC:
                if (isBasic(currentMode) == false) {
                    return new String[] { "Beats will no longer be able to use centrally-managed configuration" };
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }

    private static String[] sqlAcknowledgementMessages(OperationMode currentMode, OperationMode newMode) {
        switch (newMode) {
            case BASIC:
            case STANDARD:
            case GOLD:
                switch (currentMode) {
                    case TRIAL:
                    case PLATINUM:
                        return new String[] {
                                "JDBC and ODBC support will be disabled, but you can continue to use SQL CLI and REST endpoint" };
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }

    private static boolean isBasic(OperationMode mode) {
        return mode == OperationMode.BASIC;
    }

    /** A wrapper for the license mode and state, to allow atomically swapping. */
    private static class Status {

        /** The current "mode" of the license (ie license type). */
        final OperationMode mode;

        /** True if the license is active, or false if it is expired. */
        final boolean active;

        Status(OperationMode mode, boolean active) {
            this.mode = mode;
            this.active = active;
        }
    }

    private final List<LicenseStateListener> listeners;
    private final boolean isSecurityEnabled;
    private final boolean isSecurityExplicitlyEnabled;

    private Status status = new Status(OperationMode.TRIAL, true);

    public XPackLicenseState(Settings settings) {
        this.listeners = new CopyOnWriteArrayList<>();
        this.isSecurityEnabled = XPackSettings.SECURITY_ENABLED.get(settings);
        this.isSecurityExplicitlyEnabled = isSecurityEnabled && isSecurityExplicitlyEnabled(settings);
    }

    private XPackLicenseState(XPackLicenseState xPackLicenseState) {
        this.listeners = xPackLicenseState.listeners;
        this.isSecurityEnabled = xPackLicenseState.isSecurityEnabled;
        this.isSecurityExplicitlyEnabled = xPackLicenseState.isSecurityExplicitlyEnabled;
        this.status = xPackLicenseState.status;
    }

    private static boolean isSecurityExplicitlyEnabled(Settings settings) {
        return settings.hasValue(XPackSettings.SECURITY_ENABLED.getKey());
    }

    /**
     * Updates the current state of the license, which will change what features are available.
     *
     * @param mode   The mode (type) of the current license.
     * @param active True if the current license exists and is within its allowed usage period; false if it is expired or missing.
     * @param mostRecentTrialVersion If this cluster has, at some point commenced a trial, the most recent version on which they did that.
     *                               May be {@code null} if they have never generated a trial license on this cluster, or the most recent
     *                               trial was prior to this metadata being tracked (6.1)
     */
    void update(OperationMode mode, boolean active, @Nullable Version mostRecentTrialVersion) {
        synchronized (this) {
            status = new Status(mode, active);
        }
        listeners.forEach(LicenseStateListener::licenseStateChanged);
    }

    /** Add a listener to be notified on license change */
    public void addListener(final LicenseStateListener listener) {
        listeners.add(Objects.requireNonNull(listener));
    }

    /** Remove a listener */
    public void removeListener(final LicenseStateListener listener) {
        listeners.remove(Objects.requireNonNull(listener));
    }

    /** Return the current license type. */
    public synchronized OperationMode getOperationMode() {
        return status.mode;
    }

    /** Return true if the license is currently within its time boundaries, false otherwise. */
    public synchronized boolean isActive() {
        return status.active;
    }

    /**
     * @return true if authentication and authorization should be enabled. this does not indicate what realms are available
     * @see #allowedRealmType() for the enabled realms
     */
    public synchronized boolean isAuthAllowed() {
        OperationMode mode = status.mode;
        final boolean isSecurityCurrentlyEnabled =
            isSecurityEnabled(mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
        if (isSecurityCurrentlyEnabled) {
            switch (mode) {
                case BASIC:
                case STANDARD:
                case GOLD:
                case PLATINUM:
                case TRIAL:
                    return true;
            }
        }
        return false;
    }

    /**
     * @return true if IP filtering should be enabled
     */
    public synchronized boolean isIpFilteringAllowed() {
        OperationMode mode = status.mode;
        final boolean isSecurityCurrentlyEnabled =
            isSecurityEnabled(mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
        return isSecurityCurrentlyEnabled && (mode == OperationMode.GOLD || mode == OperationMode.PLATINUM || mode == OperationMode.TRIAL);
    }

    /**
     * @return true if auditing should be enabled
     */
    public synchronized boolean isAuditingAllowed() {
        OperationMode mode = status.mode;
        final boolean isSecurityCurrentlyEnabled =
            isSecurityEnabled(mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
        return isSecurityCurrentlyEnabled && (mode == OperationMode.GOLD || mode == OperationMode.PLATINUM || mode == OperationMode.TRIAL);
    }

    /**
     * Indicates whether the stats and health API calls should be allowed. If a license is expired and past the grace
     * period then we deny these calls.
     *
     * @return true if the license allows for the stats and health APIs to be used.
     */
    public synchronized boolean isStatsAndHealthAllowed() {
        return status.active;
    }

    /**
     * Determine if Document Level Security (DLS) and Field Level Security (FLS) should be enabled.
     * <p>
     * DLS and FLS are only disabled when the mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM}</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     * Note: This does not consider the <em>state</em> of the license so that Security does not suddenly leak information!
     *
     * @return {@code true} to enable DLS and FLS. Otherwise {@code false}.
     */
    public synchronized boolean isDocumentAndFieldLevelSecurityAllowed() {
        OperationMode mode = status.mode;
        final boolean isSecurityCurrentlyEnabled =
            isSecurityEnabled(mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
        return isSecurityCurrentlyEnabled && (mode == OperationMode.TRIAL || mode == OperationMode.PLATINUM);
    }

    /** Classes of realms that may be available based on the license type. */
    public enum AllowedRealmType {
        NONE,
        NATIVE,
        DEFAULT,
        ALL
    }

    /**
     * @return the type of realms that are enabled based on the license {@link OperationMode}
     */
    public synchronized AllowedRealmType allowedRealmType() {
        final boolean isSecurityCurrentlyEnabled =
            isSecurityEnabled(status.mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
        if (isSecurityCurrentlyEnabled) {
            switch (status.mode) {
                case PLATINUM:
                case TRIAL:
                    return AllowedRealmType.ALL;
                case GOLD:
                    return AllowedRealmType.DEFAULT;
                case BASIC:
                case STANDARD:
                    return AllowedRealmType.NATIVE;
                default:
                    return AllowedRealmType.NONE;
            }
        } else {
            return AllowedRealmType.NONE;
        }
    }

    /**
     * @return whether custom role providers are allowed based on the license {@link OperationMode}
     */
    public synchronized boolean isCustomRoleProvidersAllowed() {
        final boolean isSecurityCurrentlyEnabled =
            isSecurityEnabled(status.mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
        return isSecurityCurrentlyEnabled && (status.mode == OperationMode.PLATINUM || status.mode == OperationMode.TRIAL)
                && status.active;
    }

    /**
     * @return whether the Elasticsearch {@code TokenService} is allowed based on the license {@link OperationMode}
     */
    public synchronized boolean isTokenServiceAllowed() {
        final OperationMode mode = status.mode;
        final boolean isSecurityCurrentlyEnabled = isSecurityEnabled(mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
        return isSecurityCurrentlyEnabled && (mode == OperationMode.GOLD || mode == OperationMode.PLATINUM || mode == OperationMode.TRIAL);
    }

    /**
     * @return whether the Elasticsearch {@code ApiKeyService} is allowed based on the current node/cluster state
     */
    public synchronized boolean isApiKeyServiceAllowed() {
        final OperationMode mode = status.mode;
        final boolean isSecurityCurrentlyEnabled = isSecurityEnabled(mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
        return isSecurityCurrentlyEnabled;
    }

    /**
     * @return whether "authorization_realms" are allowed based on the license {@link OperationMode}
     * @see org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings
     */
    public synchronized boolean isAuthorizationRealmAllowed() {
        final boolean isSecurityCurrentlyEnabled =
            isSecurityEnabled(status.mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
        return isSecurityCurrentlyEnabled && (status.mode == OperationMode.PLATINUM || status.mode == OperationMode.TRIAL)
            && status.active;
    }

    /**
     * @return whether a custom authorization engine is allowed based on the license {@link OperationMode}
     * @see org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings
     */
    public synchronized boolean isAuthorizationEngineAllowed() {
        final boolean isSecurityCurrentlyEnabled =
            isSecurityEnabled(status.mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
        return isSecurityCurrentlyEnabled && (status.mode == OperationMode.PLATINUM || status.mode == OperationMode.TRIAL)
            && status.active;
    }

    /**
     * Determine if Watcher is available based on the current license.
     * <p>
     * Watcher is available if the license is active (hasn't expired) and of one of the following types:
     * <ul>
     * <li>{@link OperationMode#STANDARD}</li>
     * <li>{@link OperationMode#PLATINUM}</li>
     * <li>{@link OperationMode#GOLD}</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     *
     * @return {@code true} as long as the license is valid. Otherwise {@code false}.
     */
    public synchronized boolean isWatcherAllowed() {
        Status localStatus = status;

        if (localStatus.active == false) {
            return false;
        }

        switch (localStatus.mode) {
            case TRIAL:
            case GOLD:
            case PLATINUM:
            case STANDARD:
                return true;
            default:
                return false;
        }
    }

    /**
     * Monitoring is always available as long as there is a valid license
     *
     * @return true if the license is active
     */
    public synchronized boolean isMonitoringAllowed() {
        return status.active;
    }

    /**
     * Monitoring Cluster Alerts requires the equivalent license to use Watcher.
     *
     * @return {@link #isWatcherAllowed()}
     * @see #isWatcherAllowed()
     */
    public synchronized boolean isMonitoringClusterAlertsAllowed() {
        return isWatcherAllowed();
    }

    /**
     * Determine if the current license allows the retention of indices to be modified.
     * <p>
     * Only users with a non-{@link OperationMode#BASIC} license can update the retention period.
     * <p>
     * Note: This does not consider the <em>state</em> of the license so that any change is remembered for when they fix their license.
     *
     * @return {@code true} if the user is allowed to modify the retention. Otherwise {@code false}.
     */
    public synchronized boolean isUpdateRetentionAllowed() {
        final OperationMode mode = status.mode;
        return mode != OperationMode.BASIC && mode != OperationMode.MISSING;
    }

    /**
     * Determine if Graph Exploration should be enabled.
     * <p>
     * Exploration is only disabled when the license has expired or if the mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM}</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     *
     * @return {@code true} as long as the license is valid. Otherwise {@code false}.
     */
    public synchronized boolean isGraphAllowed() {
        Status localStatus = status;
        OperationMode operationMode = localStatus.mode;

        boolean licensed = operationMode == OperationMode.TRIAL || operationMode == OperationMode.PLATINUM;

        return licensed && localStatus.active;
    }

    /**
     * Determine if Machine Learning should be enabled.
     * <p>
     * Machine Learning is only disabled when the license has expired or if the
     * mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM}</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     *
     * @return {@code true} as long as the license is valid. Otherwise
     *         {@code false}.
     */
    public synchronized boolean isMachineLearningAllowed() {
        final Status currentStatus = status;
        return currentStatus.active && isMachineLearningAllowedForOperationMode(currentStatus.mode);
    }

    public static boolean isMachineLearningAllowedForOperationMode(final OperationMode operationMode) {
        return isPlatinumOrTrialOperationMode(operationMode);
    }

    /**
     * Data Frame is always available as long as there is a valid license
     *
     * @return true if the license is active
     */
    public synchronized boolean isDataFrameAllowed() {
        return status.active;
    }

    /**
     * Rollup is always available as long as there is a valid license
     *
     * @return true if the license is active
     */
    public synchronized boolean isRollupAllowed() {
        return status.active;
    }

    /**
     * Voting only node functionality is always available as long as there is a valid license
     *
     * @return true if the license is active
     */
    public synchronized boolean isVotingOnlyAllowed() {
        return status.active;
    }

    /**
     * Logstash is allowed as long as there is an active license of type TRIAL, STANDARD, GOLD or PLATINUM
     * @return {@code true} as long as there is a valid license
     */
    public synchronized boolean isLogstashAllowed() {
        Status localStatus = status;
        return localStatus.active && (isBasic(localStatus.mode) == false);
    }

    /**
     * Beats is allowed as long as there is an active license of type TRIAL, STANDARD, GOLD or PLATINUM
     * @return {@code true} as long as there is a valid license
     */
    public synchronized boolean isBeatsAllowed() {
        Status localStatus = status;
        return localStatus.active && (isBasic(localStatus.mode) == false);

    }

    /**
     * Deprecation APIs are always allowed as long as there is an active license
     * @return {@code true} as long as there is a valid license
     */
    public synchronized boolean isDeprecationAllowed() {
        return status.active;
    }

    /**
     * Determine if Upgrade API should be enabled.
     * <p>
     *  Upgrade API is not available in for all license types except {@link OperationMode#MISSING}
     *
     * @return {@code true} as long as the license is valid. Otherwise
     *         {@code false}.
     */
    public synchronized boolean isUpgradeAllowed() {
        // Should work on all active licenses
        return status.active;
    }

    /**
     * Determine if Index Lifecycle API should be enabled.
     * <p>
     * Index Lifecycle API is available in for all license types except
     * {@link OperationMode#MISSING}
     *
     * @return {@code true} as long as the license is valid. Otherwise
     *         {@code false}.
     */
    public boolean isIndexLifecycleAllowed() {
        // status is volatile
        Status localStatus = status;
        // Should work on all active licenses
        return localStatus.active;
    }

    /**
     * Determine if SQL support should be enabled.
     * <p>
     *  SQL is available for all license types except {@link OperationMode#MISSING}
     */
    public synchronized boolean isSqlAllowed() {
        return status.active;
    }

    /**
     * Determine if JDBC support should be enabled.
     * <p>
     *  JDBC is available only in for {@link OperationMode#PLATINUM} and {@link OperationMode#TRIAL} licences
     */
    public synchronized boolean isJdbcAllowed() {
        Status localStatus = status;
        OperationMode operationMode = localStatus.mode;

        boolean licensed = operationMode == OperationMode.TRIAL || operationMode == OperationMode.PLATINUM;

        return licensed && localStatus.active;
    }

    /**
     * Determine if support for flattened object fields should be enabled.
     * <p>
     * Flattened fields are available for all license types except {@link OperationMode#MISSING}.
     */
    public synchronized boolean isFlattenedAllowed() {
        return status.active;
    }

    /**
     * Determine if Vectors support should be enabled.
     * <p>
     *  Vectors is available for all license types except {@link OperationMode#MISSING}
     */
    public synchronized boolean isVectorsAllowed() {
        return status.active;
    }

    /**
     * Determine if ODBC support should be enabled.
     * <p>
     * ODBC is available only in for {@link OperationMode#PLATINUM} and {@link OperationMode#TRIAL} licences
     */
    public synchronized boolean isOdbcAllowed() {
        Status localStatus = status;
        OperationMode operationMode = localStatus.mode;

        boolean licensed = operationMode == OperationMode.TRIAL || operationMode == OperationMode.PLATINUM;

        return licensed && localStatus.active;
    }

    /**
     * Determine if Spatial features should be enabled.
     * <p>
     * Spatial features are available in for all license types except
     * {@link OperationMode#MISSING}
     *
     * @return {@code true} as long as the license is valid. Otherwise
     *         {@code false}.
     */
    public boolean isSpatialAllowed() {
        // status is volatile
        Status localStatus = status;
        // Should work on all active licenses
        return localStatus.active;
    }

    /**
     * Datascience is always available as long as there is a valid license
     *
     * @return true if the license is active
     */
    public synchronized boolean isDataScienceAllowed() {
        return status.active;
    }

    public synchronized boolean isTrialLicense() {
        return status.mode == OperationMode.TRIAL;
    }

    /**
     * @return true if security is available to be used with the current license type
     */
    public synchronized boolean isSecurityAvailable() {
        OperationMode mode = status.mode;
        return mode == OperationMode.GOLD || mode == OperationMode.PLATINUM || mode == OperationMode.STANDARD ||
                mode == OperationMode.TRIAL || mode == OperationMode.BASIC;
    }

    /**
     * @return true if security has been disabled due it being the default setting for this license type.
     *  The conditions necessary for this are:
     *         <ul>
     *             <li>A trial or basic license</li>
     *             <li>xpack.security.enabled not specified as a setting</li>
     *         </ul>
     */
    public synchronized boolean isSecurityDisabledByLicenseDefaults() {
        switch (status.mode) {
            case TRIAL:
            case BASIC:
                return isSecurityEnabled && isSecurityExplicitlyEnabled == false;
        }
        return false;
    }

    public static boolean isTransportTlsRequired(License license, Settings settings) {
        if (license == null) {
            return false;
        }
        switch (license.operationMode()) {
            case STANDARD:
            case GOLD:
            case PLATINUM:
                return XPackSettings.SECURITY_ENABLED.get(settings);
            case BASIC:
                return XPackSettings.SECURITY_ENABLED.get(settings) && isSecurityExplicitlyEnabled(settings);
            case MISSING:
            case TRIAL:
                return false;
            default:
                throw new AssertionError("unknown operation mode [" + license.operationMode() + "]");
        }
    }

    private static boolean isSecurityEnabled(final OperationMode mode, final boolean isSecurityExplicitlyEnabled,
                                             final boolean isSecurityEnabled) {
        switch (mode) {
            case TRIAL:
            case BASIC:
                return isSecurityExplicitlyEnabled;
            default:
                return isSecurityEnabled;
        }
    }

    /**
     * Determine if cross-cluster replication should be enabled.
     * <p>
     * Cross-cluster replication is only disabled when the license has expired or if the mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM}</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     *
     * @return true is the license is compatible, otherwise false
     */
    public synchronized boolean isCcrAllowed() {
        final Status currentStatus = status;
        return currentStatus.active && isCcrAllowedForOperationMode(currentStatus.mode);
    }

    public static boolean isCcrAllowedForOperationMode(final OperationMode operationMode) {
        return isPlatinumOrTrialOperationMode(operationMode);
    }

    public static boolean isPlatinumOrTrialOperationMode(final OperationMode operationMode) {
        return operationMode == OperationMode.PLATINUM || operationMode == OperationMode.TRIAL;
    }

    /**
     * Creates a copy of this object based on the state at the time the method was called. The
     * returned object will not be modified by a license update/expiration so it can be used to
     * make multiple method calls on the license state safely. This object should not be long
     * lived but instead used within a method when a consistent view of the license state
     * is needed for multiple interactions with the license state.
     */
    public synchronized XPackLicenseState copyCurrentLicenseState() {
        return new XPackLicenseState(this);
    }
}
