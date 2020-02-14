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
        messages.put(XPackField.TRANSFORM, new String[] {
            "Creating, starting, updating transforms will no longer be allowed.",
            "Stopping/Deleting existing transforms continue to function."
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
                    case ENTERPRISE:
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
                    case ENTERPRISE:
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
                    case ENTERPRISE:
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
                    case ENTERPRISE:
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
                    case ENTERPRISE:
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
                    case ENTERPRISE:
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
                    case ENTERPRISE:
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
                    case ENTERPRISE:
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

    /**
     * Return true if the feature is allowed by all active, i.e. no-expired licenses, false otherwise.
     * @see #isActive()
     */
    public synchronized boolean allowForAllLicenses() {
        return status.active;
    }

    // Package private for tests
    /** Return true if the license is currently within its time boundaries, false otherwise. */
    synchronized boolean isActive() {
        return status.active;
    }

    /**
     * @return true if authentication and authorization should be enabled. this does not indicate what realms are available
     * @see #allowedRealmType() for the enabled realms
     */
    public boolean isAuthAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.BASIC, false, true);
    }

    /**
     * @return true if IP filtering should be enabled
     */
    public boolean isIpFilteringAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.GOLD, false, true);
    }

    /**
     * @return true if auditing should be enabled
     */
    public boolean isAuditingAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.GOLD, false, true);
    }

    /**
     * Stats and health API calls are always allowed as long as there is an active license.
     */
    public boolean isStatsAndHealthAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Determine if Document Level Security (DLS) and Field Level Security (FLS) should be enabled.
     * <p>
     * DLS and FLS are only disabled when the mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM} or higher</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     * Note: This does not consider the <em>state</em> of the license so that Security does not suddenly leak information!
     * i.e. the same DLS guarantee keeps working for existing configuration even after license expires.
     *
     * @return {@code true} to enable DLS and FLS. Otherwise {@code false}.
     */
    public boolean isDocumentAndFieldLevelSecurityAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.PLATINUM, false, true);
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
                case ENTERPRISE:
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
    public boolean isCustomRoleProvidersAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.PLATINUM, true, true);
    }

    /**
     * @return whether the Elasticsearch {@code TokenService} is allowed based on the license {@link OperationMode}
     */
    public boolean isTokenServiceAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.GOLD, false, true);
    }

    /**
     * @return whether the Elasticsearch {@code ApiKeyService} is allowed based on the current node/cluster state
     */
    public boolean isApiKeyServiceAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.MISSING, false, true);
    }

    /**
     * @return whether "authorization_realms" are allowed based on the license {@link OperationMode}
     * @see org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings
     */
    public boolean isAuthorizationRealmAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.PLATINUM, true, true);
    }

    /**
     * @return whether a custom authorization engine is allowed based on the license {@link OperationMode}
     * @see org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings
     */
    public boolean isAuthorizationEngineAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.PLATINUM, true, true);
    }

    /**
     * Determine if Watcher is available based on the current license.
     * <p>
     * Watcher is available if the license is active (hasn't expired) and of one of the following types:
     * <ul>
     * <li>{@link OperationMode#STANDARD} or higher</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     */
    public boolean isWatcherAllowed() {
        return isAllowedByLicense(OperationMode.STANDARD);
    }

    /**
     * Monitoring is always available as long as there is an active license
     */
    public boolean isMonitoringAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Monitoring Cluster Alerts requires the equivalent license to use Watcher.
     *
     * @return {@link #isWatcherAllowed()}
     * @see #isWatcherAllowed()
     */
    public boolean isMonitoringClusterAlertsAllowed() {
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
    public boolean isUpdateRetentionAllowed() {
        return isAllowedByLicense(OperationMode.STANDARD, false, true);
    }

    /**
     * Determine if Graph Exploration should be enabled.
     * <p>
     * Exploration is only disabled when the license has expired or if the mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM} or higher</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     */
    public boolean isGraphAllowed() {
        return isAllowedByLicense(OperationMode.PLATINUM);
    }

    /**
     * Determine if Machine Learning should be enabled.
     * <p>
     * Machine Learning is only disabled when the license has expired or if the
     * mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM} or higher</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     *         {@code false}.
     */
    public boolean isMachineLearningAllowed() {
        return isAllowedByLicense(OperationMode.PLATINUM);
    }

    public static boolean isMachineLearningAllowedForOperationMode(final OperationMode operationMode) {
        return isAllowedByOperationMode(operationMode, OperationMode.PLATINUM, true);
    }

    /**
     * Transform is always available as long as there is an active license
     */
    public boolean isTransformAllowed() {
        return allowForAllLicenses();
    }

    public static boolean isTransformAllowedForOperationMode(final OperationMode operationMode) {
        // any license (basic and upwards)
        return operationMode != License.OperationMode.MISSING;
    }

    public static boolean isFipsAllowedForOperationMode(final OperationMode operationMode) {
        return isAllowedByOperationMode(operationMode, OperationMode.PLATINUM, true);
    }

    /**
     * Rollup is always allowed as long as there is an active license
     */
    public boolean isRollupAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Voting only node functionality is always allowed as long as there is an active license
     */
    public boolean isVotingOnlyAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Logstash is allowed as long as there is an active license of type TRIAL, STANDARD or higher
     */
    public boolean isLogstashAllowed() {
        return isAllowedByLicense(OperationMode.STANDARD);
    }

    /**
     * Beats is allowed as long as there is an active license of type TRIAL, STANDARD or higher.
     */
    public boolean isBeatsAllowed() {
        return isAllowedByLicense(OperationMode.STANDARD);
    }

    /**
     * Deprecation APIs are always allowed as long as there is an active license
     */
    public boolean isDeprecationAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Upgrade API is always allowed as long as there is an active license.
     */
    public boolean isUpgradeAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Index Lifecycle API is always allowed as long as there is an active license.
     */
    public boolean isIndexLifecycleAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Enrich processor and related APIs are always allowed as long as there is an active license.
     */
    public boolean isEnrichAllowed() {
        return allowForAllLicenses();
    }

    /**
     * EQL support is always allowed as long as there is an active license.
     */
    public boolean isEqlAllowed() {
        return allowForAllLicenses();
    }

    /**
     * SQL support is always allowed as long as there is an active license.
     */
    public boolean isSqlAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Determine if JDBC support should be enabled.
     * <p>
     *  JDBC is available only in for {@link OperationMode#PLATINUM} or higher and {@link OperationMode#TRIAL} licences
     */
    public boolean isJdbcAllowed() {
        return isAllowedByLicense(OperationMode.PLATINUM);
    }

    /**
     * Support for flattened object fields is always allowed as long as there is an active license.
     */
    public boolean isFlattenedAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Vectors support is always allowed as long as there is an active license.
     */
    public boolean isVectorsAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Determine if ODBC support should be enabled.
     * <p>
     * ODBC is available only in for {@link OperationMode#PLATINUM} or higher and {@link OperationMode#TRIAL} licences
     */
    public boolean isOdbcAllowed() {
        return isAllowedByLicense(OperationMode.PLATINUM);
    }

    /**
     * Spatial features are always allowed as long as there is an active license
     */
    public boolean isSpatialAllowed() {
        return allowForAllLicenses();
    }

    /**
     * Datascience is always available as long as there is an active license
     */
    public boolean isDataScienceAllowed() {
        return allowForAllLicenses();
    }

    /**
     * @return true if security is available to be used with the current license type
     */
    public synchronized boolean isSecurityAvailable() {
        return status.mode != OperationMode.MISSING;
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
            case ENTERPRISE:
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
     * <li>{@link OperationMode#PLATINUM} or higher</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     *
     * @return true is the license is compatible, otherwise false
     */
    public boolean isCcrAllowed() {
        return isAllowedByLicense(OperationMode.PLATINUM);
    }

    public static boolean isCcrAllowedForOperationMode(final OperationMode operationMode) {
        return isAllowedByOperationMode(operationMode, OperationMode.PLATINUM, true);
    }

    public static boolean isAllowedByOperationMode(
        final OperationMode operationMode, final OperationMode minimumMode, final boolean allowTrial) {
        if (allowTrial && OperationMode.TRIAL == operationMode) {
            return true;
        }
        return operationMode.compareTo(minimumMode) >= 0;
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

    /**
     * Test whether a feature is allowed by the status of license and security configuration.
     * Note the difference to {@link #isAllowedByLicense(OperationMode, boolean, boolean)}
     * is this method requires security to be enabled.
     *
     * @param minimumMode  The minimum license to meet or exceed
     * @param needActive   Whether current license needs to be active.
     * @param allowTrial   Whether the feature is allowed for trial license
     *
     * @return true if feature is allowed, otherwise false
     */
    private synchronized boolean isAllowedBySecurityAndLicense(OperationMode minimumMode, boolean needActive, boolean allowTrial) {
        if (false == isSecurityEnabled(status.mode, isSecurityExplicitlyEnabled, isSecurityEnabled)) {
            return false;
        }
        return isAllowedByLicense(minimumMode, needActive, allowTrial);
    }

    /**
     * Test whether a feature is allowed by the status of license. Note difference to
     * {@link #isAllowedBySecurityAndLicense} is this method does <b>Not</b> require security
     * to be enabled.
     *
     * @param minimumMode  The minimum license to meet or exceed
     * @param needActive   Whether current license needs to be active
     * @param allowTrial   Whether the feature is allowed for trial license
     *
     * @return true if feature is allowed, otherwise false
     */
    public synchronized boolean isAllowedByLicense(OperationMode minimumMode, boolean needActive, boolean allowTrial) {
        if (needActive && false == status.active) {
            return false;
        }
        return isAllowedByOperationMode(status.mode, minimumMode, allowTrial);
    }

    /**
     * A convenient method to test whether a feature is by license status.
     * @see #isAllowedByLicense(OperationMode, boolean, boolean)
     *
     * @param minimumMode  The minimum license to meet or exceed
     */
    public synchronized boolean isAllowedByLicense(OperationMode minimumMode) {
        return isAllowedByLicense(minimumMode, true, true);
    }

}
