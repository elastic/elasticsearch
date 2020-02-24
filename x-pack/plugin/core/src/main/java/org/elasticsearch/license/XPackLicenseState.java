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
import java.util.function.Function;
import java.util.function.Predicate;

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

    // Since Status is the only field that can be updated, we do not need to synchronize access to
    // XPackLicenseState. However, if status is read multiple times in a method, it can change in between
    // reads. Methods should use `executeAgainstStatus` and `checkAgainstStatus` to ensure that the status
    // is only read once.
    private volatile Status status = new Status(OperationMode.TRIAL, true);

    public XPackLicenseState(Settings settings) {
        this.listeners = new CopyOnWriteArrayList<>();
        this.isSecurityEnabled = XPackSettings.SECURITY_ENABLED.get(settings);
        this.isSecurityExplicitlyEnabled = isSecurityEnabled && isSecurityExplicitlyEnabled(settings);
    }

    private XPackLicenseState(List<LicenseStateListener> listeners, boolean isSecurityEnabled, boolean isSecurityExplicitlyEnabled,
                              Status status) {

        this.listeners = listeners;
        this.isSecurityEnabled = isSecurityEnabled;
        this.isSecurityExplicitlyEnabled = isSecurityExplicitlyEnabled;
        this.status = status;
    }

    private static boolean isSecurityExplicitlyEnabled(Settings settings) {
        return settings.hasValue(XPackSettings.SECURITY_ENABLED.getKey());
    }

    /** Performs function against status, only reading the status once to avoid races */
    private <T> T executeAgainstStatus(Function<Status, T> statusFn) {
        return statusFn.apply(this.status);
    }

    /** Performs predicate against status, only reading the status once to avoid races */
    private boolean checkAgainstStatus(Predicate<Status> statusPredicate) {
        return statusPredicate.test(this.status);
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
        status = new Status(mode, active);
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
    public OperationMode getOperationMode() {
        return executeAgainstStatus(status -> status.mode);
    }

    /**
     * Checks that the cluster has a valid licence of any level.
     * @see #isActive()
     */
    public boolean allowForAllLicenses() {
        return checkAgainstStatus(status -> status.active);
    }

    // Package private for tests
    /** Return true if the license is currently within its time boundaries, false otherwise. */
    public boolean isActive() {
        return checkAgainstStatus(status -> status.active);
    }

    /**
     * @return true if authentication and authorization should be enabled. this does not indicate what realms are available
     * @see #allowedRealmType() for the enabled realms
     */
    public boolean isAuthAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.BASIC, false, true);
    }

    public boolean isIpFilteringAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.GOLD, false, true);
    }

    public boolean isAuditingAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.GOLD, false, true);
    }

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
    public AllowedRealmType allowedRealmType() {
        return executeAgainstStatus(status -> {
            final boolean isSecurityCurrentlyEnabled = isSecurityEnabled(status.mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
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
        });
    }

    public boolean isCustomRoleProvidersAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.PLATINUM, true, true);
    }

    /**
     * Whether the Elasticsearch {@code TokenService} is allowed
     */
    public boolean isTokenServiceAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.GOLD, false, true);
    }

    /**
     * Whether the Elasticsearch {@code ApiKeyService} is allowed
     */
    public boolean isApiKeyServiceAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.MISSING, false, true);
    }

    /**
     * Whether "authorization_realms" is allowed
     * @see org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings
     */
    public boolean isAuthorizationRealmAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.PLATINUM, true, true);
    }

    /**
     * Whether a custom authorization engine is allowed
     * @see org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings
     */
    public boolean isAuthorizationEngineAllowed() {
        return isAllowedBySecurityAndLicense(OperationMode.PLATINUM, true, true);
    }

    public boolean isWatcherAllowed() {
        return isAllowedByLicense(OperationMode.STANDARD);
    }

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

    public boolean isGraphAllowed() {
        return isAllowedByLicense(OperationMode.PLATINUM);
    }

    public boolean isMachineLearningAllowed() {
        return isAllowedByLicense(OperationMode.PLATINUM);
    }

    public static boolean isMachineLearningAllowedForOperationMode(final OperationMode operationMode) {
        return isAllowedByOperationMode(operationMode, OperationMode.PLATINUM, true);
    }

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

    public boolean isRollupAllowed() {
        return allowForAllLicenses();
    }

    public boolean isVotingOnlyAllowed() {
        return allowForAllLicenses();
    }

    public boolean isLogstashAllowed() {
        return isAllowedByLicense(OperationMode.STANDARD);
    }

    public boolean isBeatsAllowed() {
        return isAllowedByLicense(OperationMode.STANDARD);
    }

    public boolean isDeprecationAllowed() {
        return allowForAllLicenses();
    }

    public boolean isUpgradeAllowed() {
        return allowForAllLicenses();
    }

    public boolean isIndexLifecycleAllowed() {
        return allowForAllLicenses();
    }

    public boolean isEnrichAllowed() {
        return allowForAllLicenses();
    }

    public boolean isEqlAllowed() {
        return allowForAllLicenses();
    }

    public boolean isSqlAllowed() {
        return allowForAllLicenses();
    }

    public boolean isJdbcAllowed() {
        return isAllowedByLicense(OperationMode.PLATINUM);
    }

    public boolean isFlattenedAllowed() {
        return allowForAllLicenses();
    }

    public boolean isVectorsAllowed() {
        return allowForAllLicenses();
    }

    public boolean isOdbcAllowed() {
        return isAllowedByLicense(OperationMode.PLATINUM);
    }

    public boolean isSpatialAllowed() {
        return allowForAllLicenses();
    }

    public boolean isDataScienceAllowed() {
        return allowForAllLicenses();
    }

    /**
     * @return true if security is available to be used with the current license type
     */
    public boolean isSecurityAvailable() {
        return checkAgainstStatus(status -> status.mode != OperationMode.MISSING);
    }

    /**
     * @return true if security has been disabled due it being the default setting for this license type.
     *  The conditions necessary for this are:
     *         <ul>
     *             <li>A trial or basic license</li>
     *             <li>xpack.security.enabled not specified as a setting</li>
     *         </ul>
     */
    public boolean isSecurityDisabledByLicenseDefaults() {
        return checkAgainstStatus(status -> {
            switch (status.mode) {
                case TRIAL:
                case BASIC:
                    return isSecurityEnabled && isSecurityExplicitlyEnabled == false;
            }
            return false;
        });
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
     * Determine if cross-cluster replication is allowed
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
    public XPackLicenseState copyCurrentLicenseState() {
        return executeAgainstStatus(status -> new XPackLicenseState(listeners, isSecurityEnabled, isSecurityExplicitlyEnabled, status));
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
    private boolean isAllowedBySecurityAndLicense(OperationMode minimumMode, boolean needActive, boolean allowTrial) {
        return checkAgainstStatus(status -> {
            if (false == isSecurityEnabled(status.mode, isSecurityExplicitlyEnabled, isSecurityEnabled)) {
                return false;
            }
            // Do not delegate to isAllowedByLicense as it also captures "status" which may be different from here
            if (needActive && false == status.active) {
                return false;
            }
            return isAllowedByOperationMode(status.mode, minimumMode, allowTrial);
        });
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
    public boolean isAllowedByLicense(OperationMode minimumMode, boolean needActive, boolean allowTrial) {
        return checkAgainstStatus(status -> {
            if (needActive && false == status.active) {
                return false;
            }
            return isAllowedByOperationMode(status.mode, minimumMode, allowTrial);
        });
    }

    /**
     * A convenient method to test whether a feature is by license status.
     * @see #isAllowedByLicense(OperationMode, boolean, boolean)
     *
     * @param minimumMode  The minimum license to meet or exceed
     */
    public boolean isAllowedByLicense(OperationMode minimumMode) {
        return isAllowedByLicense(minimumMode, true, true);
    }

}
