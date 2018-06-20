/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

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
        messages.put(XPackField.SQL, XPackLicenseState::sqlAcknowledgementMessages);
        ACKNOWLEDGMENT_MESSAGES = Collections.unmodifiableMap(messages);
    }

    private static String[] securityAcknowledgementMessages(OperationMode currentMode, OperationMode newMode) {
        switch (newMode) {
            case BASIC:
                switch (currentMode) {
                    case TRIAL:
                    case STANDARD:
                    case GOLD:
                    case PLATINUM:
                        return new String[] {
                            "The following X-Pack security functionality will be disabled: authentication, authorization, " +
                                "ip filtering, and auditing. Please restart your node after applying the license.",
                            "Field and document level access control will be disabled.",
                            "Custom realms will be ignored."
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
                            "Custom realms will be ignored."
                        };
                }
                break;
            case STANDARD:
                switch (currentMode) {
                    case BASIC:
                        // ^^ though technically it was already disabled, it's not bad to remind them
                    case GOLD:
                    case PLATINUM:
                    case TRIAL:
                        return new String[] {
                            "Authentication will be limited to the native realms.",
                            "IP filtering and auditing will be disabled.",
                            "Field and document level access control will be disabled.",
                            "Custom realms will be ignored."
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
                switch (currentMode) {
                    case TRIAL:
                    case STANDARD:
                    case GOLD:
                    case PLATINUM:
                        return new String[] { "Logstash will no longer poll for centrally-managed pipelines" };
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
                        return new String[] { "JDBC support will be disabled, but you can continue to use SQL CLI and REST endpoint" };
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
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

    private volatile Status status = new Status(OperationMode.TRIAL, true);
    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();
    private final boolean isSecurityEnabled;
    private final boolean isSecurityExplicitlyEnabled;

    public XPackLicenseState(Settings settings) {
        this.isSecurityEnabled = XPackSettings.SECURITY_ENABLED.get(settings);
        // 6.0+ requires TLS for production licenses, so if TLS is enabled and security is enabled
        // we can interpret this as an explicit enabling of security if the security enabled
        // setting is not explicitly set
        this.isSecurityExplicitlyEnabled = isSecurityEnabled &&
            (settings.hasValue(XPackSettings.SECURITY_ENABLED.getKey()) || XPackSettings.TRANSPORT_SSL_ENABLED.get(settings));
    }

    /** Updates the current state of the license, which will change what features are available. */
    void update(OperationMode mode, boolean active) {
        status = new Status(mode, active);
        listeners.forEach(Runnable::run);
    }

    /** Add a listener to be notified on license change */
    public void addListener(Runnable runnable) {
        listeners.add(Objects.requireNonNull(runnable));
    }

    /** Remove a listener */
    public void removeListener(Runnable runnable) {
        listeners.remove(runnable);
    }

    /** Return the current license type. */
    public OperationMode getOperationMode() {
        return status.mode;
    }

    /** Return true if the license is currently within its time boundaries, false otherwise. */
    public boolean isActive() {
        return status.active;
    }

    /**
     * @return true if authentication and authorization should be enabled. this does not indicate what realms are available
     * @see #allowedRealmType() for the enabled realms
     */
    public boolean isAuthAllowed() {
        OperationMode mode = status.mode;
        return mode == OperationMode.STANDARD || mode == OperationMode.GOLD || mode == OperationMode.PLATINUM
            || mode == OperationMode.TRIAL;
    }

    /**
     * @return true if IP filtering should be enabled
     */
    public boolean isIpFilteringAllowed() {
        OperationMode mode = status.mode;
        return mode == OperationMode.GOLD || mode == OperationMode.PLATINUM
                || mode == OperationMode.TRIAL;
    }

    /**
     * @return true if auditing should be enabled
     */
    public boolean isAuditingAllowed() {
        OperationMode mode = status.mode;
        return mode == OperationMode.GOLD || mode == OperationMode.PLATINUM
                || mode == OperationMode.TRIAL;
    }

    /**
     * Indicates whether the stats and health API calls should be allowed. If a license is expired and past the grace
     * period then we deny these calls.
     *
     * @return true if the license allows for the stats and health APIs to be used.
     */
    public boolean isStatsAndHealthAllowed() {
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
    public boolean isDocumentAndFieldLevelSecurityAllowed() {
        OperationMode mode = status.mode;
        return mode == OperationMode.TRIAL || mode == OperationMode.PLATINUM;
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
        switch (status.mode) {
            case PLATINUM:
            case TRIAL:
                return AllowedRealmType.ALL;
            case GOLD:
                return AllowedRealmType.DEFAULT;
            case STANDARD:
                return AllowedRealmType.NATIVE;
            default:
                return AllowedRealmType.NONE;
        }
    }

    /**
     * @return whether custom role providers are allowed based on the license {@link OperationMode}
     */
    public boolean isCustomRoleProvidersAllowed() {
        final Status localStatus = status;
        return (localStatus.mode == OperationMode.PLATINUM || localStatus.mode == OperationMode.TRIAL )
                && localStatus.active;
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
    public boolean isWatcherAllowed() {
        // status is volatile, so a local variable is used for a consistent view
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
    public boolean isMonitoringAllowed() {
        return status.active;
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
    public boolean isGraphAllowed() {
        // status is volatile
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
    public boolean isMachineLearningAllowed() {
        // status is volatile
        Status localStatus = status;
        OperationMode operationMode = localStatus.mode;

        boolean licensed = operationMode == OperationMode.TRIAL || operationMode == OperationMode.PLATINUM;

        return licensed && localStatus.active;
    }

    /**
     * Rollup is always available as long as there is a valid license
     *
     * @return true if the license is active
     */
    public boolean isRollupAllowed() {
        return status.active;
    }

    /**
     * Logstash is allowed as long as there is an active license of type TRIAL, STANDARD, GOLD or PLATINUM
     * @return {@code true} as long as there is a valid license
     */
    public boolean isLogstashAllowed() {
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
     * Deprecation APIs are always allowed as long as there is an active license
     * @return {@code true} as long as there is a valid license
     */
    public boolean isDeprecationAllowed() {
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
    public boolean isUpgradeAllowed() {
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
    public boolean isSqlAllowed() {
        return status.active;
    }

    /**
     * Determine if JDBC support should be enabled.
     * <p>
     *  JDBC is available only in for {@link OperationMode#PLATINUM} and {@link OperationMode#TRIAL} licences
     */
    public boolean isJdbcAllowed() {
        // status is volatile
        Status localStatus = status;
        OperationMode operationMode = localStatus.mode;

        boolean licensed = operationMode == OperationMode.TRIAL || operationMode == OperationMode.PLATINUM;

        return licensed && localStatus.active;
    }

    public boolean isTrialLicense() {
        return status.mode == OperationMode.TRIAL;
    }

    public boolean isSecurityAvailable() {
        OperationMode mode = status.mode;
        return mode == OperationMode.GOLD || mode == OperationMode.PLATINUM || mode == OperationMode.STANDARD ||
                mode == OperationMode.TRIAL;
    }

    public boolean isSecurityEnabled() {
        final OperationMode mode = status.mode;
        return mode == OperationMode.TRIAL ? isSecurityExplicitlyEnabled : isSecurityEnabled;
    }
}
