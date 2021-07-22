/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.monitoring.MonitoringField;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static org.elasticsearch.license.LicenseService.LICENSE_EXPIRATION_WARNING_PERIOD;

/**
 * A holder for the current state of the license for all xpack features.
 */
public class XPackLicenseState {

    /**
     * A licensed feature.
     *
     * Each value defines the licensed state necessary for the feature to be allowed.
     */
    public enum Feature {
        SECURITY_IP_FILTERING(OperationMode.GOLD, false),
        SECURITY_AUDITING(OperationMode.GOLD, false),
        SECURITY_DLS_FLS(OperationMode.PLATINUM, false),
        SECURITY_ALL_REALMS(OperationMode.PLATINUM, false),
        SECURITY_STANDARD_REALMS(OperationMode.GOLD, false),
        SECURITY_CUSTOM_ROLE_PROVIDERS(OperationMode.PLATINUM, true),
        SECURITY_TOKEN_SERVICE(OperationMode.STANDARD, false),
        SECURITY_AUTHORIZATION_REALM(OperationMode.PLATINUM, true),
        SECURITY_AUTHORIZATION_ENGINE(OperationMode.PLATINUM, true),

        WATCHER(OperationMode.STANDARD, true),
        // TODO: should just check WATCHER directly?
        MONITORING_CLUSTER_ALERTS(OperationMode.STANDARD, true),
        MONITORING_UPDATE_RETENTION(OperationMode.STANDARD, false),

        ENCRYPTED_SNAPSHOT(OperationMode.PLATINUM, true),

        CCR(OperationMode.PLATINUM, true),

        GRAPH(OperationMode.PLATINUM, true),

        MACHINE_LEARNING(OperationMode.PLATINUM, true),

        LOGSTASH(OperationMode.STANDARD, true),

        JDBC(OperationMode.PLATINUM, true),

        ODBC(OperationMode.PLATINUM, true),

        SPATIAL_GEO_CENTROID(OperationMode.GOLD, true),

        SPATIAL_GEO_GRID(OperationMode.GOLD, true),

        SPATIAL_GEO_LINE(OperationMode.GOLD, true),

        SEARCHABLE_SNAPSHOTS(OperationMode.ENTERPRISE, true),

        OPERATOR_PRIVILEGES(OperationMode.ENTERPRISE, true),

        AUTOSCALING(OperationMode.ENTERPRISE, true);

        // NOTE: this is temporary. The Feature enum will go away in favor of LicensedFeature.
        // Embedding the feature instance here is a stopgap to allow smaller initial PR,
        // followed by PRs to convert the current consumers of the license state.
        final LicensedFeature.Momentary feature;

        Feature(OperationMode minimumOperationMode, boolean needsActive) {
            assert minimumOperationMode.compareTo(OperationMode.BASIC) > 0: minimumOperationMode.toString();
            if (needsActive) {
                this.feature = LicensedFeature.momentary(name().toLowerCase(Locale.ROOT), minimumOperationMode);
            } else {
                this.feature = LicensedFeature.momentaryLenient(name().toLowerCase(Locale.ROOT), minimumOperationMode);
            }
        }
    }

    // temporarily non tracked features which need rework in how they are checked
    // so they are not tracked as always used
    private static final Set<Feature> NON_TRACKED_FEATURES = Set.of(
        Feature.SECURITY_IP_FILTERING,
        Feature.SECURITY_ALL_REALMS,
        Feature.SECURITY_STANDARD_REALMS
    );

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
            "Aggregations provided by Analytics plugin are no longer usable."
        });
        messages.put(XPackField.CCR, new String[]{
            "Creating new follower indices will be blocked",
            "Configuring auto-follow patterns will be blocked",
            "Auto-follow patterns will no longer discover new leader indices",
            "The CCR monitoring endpoint will be blocked",
            "Existing follower indices will continue to replicate data"
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
        messages.put(XPackField.CCR, XPackLicenseState::ccrAcknowledgementMessages);
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

    private static String[] ccrAcknowledgementMessages(final OperationMode current, final OperationMode next) {
        switch (current) {
            // the current license level permits CCR
            case TRIAL:
            case PLATINUM:
            case ENTERPRISE:
                switch (next) {
                    // the next license level does not permit CCR
                    case MISSING:
                    case BASIC:
                    case STANDARD:
                    case GOLD:
                        // so CCR will be disabled
                        return new String[]{
                            "Cross-Cluster Replication will be disabled"
                        };
                }
        }
        return Strings.EMPTY_ARRAY;
    }

    private static boolean isBasic(OperationMode mode) {
        return mode == OperationMode.BASIC;
    }

    /** A wrapper for the license mode, state, and expiration date, to allow atomically swapping. */
    private static class Status {

        /** The current "mode" of the license (ie license type). */
        final OperationMode mode;

        /** True if the license is active, or false if it is expired. */
        final boolean active;

        /** The current expiration date of the license; Long.MAX_VALUE if not available yet. */
        final long licenseExpiryDate;

        Status(OperationMode mode, boolean active, long licenseExpiryDate) {
            this.mode = mode;
            this.active = active;
            this.licenseExpiryDate = licenseExpiryDate;
        }
    }

    private final List<LicenseStateListener> listeners;
    private final boolean isSecurityEnabled;
    private final boolean isSecurityExplicitlyEnabled;

    /**
     * A Map of features for which usage is tracked exclusive by last-used-time.
     * This is the most common way feature usage is tracked, so these are handled as a special case to optimize for throughput
     * (using a pre-populated map with LongAccumulator rather than concurrent map)
     */
    private final Map<LicensedFeature, LongAccumulator> featureLastUsed;

    /**
     * A Map of features for which usage is tracked exclusive by a contextual identifier and, optionally, a last-used-time.
     * A last used time of {@code -1} means that the feature is "on" and should report the current time as the last-used-time
     * (See: {@link #epochMillisProvider}, {@link #getLastUsed})
     * The are a less common way of tracking feature usage, and cannot be known in advance (as the context is variable) so these must
     *  be managed by applying concurrent controls to the map.
     */
    private final Map<FeatureUsage, Long> contextLastUsed;

    private final LongSupplier epochMillisProvider;

    // Since Status is the only field that can be updated, we do not need to synchronize access to
    // XPackLicenseState. However, if status is read multiple times in a method, it can change in between
    // reads. Methods should use `executeAgainstStatus` and `checkAgainstStatus` to ensure that the status
    // is only read once.
    private volatile Status status = new Status(OperationMode.TRIAL, true, Long.MAX_VALUE);

    public XPackLicenseState(Settings settings, LongSupplier epochMillisProvider) {
        this.listeners = new CopyOnWriteArrayList<>();
        this.isSecurityEnabled = XPackSettings.SECURITY_ENABLED.get(settings);
        this.isSecurityExplicitlyEnabled = isSecurityEnabled && isSecurityExplicitlyEnabled(settings);

        // prepopulate feature last used map with entries for non basic features, which are the ones we
        // care to actually keep track of
        // TODO: Do we want this class to know about all features? Ideally not. In which case we will need to move to lazily creating
        //  the accumulators on first usage (or perhaps when the feature is instantiated). We can probably do that within a coase grained
        //  lock (synchronized?) since it will happen only once.
        Map<LicensedFeature, LongAccumulator> lastUsed = new HashMap<>();
        for (Feature feature : Feature.values()) {
            if (NON_TRACKED_FEATURES.contains(feature) == false) {
                lastUsed.put(feature.feature, new LongAccumulator(Long::max, 0));
            }
        }
        this.featureLastUsed = lastUsed;
        this.contextLastUsed = new ConcurrentHashMap<>(); // TODO or CopyOnWriteHashMap since writes are infrequent?
        this.epochMillisProvider = epochMillisProvider;
    }

    private XPackLicenseState(
        List<LicenseStateListener> listeners,
        boolean isSecurityEnabled,
        boolean isSecurityExplicitlyEnabled,
        Status status,
        Map<LicensedFeature, LongAccumulator> featureLastUsed,
        Map<FeatureUsage, Long> contextLastUsed,
        LongSupplier epochMillisProvider
    ) {
        this.listeners = listeners;
        this.isSecurityEnabled = isSecurityEnabled;
        this.isSecurityExplicitlyEnabled = isSecurityExplicitlyEnabled;
        this.status = status;
        this.featureLastUsed = featureLastUsed;
        this.contextLastUsed = contextLastUsed;
        this.epochMillisProvider = epochMillisProvider;
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
     * @param expirationDate Expiration date of the current license.
     * @param mostRecentTrialVersion If this cluster has, at some point commenced a trial, the most recent version on which they did that.
     *                               May be {@code null} if they have never generated a trial license on this cluster, or the most recent
     *                               trial was prior to this metadata being tracked (6.1)
     */
    protected void update(OperationMode mode, boolean active, long expirationDate, @Nullable Version mostRecentTrialVersion) {
        status = new Status(mode, active, expirationDate);
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

    // Package private for tests
    /** Return true if the license is currently within its time boundaries, false otherwise. */
    public boolean isActive() {
        return checkAgainstStatus(status -> status.active);
    }

    @Deprecated
    public boolean checkFeature(Feature feature) {
        return feature.feature.check(this);
    }

    void featureUsed(LicensedFeature feature) {
        Optional.ofNullable(this.featureLastUsed.get(feature)).ifPresent(a -> a.accumulate(epochMillisProvider.getAsLong()));
        checkForExpiry(feature);
    }

    void enableUsageTracking(LicensedFeature feature, String contextName) {
        FeatureUsage usage = new FeatureUsage(feature, Objects.requireNonNull(contextName, "Context name cannot be null"));
        this.contextLastUsed.put(usage, -1L);
        checkForExpiry(feature);
    }

    void disableUsageTracking(LicensedFeature feature, String contextName) {
        FeatureUsage usage = new FeatureUsage(feature, Objects.requireNonNull(contextName, "Context name cannot be null"));
        this.contextLastUsed.replace(usage, -1L, epochMillisProvider.getAsLong());
    }

    /**
     * Checks whether the given feature is allowed by the current license.
     * <p>
     * This method should only be used when serializing whether a feature is allowed for telemetry.
     */
    @Deprecated
    public boolean isAllowed(Feature feature) {
        return isAllowed(feature.feature);
    }

    // Package protected: Only allowed to be called by LicensedFeature
    boolean isAllowed(LicensedFeature feature) {
        if (isAllowedByLicense(feature.minimumOperationMode, feature.needsActive)) {
            return true;
        }
        return false;
    }

    private void checkForExpiry(LicensedFeature feature) {
        final long licenseExpiryDate = getLicenseExpiryDate();
        final long diff = licenseExpiryDate - System.currentTimeMillis();
        if (feature.minimumOperationMode.compareTo(OperationMode.BASIC) > 0 &&
            LICENSE_EXPIRATION_WARNING_PERIOD.getMillis() > diff) {
            final long days = TimeUnit.MILLISECONDS.toDays(diff);
            final String expiryMessage = (days == 0 && diff > 0)? "expires today":
                (diff > 0? String.format(Locale.ROOT, "will expire in [%d] days", days):
                    String.format(Locale.ROOT, "expired on [%s]", LicenseService.DATE_FORMATTER.formatMillis(licenseExpiryDate)));
            HeaderWarning.addWarning("Your license {}. " +
                "Contact your administrator or update your license for continued use of features", expiryMessage);
        }
    }

    /**
     * Returns a mapping of gold+ features to the last time that feature was used.
     *
     * Note that if a feature has not been used, it will not appear in the map.
     */
    public Map<FeatureUsage, Long> getLastUsed() {
        Map<FeatureUsage, Long> lastUsed = new HashMap<>(this.featureLastUsed.size() + this.contextLastUsed.size());
        featureLastUsed.forEach((feature, accumulator) -> {
            long time = accumulator.get();
            if (time > 0) {
                lastUsed.put(new FeatureUsage(feature, null), time);
            }
        });
        contextLastUsed.forEach((usage, time) -> {
            if (time == -1) {
                time = epochMillisProvider.getAsLong();
                lastUsed.put(usage, time);
            }
        });

        return lastUsed;
    }

    public static boolean isMachineLearningAllowedForOperationMode(final OperationMode operationMode) {
        return isAllowedByOperationMode(operationMode, OperationMode.PLATINUM);
    }

    public static boolean isFipsAllowedForOperationMode(final OperationMode operationMode) {
        return isAllowedByOperationMode(operationMode, OperationMode.PLATINUM);
    }

    /**
     * Returns whether security is enabled, taking into account the default enabled state
     * based on the current license level.
     */
    public boolean isSecurityEnabled() {
        return isSecurityEnabled(status.mode, isSecurityExplicitlyEnabled, isSecurityEnabled);
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

    public static boolean isCcrAllowedForOperationMode(final OperationMode operationMode) {
        return isAllowedByOperationMode(operationMode, OperationMode.PLATINUM);
    }

    public static boolean isAllowedByOperationMode(
        final OperationMode operationMode, final OperationMode minimumMode) {
        if (OperationMode.TRIAL == operationMode) {
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
        return executeAgainstStatus(
            status -> new XPackLicenseState(
                listeners,
                isSecurityEnabled,
                isSecurityExplicitlyEnabled,
                status,
                featureLastUsed,
                contextLastUsed,
                epochMillisProvider
            )
        );
    }

    /**
     * Test whether a feature is allowed by the status of license.
     *
     * @param minimumMode  The minimum license to meet or exceed
     * @param needActive   Whether current license needs to be active
     *
     * @return true if feature is allowed, otherwise false
     */
    @Deprecated
    public boolean isAllowedByLicense(OperationMode minimumMode, boolean needActive) {
        return checkAgainstStatus(status -> {
            if (needActive && false == status.active) {
                return false;
            }
            return isAllowedByOperationMode(status.mode, minimumMode);
        });
    }

    /** Return the current license expiration date. */
    public long getLicenseExpiryDate() {
        return executeAgainstStatus(status -> status.licenseExpiryDate);
    }

    /**
     * A convenient method to test whether a feature is by license status.
     * @see #isAllowedByLicense(OperationMode, boolean)
     *
     * @param minimumMode  The minimum license to meet or exceed
     */
    public boolean isAllowedByLicense(OperationMode minimumMode) {
        return isAllowedByLicense(minimumMode, true);
    }

    public static class FeatureUsage {
        private final LicensedFeature feature;

        @Nullable
        private final String context;

        private FeatureUsage(LicensedFeature feature, String context) {
            this.feature = Objects.requireNonNull(feature, "Feature cannot be null");
            this.context = context;
        }

        @Override
        public String toString() {
            return context == null ? feature.name : feature.name + ":" + context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FeatureUsage usage = (FeatureUsage) o;
            return Objects.equals(feature, usage.feature) && Objects.equals(context, usage.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(feature, context);
        }

        public String featureName() {
            return feature.name;
        }

        public String contextName() {
            return context;
        }

        public OperationMode minimumOperationMode() {
            return feature.minimumOperationMode;
        }
    }
}
