/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The settings of the plugin, all node scoped and not dynamic. Cloud sets {@code enabled}, the endpoint ({@code url}), the
 * certificate ({@code ssl.*}) and the identity ({@code deployment_id}, {@code cluster_id}, {@code provider}, {@code region}); the
 * others have defaults that are only changed in tests.
 * <p>
 * An invalid value of a setting that only tests change, such as a period that does not divide an hour, fails the startup like any
 * bad setting. The values Cloud injects are different: when the plugin is enabled but {@code url}, the client certificate or an
 * identity value is missing, or the {@code url} is not usable, {@link #problems()} lists what is wrong and the plugin stays
 * inactive instead of failing the node, because a node that does not start for a reporting reason is worse than a node that does
 * not report.
 */
public record UsageReportingSettings(
    boolean enabled,
    String url,
    TimeValue period,
    int batchSize,
    TimeValue requestTimeout,
    int pendingMax,
    boolean reportExempt,
    String deploymentId,
    String clusterId,
    String provider,
    String region,
    boolean clientCertificateConfigured
) {

    public static final String PREFIX = "xpack.esql.usage_reporting.";
    public static final String SSL_PREFIX = PREFIX + "ssl";

    private static final long SECONDS_PER_HOUR = 3600;

    /** Master switch, off by default; Cloud turns it on. */
    public static final Setting<Boolean> ENABLED = Setting.boolSetting(PREFIX + "enabled", false, Property.NodeScope);

    /**
     * The Usage API endpoint; required when enabled. It must be {@code https} because the connection uses mutual TLS; snapshot
     * builds also accept {@code http}, so that tests can use a plain local server. It is not validated as a setting, because an
     * unusable value must leave the plugin inactive, not fail the node: see {@link #problems()}. Credentials in the url are
     * rejected, and the value is filtered from the settings output (node info API) so that a rejected value cannot expose them.
     */
    public static final Setting<String> URL = Setting.simpleString(PREFIX + "url", Property.NodeScope, Property.Filtered);

    /** The reporting interval. It is also the {@code period_seconds} of each record, so it must divide an hour exactly. */
    public static final Setting<TimeValue> PERIOD = Setting.timeSetting(
        PREFIX + "period",
        TimeValue.timeValueMinutes(5),
        UsageReportingSettings::validatePeriod,
        Property.NodeScope
    );

    /** The most records sent in one request. */
    public static final Setting<Integer> BATCH_SIZE = Setting.intSetting(PREFIX + "batch_size", 100, 1, 1000, Property.NodeScope);

    /** The timeout of one request to the Usage API. */
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.timeSetting(
        PREFIX + "request_timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueMillis(1),
        Property.NodeScope
    );

    /** The bound of the list of records that are built but not yet accepted; the oldest are dropped past it. */
    public static final Setting<Integer> PENDING_MAX = Setting.intSetting(PREFIX + "pending_max", 1000, 1, Property.NodeScope);

    /** Whether the usage of queries that did not succeed is sent, as exempt records that are not counted. */
    public static final Setting<Boolean> REPORT_EXEMPT = Setting.boolSetting(PREFIX + "report_exempt", false, Property.NodeScope);

    /** Identity values injected by Cloud: deployment id, cluster id, cloud provider (for example {@code aws}) and region. */
    public static final Setting<String> DEPLOYMENT_ID = Setting.simpleString(PREFIX + "deployment_id", Property.NodeScope);
    public static final Setting<String> CLUSTER_ID = Setting.simpleString(PREFIX + "cluster_id", Property.NodeScope);
    public static final Setting<String> PROVIDER = Setting.simpleString(PREFIX + "provider", Property.NodeScope);
    public static final Setting<String> REGION = Setting.simpleString(PREFIX + "region", Property.NodeScope);

    public static List<Setting<?>> settings() {
        return List.of(
            ENABLED,
            URL,
            PERIOD,
            BATCH_SIZE,
            REQUEST_TIMEOUT,
            PENDING_MAX,
            REPORT_EXEMPT,
            DEPLOYMENT_ID,
            CLUSTER_ID,
            PROVIDER,
            REGION
        );
    }

    public static UsageReportingSettings of(Settings settings) {
        return new UsageReportingSettings(
            ENABLED.get(settings),
            URL.get(settings),
            PERIOD.get(settings),
            BATCH_SIZE.get(settings),
            REQUEST_TIMEOUT.get(settings),
            PENDING_MAX.get(settings),
            REPORT_EXEMPT.get(settings),
            DEPLOYMENT_ID.get(settings),
            CLUSTER_ID.get(settings),
            PROVIDER.get(settings),
            REGION.get(settings),
            hasClientCertificate(settings)
        );
    }

    /**
     * What keeps the plugin from running when it is enabled: the required settings that are absent, by name, and an unusable
     * {@code url}; empty when the configuration is complete. A client certificate is required only for an {@code https} url (the
     * {@code http} of tests has no TLS). The {@code ssl.*} settings themselves are validated by x-pack-core when it loads the
     * profile, whether or not this plugin is enabled, so a partial PEM configuration (a key without a certificate) fails the
     * startup there, before this method runs.
     */
    public List<String> problems() {
        List<String> problems = new ArrayList<>();
        addIfBlank(problems, URL, url);
        if (url != null && url.isBlank() == false) {
            try {
                validateUrl(url, Build.current().isSnapshot());
            } catch (IllegalArgumentException e) {
                problems.add(URL.getKey() + " (" + e.getMessage() + ")");
            }
            if (url.regionMatches(true, 0, "https:", 0, 6) && clientCertificateConfigured == false) {
                problems.add(SSL_PREFIX + ".certificate and " + SSL_PREFIX + ".key, or " + SSL_PREFIX + ".keystore.path");
            }
        }
        addIfBlank(problems, DEPLOYMENT_ID, deploymentId);
        addIfBlank(problems, CLUSTER_ID, clusterId);
        addIfBlank(problems, PROVIDER, provider);
        addIfBlank(problems, REGION, region);
        return problems;
    }

    /** A client certificate comes either as a certificate and a key, or from a keystore. */
    private static boolean hasClientCertificate(Settings settings) {
        boolean certificateAndKey = isSet(settings, SSL_PREFIX + ".certificate") && isSet(settings, SSL_PREFIX + ".key");
        return certificateAndKey || isSet(settings, SSL_PREFIX + ".keystore.path");
    }

    /** An empty value counts as unset, as the SSL loader treats it. */
    private static boolean isSet(Settings settings, String key) {
        return settings.get(key, "").isBlank() == false;
    }

    private static void addIfBlank(List<String> missing, Setting<String> setting, String value) {
        if (value == null || value.isBlank()) {
            missing.add(setting.getKey());
        }
    }

    /** An empty value is a missing setting, reported as such by {@link #problems()}; anything else must be an absolute https URI. */
    static void validateUrl(String url, boolean allowHttp) {
        if (url.isBlank()) {
            return;
        }
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            // The value is left out of the messages: it can carry credentials (user:password@host) and the messages are logged.
            throw new IllegalArgumentException("[" + PREFIX + "url] is not a valid URI");
        }
        int port = uri.getPort();
        if (port == 0 || port > 65535) {
            throw new IllegalArgumentException("[" + PREFIX + "url] has a port outside 1 to 65535");
        }
        if (uri.getUserInfo() != null) {
            // Never legitimate with mutual TLS, and the value must not reach logs or other output, so it is not echoed.
            throw new IllegalArgumentException("[" + PREFIX + "url] must not contain credentials");
        }
        boolean schemeAllowed = "https".equalsIgnoreCase(uri.getScheme()) || (allowHttp && "http".equalsIgnoreCase(uri.getScheme()));
        if (schemeAllowed == false || uri.getHost() == null) {
            throw new IllegalArgumentException(
                "["
                    + PREFIX
                    + "url] must be an absolute https URL with a host, but has scheme ["
                    + loggableScheme(uri)
                    + "] and host ["
                    + uri.getHost()
                    + "]"
            );
        }
    }

    /**
     * A url without a scheme, such as {@code admin:secret@host}, parses as an opaque URI whose scheme is the user name, so the
     * scheme is shown only when it is one of the two expected ones.
     */
    private static String loggableScheme(URI uri) {
        String scheme = uri.getScheme();
        return "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme) ? scheme.toLowerCase(Locale.ROOT) : "unsupported";
    }

    private static void validatePeriod(TimeValue period) {
        long seconds = period.seconds();
        if (period.nanos() % TimeValue.timeValueSeconds(1).nanos() != 0 || seconds < 1 || SECONDS_PER_HOUR % seconds != 0) {
            throw new IllegalArgumentException(
                "["
                    + PREFIX
                    + "period] must be a whole number of seconds, at least 1s, that divides one hour exactly, but was ["
                    + period.getStringRep()
                    + "]"
            );
        }
    }
}
