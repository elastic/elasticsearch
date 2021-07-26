/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.Monitoring;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.monitoring.exporter.ExportBulk;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringMigrationCoordinator;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Map.entry;

/**
 * {@code HttpExporter} uses the low-level {@link RestClient} to connect to a user-specified set of nodes for exporting Monitoring
 * documents via HTTP or HTTPS.
 * <p>
 * In addition to the set of nodes, it can be configured to use:
 * <ul>
 * <li>Certain timeouts (e.g., connection timeouts).</li>
 * <li>User authentication.</li>
 * <li>Sniffing (automatic detection of other nodes in the cluster to improve round robin behavior).</li>
 * <li>Custom headers (e.g., for proxies).</li>
 * <li>SSL / TLS.</li>
 * </ul>
 */
public class HttpExporter extends Exporter {

    private static final Logger logger = LogManager.getLogger(HttpExporter.class);

    public static final String TYPE = "http";

    private static Setting.AffixSettingDependency HTTP_TYPE_DEPENDENCY = new Setting.AffixSettingDependency() {
        @Override
        public Setting.AffixSetting<String> getSetting() {
            return Exporter.TYPE_SETTING;
        }

        @Override
        public void validate(final String key, final Object value, final Object dependency) {
            if (TYPE.equals(dependency) == false) {
                throw new SettingsException("[" + key + "] is set but type is [" + dependency + "]");
            }
        }
    };

    /**
     * A string array representing the Elasticsearch node(s) to communicate with over HTTP(S).
     */
    public static final Setting.AffixSetting<List<String>> HOST_SETTING =
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                "host",
                key -> Setting.listSetting(
                    key,
                    Collections.emptyList(),
                    Function.identity(),
                    new Setting.Validator<>() {

                        @Override
                        public void validate(final List<String> value) {

                        }

                        @Override
                        public void validate(final List<String> hosts, final Map<Setting<?>, Object> settings) {
                            final String namespace =
                                HttpExporter.HOST_SETTING.getNamespace(HttpExporter.HOST_SETTING.getConcreteSetting(key));
                            final String type = (String) settings.get(Exporter.TYPE_SETTING.getConcreteSettingForNamespace(namespace));

                            if (hosts.isEmpty()) {
                                final String defaultType =
                                    Exporter.TYPE_SETTING.getConcreteSettingForNamespace(namespace).get(Settings.EMPTY);
                                if (Objects.equals(type, defaultType)) {
                                    // hosts can only be empty if the type is unset
                                    return;
                                } else {
                                    throw new SettingsException("host list for [" + key + "] is empty but type is [" + type + "]");
                                }
                            }

                            boolean httpHostFound = false;
                            boolean httpsHostFound = false;

                            // every host must be configured
                            for (final String host : hosts) {
                                final HttpHost httpHost;

                                try {
                                    httpHost = HttpHostBuilder.builder(host).build();
                                } catch (final IllegalArgumentException e) {
                                    throw new SettingsException("[" + key + "] invalid host: [" + host + "]", e);
                                }

                                if (TYPE.equals(httpHost.getSchemeName())) {
                                    httpHostFound = true;
                                } else {
                                    httpsHostFound = true;
                                }

                                // fail if we find them configuring the scheme/protocol in different ways
                                if (httpHostFound && httpsHostFound) {
                                    throw new SettingsException("[" + key + "] must use a consistent scheme: http or https");
                                }
                            }
                        }

                        @Override
                        public Iterator<Setting<?>> settings() {
                            final String namespace =
                                HttpExporter.HOST_SETTING.getNamespace(HttpExporter.HOST_SETTING.getConcreteSetting(key));
                            final List<Setting<?>> settings = List.of(Exporter.TYPE_SETTING.getConcreteSettingForNamespace(namespace));
                            return settings.iterator();
                        }

                    },
                    Property.Dynamic,
                    Property.NodeScope),
                HTTP_TYPE_DEPENDENCY);

    /**
     * Master timeout associated with bulk requests.
     */
    public static final Setting.AffixSetting<TimeValue> BULK_TIMEOUT_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","bulk.timeout",
                    (key) -> Setting.timeSetting(key, TimeValue.MINUS_ONE, Property.Dynamic, Property.NodeScope), HTTP_TYPE_DEPENDENCY);
    /**
     * Timeout used for initiating a connection.
     */
    public static final Setting.AffixSetting<TimeValue> CONNECTION_TIMEOUT_SETTING =
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                "connection.timeout",
                (key) -> Setting.timeSetting(key, TimeValue.timeValueSeconds(6), Property.Dynamic, Property.NodeScope),
                HTTP_TYPE_DEPENDENCY);
    /**
     * Timeout used for reading from the connection.
     */
    public static final Setting.AffixSetting<TimeValue> CONNECTION_READ_TIMEOUT_SETTING =
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                "connection.read_timeout",
                (key) -> Setting.timeSetting(key, TimeValue.timeValueSeconds(60), Property.Dynamic, Property.NodeScope),
                HTTP_TYPE_DEPENDENCY);
    /**
     * Username for basic auth.
     */
    public static final Setting.AffixSetting<String> AUTH_USERNAME_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","auth.username",
                    (key) -> Setting.simpleString(
                        key,
                        new Setting.Validator<String>() {
                            @Override
                            public void validate(final String password) {
                                 // no username validation that is independent of other settings
                            }

                            @Override
                            public void validate(final String username, final Map<Setting<?>, Object> settings) {
                                final String namespace =
                                    HttpExporter.AUTH_USERNAME_SETTING.getNamespace(
                                        HttpExporter.AUTH_USERNAME_SETTING.getConcreteSetting(key));

                                if (Strings.isNullOrEmpty(username) == false) {
                                    final String type =
                                        (String) settings.get(Exporter.TYPE_SETTING.getConcreteSettingForNamespace(namespace));
                                    if ("http".equals(type) == false) {
                                        throw new SettingsException("username for [" + key + "] is set but type is [" + type + "]");
                                    }
                                }
                            }

                            @Override
                            public Iterator<Setting<?>> settings() {
                                final String namespace =
                                    HttpExporter.AUTH_USERNAME_SETTING.getNamespace(
                                        HttpExporter.AUTH_USERNAME_SETTING.getConcreteSetting(key));

                                final List<Setting<?>> settings = List.of(
                                    Exporter.TYPE_SETTING.getConcreteSettingForNamespace(namespace));
                                return settings.iterator();
                            }

                        },
                        Property.Dynamic,
                        Property.NodeScope,
                        Property.Filtered),
                HTTP_TYPE_DEPENDENCY);
    /**
     * Secure password for basic auth.
     */
    public static final Setting.AffixSetting<SecureString> AUTH_SECURE_PASSWORD_SETTING =
        Setting.affixKeySetting(
            "xpack.monitoring.exporters.",
            "auth.secure_password",
            key -> SecureSetting.secureString(key, null),
            HTTP_TYPE_DEPENDENCY);
    /**
     * The SSL settings.
     *
     * @see SSLService
     */
    public static final Setting.AffixSetting<Settings> SSL_SETTING =
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                "ssl",
                (key) -> Setting.groupSetting(key + ".", Property.Dynamic, Property.NodeScope, Property.Filtered),
                HTTP_TYPE_DEPENDENCY);

    /**
     * Proxy setting to allow users to send requests to a remote cluster that requires a proxy base path.
     */
    public static final Setting.AffixSetting<String> PROXY_BASE_PATH_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","proxy.base_path",
                    (key) -> Setting.simpleString(
                        key,
                        value -> {
                            if (Strings.isNullOrEmpty(value) == false) {
                                try {
                                    RestClientBuilder.cleanPathPrefix(value);
                                } catch (RuntimeException e) {
                                    Setting<?> concreteSetting = HttpExporter.PROXY_BASE_PATH_SETTING.getConcreteSetting(key);
                                    throw new SettingsException("[" + concreteSetting.getKey() + "] is malformed [" + value + "]", e);
                                }
                            }
                        },
                        Property.Dynamic,
                        Property.NodeScope),
                HTTP_TYPE_DEPENDENCY);
    /**
     * A boolean setting to enable or disable sniffing for extra connections.
     */
    public static final Setting.AffixSetting<Boolean> SNIFF_ENABLED_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","sniff.enabled",
                    (key) -> Setting.boolSetting(key, false, Property.Dynamic, Property.NodeScope), HTTP_TYPE_DEPENDENCY);
    /**
     * A parent setting to header key/value pairs, whose names are user defined.
     */
    public static final Setting.AffixSetting<Settings> HEADERS_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","headers",
                    (key) -> Setting.groupSetting(
                        key + ".",
                        settings -> {
                            final Set<String> names = settings.names();
                            for (String name : names) {
                                final String fullSetting = key + "." + name;
                                if (HttpExporter.BLACKLISTED_HEADERS.contains(name)) {
                                    throw new SettingsException("header cannot be overwritten via [" + fullSetting + "]");
                                }
                                final List<String> values = settings.getAsList(name);
                                if (values.isEmpty()) {
                                    throw new SettingsException("headers must have values, missing for setting [" + fullSetting + "]");
                                }
                            }
                        },
                        Property.Dynamic,
                        Property.NodeScope),
                HTTP_TYPE_DEPENDENCY);
    /**
     * Blacklist of headers that the user is not allowed to set.
     * <p>
     * Headers are blacklisted if they have the opportunity to break things and we won't be guaranteed to overwrite them.
     */
    public static final Set<String> BLACKLISTED_HEADERS = Collections.unmodifiableSet(Sets.newHashSet("Content-Length", "Content-Type"));
    /**
     * ES level timeout used when checking and writing templates (used to speed up tests)
     */
    public static final Setting.AffixSetting<TimeValue> TEMPLATE_CHECK_TIMEOUT_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","index.template.master_timeout",
                    (key) -> Setting.timeSetting(key, TimeValue.MINUS_ONE, Property.Dynamic, Property.NodeScope), HTTP_TYPE_DEPENDENCY);
    /**
     * A boolean setting to enable or disable whether to create placeholders for the old templates.
     */
    public static final Setting.AffixSetting<Boolean> TEMPLATE_CREATE_LEGACY_VERSIONS_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","index.template.create_legacy_templates",
                    (key) -> Setting.boolSetting(key, true, Property.Dynamic, Property.NodeScope), HTTP_TYPE_DEPENDENCY);
    /**
     * ES level timeout used when checking and writing pipelines (used to speed up tests)
     */
    public static final Setting.AffixSetting<TimeValue> PIPELINE_CHECK_TIMEOUT_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","index.pipeline.master_timeout",
                    (key) -> Setting.timeSetting(key, TimeValue.MINUS_ONE, Property.Dynamic, Property.NodeScope), HTTP_TYPE_DEPENDENCY);

    /**
     * Minimum supported version of the remote monitoring cluster (same major).
     */
    public static final Version MIN_SUPPORTED_CLUSTER_VERSION = Version.V_7_0_0;

    /**
     * The {@link RestClient} automatically pools connections and keeps them alive as necessary.
     */
    private final RestClient client;
    /**
     * The optional {@link Sniffer} to add hosts to the {@link #client}.
     */
    @Nullable
    private final Sniffer sniffer;
    /**
     * The parameters (query string variables) to supply with every bulk request.
     */
    private final Map<String, String> defaultParams;

    /**
     * {@link HttpResource} allow us to wait to send bulk payloads until we have confirmed the remote cluster is ready.
     */
    private final HttpResource resource;

    /**
     * {@link HttpResource} for setting up or tearing down cluster alerts specifically.
     */
    private final HttpResource alertingResource;

    /**
     * Track whether cluster alerts are allowed or not between requests. This allows us to avoid wiring a listener and to lazily change it.
     */
    private final AtomicBoolean clusterAlertsAllowed = new AtomicBoolean(false);

    /**
     * A barrier object to keep the exporter from installing or operating during a migration operation.
     */
    private final MonitoringMigrationCoordinator migrationCoordinator;

    private static final ConcurrentHashMap<String, SecureString> SECURE_AUTH_PASSWORDS = new ConcurrentHashMap<>();
    private final ThreadContext threadContext;
    private final DateFormatter dateTimeFormatter;
    private final ClusterStateListener onLocalMasterListener;

    /**
     * Helper class to separate all resources from just watcher resources
     */
    static class Resources {
        MultiHttpResource allResources;
        HttpResource alertingResource;

        Resources(MultiHttpResource allResources, HttpResource alertingResource) {
            this.allResources = allResources;
            this.alertingResource = alertingResource;
        }
    }

    /**
     * Create an {@link HttpExporter}.
     *
     * @param config The HTTP Exporter's configuration
     * @param sslService The SSL Service used to create the SSL Context necessary for TLS / SSL communication
     * @param threadContext The thread context that should be used for async operations
     * @param migrationCoordinator The shared coordinator for determining monitoring migrations in progress
     * @throws SettingsException if any setting is malformed
     */
    public HttpExporter(final Config config, final SSLService sslService, final ThreadContext threadContext,
                        MonitoringMigrationCoordinator migrationCoordinator) {
        this(config, sslService, threadContext, migrationCoordinator, new NodeFailureListener(), createResources(config));
    }

    /**
     * Create an {@link HttpExporter}.
     *
     * @param config The HTTP Exporter's configuration
     * @param sslService The SSL Service used to create the SSL Context necessary for TLS / SSL communication
     * @param threadContext The thread context that should be used for async operations
     * @param migrationCoordinator The shared coordinator for determining monitoring migrations in progress
     * @param listener The node failure listener used to notify an optional sniffer and resources
     * @param resource Both the resource for all things required for bulk operations and those for just cluster alerts
     * @throws SettingsException if any setting is malformed
     */
    private HttpExporter(final Config config, final SSLService sslService, final ThreadContext threadContext,
                         final MonitoringMigrationCoordinator migrationCoordinator, final NodeFailureListener listener,
                         final Resources resource) {
        this(config, sslService, threadContext, migrationCoordinator, listener, resource.allResources, resource.alertingResource);
    }

    /**
     * Create an {@link HttpExporter}.
     *
     * @param config The HTTP Exporter's configuration
     * @param sslService The SSL Service used to create the SSL Context necessary for TLS / SSL communication
     * @param threadContext The thread context that should be used for async operations
     * @param migrationCoordinator The shared coordinator for determining monitoring migrations in progress
     * @param listener The node failure listener used to notify an optional sniffer and resources
     * @param resource Blocking HTTP resource to prevent bulks until all requirements are met
     * @param alertingResource The HTTP resource used to configure cluster alerts
     * @throws SettingsException if any setting is malformed
     */
    HttpExporter(final Config config, final SSLService sslService, final ThreadContext threadContext,
                 final MonitoringMigrationCoordinator migrationCoordinator, final NodeFailureListener listener,
                 final HttpResource resource, final HttpResource alertingResource) {
        this(config, createRestClient(config, sslService, listener), threadContext, migrationCoordinator, listener, resource,
            alertingResource);
    }

    /**
     * Create an {@link HttpExporter}.
     *
     * @param config The HTTP Exporter's configuration
     * @param client The REST Client used to make all requests to the remote Elasticsearch cluster
     * @param threadContext The thread context that should be used for async operations
     * @param migrationCoordinator The shared coordinator for determining monitoring migrations in progress
     * @param listener The node failure listener used to notify an optional sniffer and resources
     * @param resource Blocking HTTP resource to prevent bulks until all requirements are met
     * @param alertingResource The HTTP resource used to configure cluster alerts
     * @throws SettingsException if any setting is malformed
     */
    HttpExporter(final Config config, final RestClient client, final ThreadContext threadContext,
                 final MonitoringMigrationCoordinator migrationCoordinator, final NodeFailureListener listener,
                 final HttpResource resource, final HttpResource alertingResource) {
        this(config, client, createSniffer(config, client, listener), threadContext, migrationCoordinator, listener, resource,
            alertingResource);
    }

    /**
     * Create an {@link HttpExporter}.
     *
     * @param config The HTTP Exporter's configuration
     * @param client The REST Client used to make all requests to the remote Elasticsearch cluster
     * @param sniffer The optional sniffer, which has already been associated with the {@code listener}
     * @param threadContext The thread context that should be used for async operations
     * @param migrationCoordinator The shared coordinator for determining monitoring migrations in progress
     * @param listener The node failure listener used to notify resources
     * @param resource Blocking HTTP resource to prevent bulks until all requirements are met
     * @param alertingResource The HTTP resource used to configure cluster alerts
     * @throws SettingsException if any setting is malformed
     */
    HttpExporter(final Config config, final RestClient client, @Nullable final Sniffer sniffer, final ThreadContext threadContext,
                 final MonitoringMigrationCoordinator migrationCoordinator, final NodeFailureListener listener,
                 final HttpResource resource, final HttpResource alertingResource) {
        super(config);

        this.client = Objects.requireNonNull(client);
        this.sniffer = sniffer;
        this.resource = resource;
        this.alertingResource = alertingResource;
        this.defaultParams = createDefaultParams(config);
        this.threadContext = threadContext;
        this.migrationCoordinator = migrationCoordinator;
        this.dateTimeFormatter = dateTimeFormatter(config);

        // mark resources as dirty after any node failure or license change
        listener.setResource(resource);

        //for a mixed cluster upgrade, ensure that if master changes and this is the master, allow the resources to re-publish
        onLocalMasterListener = clusterChangedEvent -> {
            if (clusterChangedEvent.nodesDelta().masterNodeChanged() && clusterChangedEvent.localNodeMaster()) {
                resource.markDirty();
            }
        };
        config.clusterService().addListener(onLocalMasterListener);
    }

    /**
     * Adds a validator for the {@link #SSL_SETTING} to prevent dynamic updates when secure settings also exist within that setting
     * groups (ssl context).
     * Because it is not possible to re-read the secure settings during a dynamic update, we cannot rebuild the {@link SSLIOSessionStrategy}
     * (see {@link #configureSecurity(RestClientBuilder, Config, SSLService)} if this exporter has been configured with secure settings
     */
    public static void registerSettingValidators(ClusterService clusterService, SSLService sslService) {
        clusterService.getClusterSettings().addAffixUpdateConsumer(SSL_SETTING,
            (ignoreKey, ignoreSettings) -> {
            // no-op update. We only care about the validator
            },
            (key, settings) -> {
                validateSslSettings(key, settings);
                configureSslStrategy(settings, null, sslService);
            });
    }

    /**
     * Validates that secure settings are not being used to rebuild the {@link SSLIOSessionStrategy}.
     *
     * @param exporter Name of the exporter to validate
     * @param settings Settings for the exporter
     * @throws IllegalStateException if any secure settings are used in the SSL configuration
     */
    private static void validateSslSettings(String exporter, Settings settings) {
        final List<String> secureSettings = SSLConfigurationSettings.withoutPrefix()
            .getSecureSettingsInUse(settings)
            .stream()
            .map(Setting::getKey)
            .collect(Collectors.toList());
        if (secureSettings.isEmpty() == false) {
            throw new IllegalStateException("Cannot dynamically update SSL settings for the exporter [" + exporter
                + "] as it depends on the secure setting(s) [" + Strings.collectionToCommaDelimitedString(secureSettings) + "]");
        }
    }

    /**
     * Create a {@link RestClientBuilder} from the HTTP Exporter's {@code config}.
     *
     * @param config The HTTP Exporter's configuration
     * @param sslService The SSL Service used to create the SSL Context necessary for TLS / SSL communication
     * @param listener The node failure listener used to log node failures
     * @return Never {@code null}.
     * @throws SettingsException if any required setting is missing or any setting is malformed
     */
    static RestClient createRestClient(final Config config, final SSLService sslService, final NodeFailureListener listener) {
        final RestClientBuilder builder = RestClient.builder(createHosts(config)).setFailureListener(listener);
        Setting<String> concreteSetting = PROXY_BASE_PATH_SETTING.getConcreteSettingForNamespace(config.name());
        final String proxyBasePath = concreteSetting.get(config.settings());

        // allow the user to configure proxies
        if (Strings.isNullOrEmpty(proxyBasePath) == false) {
            try {
                builder.setPathPrefix(proxyBasePath);
            } catch (final IllegalArgumentException e) {
                throw new SettingsException("[" + concreteSetting.getKey() + "] is malformed [" + proxyBasePath + "]", e);
            }
        }

        // allow the user to configure headers that go along with _every_ request
        configureHeaders(builder, config);
        // commercial X-Pack users can have Security enabled (auth and SSL/TLS), and also clusters behind proxies
        configureSecurity(builder, config, sslService);
        // timeouts for requests
        configureTimeouts(builder, config);

        return builder.build();
    }

    /**
     * Create a {@link Sniffer} from the HTTP Exporter's {@code config} for the {@code client}.
     *
     * @param config The HTTP Exporter's configuration
     * @param client The REST client to sniff
     * @param listener The node failure listener used to help improve sniffing
     * @return Can be {@code null} if the sniffer is disabled.
     * @throws IndexOutOfBoundsException if no {@linkplain #HOST_SETTING hosts} are set
     */
    static Sniffer createSniffer(final Config config, final RestClient client, final NodeFailureListener listener) {
        Sniffer sniffer = null;

        // the sniffer is allowed to be ENABLED; it's disabled by default until we think it's ready for use
        boolean sniffingEnabled = SNIFF_ENABLED_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());
        if (sniffingEnabled) {
            final List<String> hosts = HOST_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());
            // createHosts(config) ensures that all schemes are the same for all hosts!
            final ElasticsearchNodesSniffer.Scheme scheme = hosts.get(0).startsWith("https") ?
                    ElasticsearchNodesSniffer.Scheme.HTTPS : ElasticsearchNodesSniffer.Scheme.HTTP;
            final ElasticsearchNodesSniffer hostsSniffer =
                    new ElasticsearchNodesSniffer(client, ElasticsearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT, scheme);

            sniffer = Sniffer.builder(client).setNodesSniffer(hostsSniffer).build();

            // inform the sniffer whenever there's a node failure
            listener.setSniffer(sniffer);

            logger.debug("exporter [{}] using host sniffing", config.name());
        }

        return sniffer;
    }

    /**
     * Create a {@link MultiHttpResource} that can be used to block bulk exporting until all expected resources are available.
     *
     * @param config The HTTP Exporter's configuration
     * @return Never {@code null}.
     */
    static Resources createResources(final Config config) {
        final String resourceOwnerName = "xpack.monitoring.exporters." + config.name();
        // order controls the order that each is checked; more direct checks should always happen first (e.g., version checks)
        final List<HttpResource> resources = new ArrayList<>();

        // block the exporter from working against a monitoring cluster with the wrong version
        resources.add(new VersionHttpResource(resourceOwnerName, MIN_SUPPORTED_CLUSTER_VERSION));
        // load all templates (template bodies are lazily loaded on demand)
        configureTemplateResources(config, resourceOwnerName, resources);
        // load the pipeline (this will get added to as the monitoring API version increases)
        configurePipelineResources(config, resourceOwnerName, resources);

        // load the watches for cluster alerts if Watcher is available
        final HttpResource alertingResource = configureClusterAlertsResources(config, resourceOwnerName);
        if (alertingResource != null) {
            resources.add(alertingResource);
        }

        return new Resources(new MultiHttpResource(resourceOwnerName, resources), alertingResource);
    }

    /**
     * Create the {@link HttpHost}s that will be connected too.
     *
     * @param config The exporter's configuration
     * @return Never {@code null} or empty.
     * @throws SettingsException if any setting is malformed or if no host is set
     */
    private static HttpHost[] createHosts(final Config config) {
        final List<String> hosts = HOST_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());

        final List<HttpHost> httpHosts = new ArrayList<>(hosts.size());

        for (final String host : hosts) {
            final HttpHost httpHost = HttpHostBuilder.builder(host).build();
            httpHosts.add(httpHost);
        }

        logger.debug("exporter [{}] using hosts {}", config.name(), hosts);

        return httpHosts.toArray(new HttpHost[0]);
    }

    /**
     * Configures the {@linkplain RestClientBuilder#setDefaultHeaders(Header[]) default headers} to use with <em>all</em> requests.
     *
     * @param builder The REST client builder to configure
     * @param config The exporter's configuration
     * @throws SettingsException if any header is {@linkplain #BLACKLISTED_HEADERS blacklisted}
     */
    private static void configureHeaders(final RestClientBuilder builder, final Config config) {
        Setting<Settings> concreteSetting = HEADERS_SETTING.getConcreteSettingForNamespace(config.name());
        final Settings headerSettings = concreteSetting.get(config.settings());
        final Set<String> names = headerSettings.names();

        // Most users won't define headers
        if (names.isEmpty()) {
            return;
        }

        final List<Header> headers = new ArrayList<>();

        // record and validate each header as best we can
        for (final String name : names) {
            final List<String> values = headerSettings.getAsList(name);
            // add each value as a separate header; they literally appear like:
            //
            //  Warning: abc
            //  Warning: xyz
            for (final String value : values) {
                headers.add(new BasicHeader(name, value));
            }
        }

        builder.setDefaultHeaders(headers.toArray(new Header[headers.size()]));
    }

    /**
     * Configure the {@link RestClientBuilder} to use {@linkplain CredentialsProvider user authentication} and/or
     * {@linkplain SSLContext SSL / TLS}.
     *
     * @param builder The REST client builder to configure
     * @param config The exporter's configuration
     * @param sslService The SSL Service used to create the SSL Context necessary for TLS / SSL communication
     * @throws SettingsException if any setting causes issues
     */
    private static void configureSecurity(final RestClientBuilder builder, final Config config, final SSLService sslService) {
        final Setting<Settings> concreteSetting = SSL_SETTING.getConcreteSettingForNamespace(config.name());
        final Settings sslSettings = concreteSetting.get(config.settings());
        final SSLIOSessionStrategy sslStrategy = configureSslStrategy(sslSettings, concreteSetting, sslService);
        final CredentialsProvider credentialsProvider = createCredentialsProvider(config);
        List<String> hostList = HOST_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());
        // sending credentials in plaintext!
        if (credentialsProvider != null && hostList.stream().findFirst().orElse("").startsWith("https") == false) {
            logger.warn("exporter [{}] is not using https, but using user authentication with plaintext " +
                    "username/password!", config.name());
        }

        if (sslStrategy != null) {
            builder.setHttpClientConfigCallback(new SecurityHttpClientConfigCallback(sslStrategy, credentialsProvider));
        }
    }

    /**
     * Configures the {@link SSLIOSessionStrategy} to use. Relies on {@link #registerSettingValidators(ClusterService, SSLService)}
     * to prevent invalid usage of secure settings in the SSL strategy.
     * @param sslSettings The exporter's SSL settings
     * @param concreteSetting Settings to use for {@link SSLConfiguration} if secure settings are used
     * @param sslService The SSL Service used to create the SSL Context necessary for TLS / SSL communication
     * @return Appropriately configured instance of {@link SSLIOSessionStrategy}
     */
    private static SSLIOSessionStrategy configureSslStrategy(final Settings sslSettings, final Setting<Settings> concreteSetting,
                                                             final SSLService sslService) {
        final SSLIOSessionStrategy sslStrategy;
        if (SSLConfigurationSettings.withoutPrefix().getSecureSettingsInUse(sslSettings).isEmpty()) {
            // This configuration does not use secure settings, so it is possible that is has been dynamically updated.
            // We need to load a new SSL strategy in case these settings differ from the ones that the SSL service was configured with.
            sslStrategy = sslService.sslIOSessionStrategy(sslSettings);
        } else {
            // This configuration uses secure settings. We cannot load a new SSL strategy, as the secure settings have already been closed.
            // Due to #registerSettingValidators we know that the settings not been dynamically updated, and the pre-configured strategy
            // is still the correct configuration for use in this exporter.
            final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration(concreteSetting.getKey());
            sslStrategy = sslService.sslIOSessionStrategy(sslConfiguration);
        }
        return sslStrategy;
    }

    /**
     * Configure the {@link RestClientBuilder} to use initial connection and socket timeouts.
     *
     * @param builder The REST client builder to configure
     * @param config The exporter's configuration
     */
    private static void configureTimeouts(final RestClientBuilder builder, final Config config) {
        final TimeValue connectTimeout =
                CONNECTION_TIMEOUT_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());
        final TimeValue socketTimeout =
                CONNECTION_READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());

        // if the values could ever be null, then we should only set it if they're not null
        builder.setRequestConfigCallback(new TimeoutRequestConfigCallback(connectTimeout, socketTimeout));
    }


    /**
     * Caches secure settings for use when dynamically configuring HTTP exporters
     * @param settings settings used for configuring HTTP exporter
     * @return names of HTTP exporters whose secure settings changed, if any
     */
    public static List<String> loadSettings(Settings settings) {
        final List<String> changedExporters = new ArrayList<>();
        for (final String namespace : AUTH_SECURE_PASSWORD_SETTING.getNamespaces(settings)) {
            final Setting<SecureString> s = AUTH_SECURE_PASSWORD_SETTING.getConcreteSettingForNamespace(namespace);
            final SecureString securePassword = s.get(settings);
            final SecureString existingPassword = SECURE_AUTH_PASSWORDS.put(namespace, securePassword);
            if (securePassword.equals(existingPassword) == false) {
                changedExporters.add(namespace);
            }
        }
        return changedExporters;
    }

    /**
     * Creates the optional {@link CredentialsProvider} with the username/password to use with <em>all</em> requests for user
     * authentication.
     *
     * @param config The exporter's configuration
     * @return {@code null} if username and password not are provided. Otherwise the {@link CredentialsProvider} to use.
     * @throws SettingsException if the username is missing, but a password is supplied
     */
    @Nullable
    // visible for testing
    static CredentialsProvider createCredentialsProvider(final Config config) {
        final String username = AUTH_USERNAME_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());

        if (Strings.isNullOrEmpty(username)) {
            // nothing to configure; default situation for most users
            return null;
        }

        final SecureString securePassword = SECURE_AUTH_PASSWORDS.get(config.name());
        final String password = securePassword != null ? securePassword.toString() : null;

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        return credentialsProvider;
    }

    /**
     * Create the default parameters to use with bulk indexing operations.
     *
     * @param config The exporter's configuration
     * @return Never {@code null}. Can be empty.
     */
    static Map<String, String> createDefaultParams(final Config config) {
        final TimeValue bulkTimeout = BULK_TIMEOUT_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());

        final var entries = new ArrayList<Map.Entry<String, String>>(3);

        if (TimeValue.MINUS_ONE.equals(bulkTimeout) == false) {
            entries.add(entry("timeout", bulkTimeout.toString()));
        }

        // allow the use of ingest pipelines to be completely optional
        if (USE_INGEST_PIPELINE_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings())) {
            entries.add(entry("pipeline", MonitoringTemplateUtils.pipelineName(MonitoringTemplateUtils.TEMPLATE_VERSION)));
        }

        // widdle down the response to just what we care to check
        entries.add(entry("filter_path", "errors,items.*.error"));

        return Maps.ofEntries(entries);
    }

    /**
     * Adds the {@code resources} necessary for checking and publishing monitoring templates.
     *
     * @param config The HTTP Exporter's configuration
     * @param resourceOwnerName The resource owner name to display for any logging messages.
     * @param resources The resources to add too.
     */
    private static void configureTemplateResources(final Config config,
                                                   final String resourceOwnerName,
                                                   final List<HttpResource> resources) {
        final TimeValue templateTimeout =
                TEMPLATE_CHECK_TIMEOUT_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());

        // add templates not managed by resolvers
        for (final String templateId : MonitoringTemplateUtils.TEMPLATE_IDS) {
            final String templateName = MonitoringTemplateUtils.templateName(templateId);
            final Supplier<String> templateLoader = () -> MonitoringTemplateUtils.loadTemplate(templateId);

            resources.add(new TemplateHttpResource(resourceOwnerName, templateTimeout, templateName, templateLoader));
        }

        // Add dummy templates (e.g. ".monitoring-es-6") to enable the ability to check which version of the actual
        // index template (e.g. ".monitoring-es") should be applied.
        boolean createLegacyTemplates =
                TEMPLATE_CREATE_LEGACY_VERSIONS_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());
        if (createLegacyTemplates) {
            for (final String templateId : MonitoringTemplateUtils.OLD_TEMPLATE_IDS) {
                final String templateName = MonitoringTemplateUtils.oldTemplateName(templateId);
                final Supplier<String> templateLoader = () -> MonitoringTemplateUtils.createEmptyTemplate(templateId);

                resources.add(new TemplateHttpResource(resourceOwnerName, templateTimeout, templateName, templateLoader));
            }
        }
    }

    /**
     * Adds the {@code resources} necessary for checking and publishing monitoring pipelines.
     *
     * @param config The HTTP Exporter's configuration
     * @param resourceOwnerName The resource owner name to display for any logging messages.
     * @param resources The resources to add too.
     */
    private static void configurePipelineResources(final Config config, final String resourceOwnerName,
                                                   final List<HttpResource> resources) {
        // don't require pipelines if we're not using them
        if (USE_INGEST_PIPELINE_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings())) {
            final TimeValue pipelineTimeout =
                    PIPELINE_CHECK_TIMEOUT_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings());

            // add all pipelines
            for (final String pipelineId : MonitoringTemplateUtils.PIPELINE_IDS) {
                final String pipelineName = MonitoringTemplateUtils.pipelineName(pipelineId);
                // lazily load the pipeline
                final Supplier<byte[]> pipeline =
                        () -> BytesReference.toBytes(BytesReference.bytes(MonitoringTemplateUtils.loadPipeline(pipelineId,
                                                XContentType.JSON)));

                resources.add(new PipelineHttpResource(resourceOwnerName, pipelineTimeout, pipelineName, pipeline));
            }
        }
    }

    /**
     * Adds the {@code resources} necessary for checking and publishing cluster alerts.
     *
     * @param config The HTTP Exporter's configuration
     * @param resourceOwnerName The resource owner name to display for any logging messages.
     */
    private static HttpResource configureClusterAlertsResources(final Config config, final String resourceOwnerName) {
        // don't create watches if we're not using them
        if (CLUSTER_ALERTS_MANAGEMENT_SETTING.getConcreteSettingForNamespace(config.name()).get(config.settings())) {
            final ClusterService clusterService = config.clusterService();
            final List<HttpResource> watchResources = new ArrayList<>();
            final List<String> blacklist = ClusterAlertsUtil.getClusterAlertsBlacklist(config);

            // add a resource per watch
            for (final String watchId : ClusterAlertsUtil.WATCH_IDS) {
                final boolean blacklisted = blacklist.contains(watchId) || Monitoring.MIGRATION_DECOMMISSION_ALERTS.get(config.settings());
                // lazily load the cluster state to fetch the cluster UUID once it's loaded
                final Supplier<String> uniqueWatchId = () -> ClusterAlertsUtil.createUniqueWatchId(clusterService, watchId);
                final Supplier<String> watch = blacklisted ? null : () -> ClusterAlertsUtil.loadWatch(clusterService, watchId);

                watchResources.add(new ClusterAlertHttpResource(resourceOwnerName, config.licenseState(), uniqueWatchId, watch));
            }

            // wrap the watches in a conditional resource check to ensure the remote cluster has watcher available / enabled
            return new WatcherExistsHttpResource(resourceOwnerName, clusterService,
                                                        new MultiHttpResource(resourceOwnerName, watchResources));
        }
        return null;
    }

    @Override
    public void removeAlerts(Consumer<ExporterResourceStatus> listener) {
        alertingResource.checkAndPublish(client, ActionListener.wrap(
            (result) -> {
                ExporterResourceStatus status;
                if (result.isSuccess()) {
                    status = ExporterResourceStatus.ready(name(), TYPE);
                } else {
                    switch (result.getResourceState()) {
                        case CLEAN:
                            status = ExporterResourceStatus.ready(name(), TYPE);
                            break;
                        case CHECKING:
                        case DIRTY:
                            // CHECKING should be unlikely, but in case of that, we mark it as not ready
                            status = ExporterResourceStatus.notReady(name(), TYPE, result.getReason());
                            break;
                        default:
                            throw new ElasticsearchException("Illegal exporter resource status state [{}]", result.getResourceState());
                    }
                }
                listener.accept(status);
            },
            (exception) -> listener.accept(ExporterResourceStatus.notReady(name(), TYPE, exception))
        ));
    }

    @Override
    public void openBulk(final ActionListener<ExportBulk> listener) {
        final boolean canUseClusterAlerts = config.licenseState().checkFeature(Feature.MONITORING_CLUSTER_ALERTS);

        // if this changes between updates, then we need to add OR remove the watches
        if (clusterAlertsAllowed.compareAndSet(canUseClusterAlerts == false, canUseClusterAlerts)) {
            resource.markDirty();
        }

        if (migrationCoordinator.canInstall()) {
            resource.checkAndPublishIfDirty(client, ActionListener.wrap((success) -> {
                if (success) {
                    final String name = "xpack.monitoring.exporters." + config.name();

                    listener.onResponse(new HttpExportBulk(name, client, defaultParams, dateTimeFormatter, threadContext));
                } else {
                    // we're not ready yet, so keep waiting
                    listener.onResponse(null);
                }
            }, listener::onFailure));
        } else {
            // we're migrating right now, so keep waiting
            listener.onResponse(null);
        }
    }

    @Override
    public void doClose() {
        try {
            config.clusterService().removeListener(onLocalMasterListener);
            if (sniffer != null) {
                sniffer.close();
            }

        } catch (Exception e) {
            logger.error("an error occurred while closing the internal client sniffer", e);
        } finally {
            try {
                client.close();
            } catch (Exception e) {
                logger.error("an error occurred while closing the internal client", e);
            }
        }
    }

    public static List<Setting.AffixSetting<?>> getDynamicSettings() {
        return Arrays.asList(HOST_SETTING, TEMPLATE_CREATE_LEGACY_VERSIONS_SETTING, AUTH_USERNAME_SETTING, BULK_TIMEOUT_SETTING,
                CONNECTION_READ_TIMEOUT_SETTING, CONNECTION_TIMEOUT_SETTING, PIPELINE_CHECK_TIMEOUT_SETTING, PROXY_BASE_PATH_SETTING,
                SNIFF_ENABLED_SETTING, TEMPLATE_CHECK_TIMEOUT_SETTING, SSL_SETTING, HEADERS_SETTING);
    }

    public static List<Setting.AffixSetting<?>> getSecureSettings() {
        return List.of(AUTH_SECURE_PASSWORD_SETTING);
    }

    public static List<Setting.AffixSetting<?>> getSettings() {
        List<Setting.AffixSetting<?>> allSettings = new ArrayList<>(getDynamicSettings());
        allSettings.addAll(getSecureSettings());
        return allSettings;
    }
}
