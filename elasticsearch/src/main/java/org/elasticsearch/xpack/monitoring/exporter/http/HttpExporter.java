/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.logging.log4j.Logger;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.Version;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.ElasticsearchHostsSniffer;
import org.elasticsearch.client.sniff.ElasticsearchHostsSniffer.Scheme;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.xpack.monitoring.resolver.ResolversRegistry;
import org.elasticsearch.xpack.ssl.SSLService;

import javax.net.ssl.SSLContext;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

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

    private static final Logger logger = Loggers.getLogger(HttpExporter.class);

    public static final String TYPE = "http";

    /**
     * A string array representing the Elasticsearch node(s) to communicate with over HTTP(S).
     */
    public static final String HOST_SETTING = "host";
    /**
     * Master timeout associated with bulk requests.
     */
    public static final String BULK_TIMEOUT_SETTING = "bulk.timeout";
    /**
     * Timeout used for initiating a connection.
     */
    public static final String CONNECTION_TIMEOUT_SETTING = "connection.timeout";
    /**
     * Timeout used for reading from the connection.
     */
    public static final String CONNECTION_READ_TIMEOUT_SETTING = "connection.read_timeout";
    /**
     * Username for basic auth.
     */
    public static final String AUTH_USERNAME_SETTING = "auth.username";
    /**
     * Password for basic auth.
     */
    public static final String AUTH_PASSWORD_SETTING = "auth.password";
    /**
     * The SSL settings.
     *
     * @see SSLService
     */
    public static final String SSL_SETTING = "ssl";
    /**
     * Proxy setting to allow users to send requests to a remote cluster that requires a proxy base path.
     */
    public static final String PROXY_BASE_PATH_SETTING = "proxy.base_path";
    /**
     * A boolean setting to enable or disable sniffing for extra connections.
     */
    public static final String SNIFF_ENABLED_SETTING = "sniff.enabled";
    /**
     * A parent setting to header key/value pairs, whose names are user defined.
     */
    public static final String HEADERS_SETTING = "headers";
    /**
     * Blacklist of headers that the user is not allowed to set.
     * <p>
     * Headers are blacklisted if they have the opportunity to break things and we won't be guaranteed to overwrite them.
     */
    public static final Set<String> BLACKLISTED_HEADERS = Collections.unmodifiableSet(Sets.newHashSet("Content-Length", "Content-Type"));
    /**
     * ES level timeout used when checking and writing templates (used to speed up tests)
     */
    public static final String TEMPLATE_CHECK_TIMEOUT_SETTING = "index.template.master_timeout";
    /**
     * ES level timeout used when checking and writing pipelines (used to speed up tests)
     */
    public static final String PIPELINE_CHECK_TIMEOUT_SETTING = "index.pipeline.master_timeout";
    /**
     * ES level timeout used when checking and writing aliases (used to speed up tests)
     */
    public static final String ALIAS_TIMEOUT_SETTING = "index.alias.master_timeout";

    /**
     * Minimum supported version of the remote monitoring cluster.
     * <p>
     * We must have support for ingest pipelines, which requires a minimum of 5.0.
     */
    public static final Version MIN_SUPPORTED_CLUSTER_VERSION = Version.V_5_0_0_beta1;

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

    private final ResolversRegistry resolvers;

    /**
     * Create an {@link HttpExporter}.
     *
     * @param config The HTTP Exporter's configuration
     * @param sslService The SSL Service used to create the SSL Context necessary for TLS / SSL communication
     * @throws SettingsException if any setting is malformed
     */
    public HttpExporter(final Config config, final SSLService sslService) {
        this(config, sslService, new NodeFailureListener());
    }

    /**
     * Create an {@link HttpExporter}.
     *
     * @param config The HTTP Exporter's configuration
     * @param sslService The SSL Service used to create the SSL Context necessary for TLS / SSL communication
     * @param listener The node failure listener used to notify an optional sniffer and resources
     * @throws SettingsException if any setting is malformed
     */
    HttpExporter(final Config config, final SSLService sslService, final NodeFailureListener listener) {
        this(config, createRestClient(config, sslService, listener), listener);
    }

    /**
     * Create an {@link HttpExporter}.
     *
     * @param config The HTTP Exporter's configuration
     * @param client The REST Client used to make all requests to the remote Elasticsearch cluster
     * @param listener The node failure listener used to notify an optional sniffer and resources
     * @throws SettingsException if any setting is malformed
     */
    HttpExporter(final Config config, final RestClient client, final NodeFailureListener listener) {
        this(config, client, createSniffer(config, client, listener), listener, new ResolversRegistry(config.settings()));
    }

    /**
     * Create an {@link HttpExporter}.
     *
     * @param config The HTTP Exporter's configuration
     * @param client The REST Client used to make all requests to the remote Elasticsearch cluster
     * @param listener The node failure listener used to notify an optional sniffer and resources
     * @param resolvers The resolver registry used to load templates and resolvers
     * @throws SettingsException if any setting is malformed
     */
    HttpExporter(final Config config, final RestClient client, @Nullable final Sniffer sniffer, final NodeFailureListener listener,
                 final ResolversRegistry resolvers) {
        this(config, client, sniffer, listener, resolvers, createResources(config, resolvers));
    }

    /**
     * Create an {@link HttpExporter}.
     *
     * @param config The HTTP Exporter's configuration
     * @param client The REST Client used to make all requests to the remote Elasticsearch cluster
     * @param sniffer The optional sniffer, which has already been associated with the {@code listener}
     * @param listener The node failure listener used to notify resources
     * @param resolvers The resolver registry used to load templates and resolvers
     * @param resource Blocking HTTP resource to prevent bulks until all requirements are met
     * @throws SettingsException if any setting is malformed
     */
    HttpExporter(final Config config, final RestClient client, @Nullable final Sniffer sniffer, final NodeFailureListener listener,
                 final ResolversRegistry resolvers, final HttpResource resource) {
        super(config);

        this.client = Objects.requireNonNull(client);
        this.sniffer = sniffer;
        this.resolvers = resolvers;
        this.resource = resource;
        this.defaultParams = createDefaultParams(config);

        // mark resources as dirty after any node failure
        listener.setResource(resource);
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
        final String proxyBasePath = config.settings().get(PROXY_BASE_PATH_SETTING);

        // allow the user to configure proxies
        if (proxyBasePath != null) {
            try {
                builder.setPathPrefix(proxyBasePath);
            } catch (final IllegalArgumentException e) {
                throw new SettingsException("[" + settingFQN(config, "proxy.base_path") + "] is malformed [" + proxyBasePath + "]", e);
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
        final Settings settings = config.settings();
        Sniffer sniffer = null;

        // the sniffer is allowed to be ENABLED; it's disabled by default until we think it's ready for use
        if (settings.getAsBoolean(SNIFF_ENABLED_SETTING, false)) {
            final String[] hosts = config.settings().getAsArray(HOST_SETTING);
            // createHosts(config) ensures that all schemes are the same for all hosts!
            final Scheme scheme = hosts[0].startsWith("https") ? Scheme.HTTPS : Scheme.HTTP;
            final ElasticsearchHostsSniffer hostsSniffer =
                    new ElasticsearchHostsSniffer(client, ElasticsearchHostsSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT, scheme);

            sniffer = Sniffer.builder(client).setHostsSniffer(hostsSniffer).build();

            // inform the sniffer whenever there's a node failure
            listener.setSniffer(sniffer);

            logger.debug("[" + settingFQN(config) + "] using host sniffing");
        }

        return sniffer;
    }

    /**
     * Create a {@link MultiHttpResource} that can be used to block bulk exporting until all expected resources are available.
     *
     * @param config The HTTP Exporter's configuration
     * @param resolvers The resolvers that contain all known templates.
     * @return Never {@code null}.
     */
    static MultiHttpResource createResources(final Config config, final ResolversRegistry resolvers) {
        final String resourceOwnerName = settingFQN(config);
        // order controls the order that each is checked; more direct checks should always happen first (e.g., version checks)
        final List<HttpResource> resources = new ArrayList<>();

        // block the exporter from working against a monitoring cluster with the wrong version
        resources.add(new VersionHttpResource(resourceOwnerName, MIN_SUPPORTED_CLUSTER_VERSION));
        // load all templates (template bodies are lazily loaded on demand)
        configureTemplateResources(config, resolvers, resourceOwnerName, resources);
        // load the pipeline (this will get added to as the monitoring API version increases)
        configurePipelineResources(config, resourceOwnerName, resources);
        resources.add(new BackwardsCompatibilityAliasesResource(resourceOwnerName,
                config.settings().getAsTime(ALIAS_TIMEOUT_SETTING, timeValueSeconds(30))));

        return new MultiHttpResource(resourceOwnerName, resources);
    }

    /**
     * Create the {@link HttpHost}s that will be connected too.
     *
     * @param config The exporter's configuration
     * @return Never {@code null} or empty.
     * @throws SettingsException if any setting is malformed or if no host is set
     */
    private static HttpHost[] createHosts(final Config config) {
        final String[] hosts = config.settings().getAsArray(HOST_SETTING);

        if (hosts.length == 0) {
            throw new SettingsException("missing required setting [" + settingFQN(config, HOST_SETTING) + "]");
        }

        final List<HttpHost> httpHosts = new ArrayList<>(hosts.length);
        boolean httpHostFound = false;
        boolean httpsHostFound = false;

        // every host must be configured
        for (final String host : hosts) {
            final HttpHost httpHost;

            try {
                httpHost = HttpHostBuilder.builder(host).build();
            } catch (IllegalArgumentException e) {
                throw new SettingsException("[" + settingFQN(config, HOST_SETTING) + "] invalid host: [" + host + "]", e);
            }

            if ("http".equals(httpHost.getSchemeName())) {
                httpHostFound = true;
            } else {
                httpsHostFound = true;
            }

            // fail if we find them configuring the scheme/protocol in different ways
            if (httpHostFound && httpsHostFound) {
                throw new SettingsException(
                        "[" + settingFQN(config, HOST_SETTING) + "] must use a consistent scheme: http or https");
            }

            httpHosts.add(httpHost);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[{}] using hosts [{}]", settingFQN(config), Strings.arrayToCommaDelimitedString(hosts));
        }

        return httpHosts.toArray(new HttpHost[httpHosts.size()]);
    }

    /**
     * Configures the {@linkplain RestClientBuilder#setDefaultHeaders(Header[]) default headers} to use with <em>all</em> requests.
     *
     * @param builder The REST client builder to configure
     * @param config The exporter's configuration
     * @throws SettingsException if any header is {@linkplain #BLACKLISTED_HEADERS blacklisted}
     */
    private static void configureHeaders(final RestClientBuilder builder, final Config config) {
        final Settings headerSettings = config.settings().getAsSettings(HEADERS_SETTING);
        final Set<String> names = headerSettings.names();

        // Most users won't define headers
        if (names.isEmpty()) {
            return;
        }

        final List<Header> headers = new ArrayList<>();

        // record and validate each header as best we can
        for (final String name : names) {
            if (BLACKLISTED_HEADERS.contains(name)) {
                throw new SettingsException("[" + name + "] cannot be overwritten via [" + settingFQN(config, "headers") + "]");
            }

            final String[] values = headerSettings.getAsArray(name);

            if (values.length == 0) {
                final String settingName = settingFQN(config, "headers." + name);
                throw new SettingsException("headers must have values, missing for setting [" + settingName + "]");
            }

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
        final Settings sslSettings = config.settings().getAsSettings(SSL_SETTING);
        final SSLIOSessionStrategy sslStrategy = sslService.sslIOSessionStrategy(sslSettings);
        final CredentialsProvider credentialsProvider = createCredentialsProvider(config);

        // sending credentials in plaintext!
        if (credentialsProvider != null && config.settings().getAsArray(HOST_SETTING)[0].startsWith("https") == false) {
            logger.warn("[" + settingFQN(config) + "] is not using https, but using user authentication with plaintext username/password!");
        }

        builder.setHttpClientConfigCallback(new SecurityHttpClientConfigCallback(sslStrategy, credentialsProvider));
    }

    /**
     * Configure the {@link RestClientBuilder} to use initial connection and socket timeouts.
     *
     * @param builder The REST client builder to configure
     * @param config The exporter's configuration
     */
    private static void configureTimeouts(final RestClientBuilder builder, final Config config) {
        final Settings settings = config.settings();
        final TimeValue connectTimeout = settings.getAsTime(CONNECTION_TIMEOUT_SETTING, TimeValue.timeValueMillis(6000));
        final TimeValue socketTimeout = settings.getAsTime(CONNECTION_READ_TIMEOUT_SETTING,
                                                           TimeValue.timeValueMillis(connectTimeout.millis() * 10));

        // if the values could ever be null, then we should only set it if they're not null
        builder.setRequestConfigCallback(new TimeoutRequestConfigCallback(connectTimeout, socketTimeout));
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
    private static CredentialsProvider createCredentialsProvider(final Config config) {
        final Settings settings = config.settings();
        final String username = settings.get(AUTH_USERNAME_SETTING);
        final String password = settings.get(AUTH_PASSWORD_SETTING);

        // username is required for any auth
        if (username == null) {
            if (password != null) {
                throw new SettingsException(
                        "[" + settingFQN(config, AUTH_PASSWORD_SETTING) + "] without [" + settingFQN(config, AUTH_USERNAME_SETTING) + "]");
            }
            // nothing to configure; default situation for most users
            return null;
        }

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
        final Settings settings = config.settings();
        final TimeValue bulkTimeout = settings.getAsTime(BULK_TIMEOUT_SETTING, null);

        final MapBuilder<String, String> params = new MapBuilder<>();

        if (bulkTimeout != null) {
            params.put("master_timeout", bulkTimeout.toString());
        }

        // allow the use of ingest pipelines to be completely optional
        if (settings.getAsBoolean(USE_INGEST_PIPELINE_SETTING, true)) {
            params.put("pipeline", EXPORT_PIPELINE_NAME);
        }

        // widdle down the response to just what we care to check
        params.put("filter_path", "errors,items.*.error");

        return params.immutableMap();
    }

    /**
     * Adds the {@code resources} necessary for checking and publishing monitoring templates.
     *
     * @param config The HTTP Exporter's configuration
     * @param resolvers The resolvers that contain all known templates.
     * @param resourceOwnerName The resource owner name to display for any logging messages.
     * @param resources The resources to add too.
     */
    private static void configureTemplateResources(final Config config, final ResolversRegistry resolvers, final String resourceOwnerName,
                                                   final List<HttpResource> resources) {
        final TimeValue templateTimeout = config.settings().getAsTime(TEMPLATE_CHECK_TIMEOUT_SETTING, null);
        final Set<String> templateNames = new HashSet<>();

        // add a resource to check the index mappings of the .monitoring-data-# index
        //  We ensure (and add if it's not) that the kibana type is there for the index for those few customers that upgraded from alphas;
        //  this step makes it very easy to add logstash in 5.2+ (and eventually beats)
        for (final String type : MonitoringTemplateUtils.NEW_DATA_TYPES) {
            resources.add(new DataTypeMappingHttpResource(resourceOwnerName, templateTimeout, type));
        }

        for (final MonitoringIndexNameResolver resolver : resolvers) {
            final String templateName = resolver.templateName();

            // ignore duplicates
            if (templateNames.contains(templateName) == false) {
                templateNames.add(templateName);

                resources.add(new TemplateHttpResource(resourceOwnerName, templateTimeout, templateName, resolver::template));
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
        final Settings settings = config.settings();

        // don't require pipelines if we're not using them
        if (settings.getAsBoolean(USE_INGEST_PIPELINE_SETTING, true)) {
            final TimeValue pipelineTimeout = settings.getAsTime(PIPELINE_CHECK_TIMEOUT_SETTING, null);
            // lazily load the pipeline
            final Supplier<byte[]> pipeline = () -> BytesReference.toBytes(emptyPipeline(XContentType.JSON).bytes());

            resources.add(new PipelineHttpResource(resourceOwnerName, pipelineTimeout, EXPORT_PIPELINE_NAME, pipeline));
        }
    }

    @Override
    public HttpExportBulk openBulk() {
        // block until all resources are verified to exist
        if (resource.checkAndPublishIfDirty(client)) {
            return new HttpExportBulk(settingFQN(config), client, defaultParams, resolvers);
        }

        return null;
    }

    @Override
    public void doClose() {
        try {
            if (sniffer != null) {
                sniffer.close();
            }
        } catch (IOException | RuntimeException e) {
            logger.error("an error occurred while closing the internal client sniffer", e);
        } finally {
            try {
                client.close();
            } catch (IOException | RuntimeException e) {
                logger.error("an error occurred while closing the internal client", e);
            }
        }
    }
}
