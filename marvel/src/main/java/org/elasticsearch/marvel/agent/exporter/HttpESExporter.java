/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererRegistry;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.agent.support.AgentUtils;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class HttpESExporter extends AbstractExporter<HttpESExporter> implements NodeSettingsService.Listener {

    private static final String NAME = "es_exporter";

    private static final String SETTINGS_PREFIX = "marvel.agent.exporter.es.";
    public static final String SETTINGS_HOSTS = SETTINGS_PREFIX + "hosts";
    public static final String SETTINGS_INDEX_TIME_FORMAT = SETTINGS_PREFIX + "index.timeformat";
    public static final String SETTINGS_TIMEOUT = SETTINGS_PREFIX + "timeout";
    public static final String SETTINGS_READ_TIMEOUT = SETTINGS_PREFIX + "read_timeout";

    // es level timeout used when checking and writing templates (used to speed up tests)
    public static final String SETTINGS_CHECK_TEMPLATE_TIMEOUT = SETTINGS_PREFIX + ".template.master_timeout";

    // es level timeout used for bulk indexing (used to speed up tests)
    public static final String SETTINGS_BULK_TIMEOUT = SETTINGS_PREFIX + ".bulk.timeout";

    public static final String DEFAULT_INDEX_TIME_FORMAT = "YYYY.MM.dd";

    volatile String[] hosts;
    volatile boolean boundToLocalNode = false;
    volatile DateTimeFormatter indexTimeFormatter;
    volatile int timeoutInMillis;
    volatile int readTimeoutInMillis;


    /** https support * */
    final SSLSocketFactory sslSocketFactory;
    volatile boolean hostnameVerification;

    final ClusterService clusterService;
    final ClusterName clusterName;
    final NodeService nodeService;
    final Environment environment;
    final RendererRegistry registry;

    HttpServer httpServer;
    final boolean httpEnabled;

    @Nullable
    final TimeValue templateCheckTimeout;
    @Nullable
    final TimeValue bulkTimeout;

    volatile boolean checkedAndUploadedIndexTemplate = false;

    final ConnectionKeepAliveWorker keepAliveWorker;
    Thread keepAliveThread;

    @Inject
    public HttpESExporter(Settings settings, ClusterService clusterService, ClusterName clusterName,
                          NodeSettingsService nodeSettingsService,
                          NodeService nodeService, Environment environment,
                          RendererRegistry registry) {
        super(settings, NAME, clusterService);

        this.clusterService = clusterService;

        this.clusterName = clusterName;
        this.nodeService = nodeService;
        this.environment = environment;
        this.registry = registry;

        httpEnabled = settings.getAsBoolean(Node.HTTP_ENABLED, true);

        hosts = settings.getAsArray(SETTINGS_HOSTS, Strings.EMPTY_ARRAY);

        validateHosts(hosts);

        String indexTimeFormat = settings.get(SETTINGS_INDEX_TIME_FORMAT, DEFAULT_INDEX_TIME_FORMAT);
        try {
            logger.debug("checking that index time format [{}] is correct", indexTimeFormat);
            indexTimeFormatter = DateTimeFormat.forPattern(indexTimeFormat).withZoneUTC();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid marvel index time format [" + indexTimeFormat + "] configured in setting [" + SETTINGS_INDEX_TIME_FORMAT + "]", e);
        }

        timeoutInMillis = (int) settings.getAsTime(SETTINGS_TIMEOUT, new TimeValue(6000)).millis();
        readTimeoutInMillis = (int) settings.getAsTime(SETTINGS_READ_TIMEOUT, new TimeValue(timeoutInMillis * 10)).millis();

        templateCheckTimeout = settings.getAsTime(SETTINGS_CHECK_TEMPLATE_TIMEOUT, null);
        bulkTimeout = settings.getAsTime(SETTINGS_BULK_TIMEOUT, null);

        keepAliveWorker = new ConnectionKeepAliveWorker();
        nodeSettingsService.addListener(this);

        if (!settings.getByPrefix(SETTINGS_SSL_PREFIX).getAsMap().isEmpty()) {
            sslSocketFactory = createSSLSocketFactory(settings);
        } else {
            logger.trace("no ssl context configured");
            sslSocketFactory = null;
        }
        hostnameVerification = settings.getAsBoolean(SETTINGS_SSL_HOSTNAME_VERIFICATION, true);

        logger.debug("initialized with targets: {}, index prefix [{}], index time format [{}]",
                AgentUtils.santizeUrlPwds(Strings.arrayToCommaDelimitedString(hosts)), MarvelSettings.MARVEL_INDICES_PREFIX, indexTimeFormat);
    }

    static private void validateHosts(String[] hosts) {
        for (String host : hosts) {
            try {
                AgentUtils.parseHostWithPath(host, "");
            } catch (URISyntaxException e) {
                throw new RuntimeException("[marvel.agent.exporter] invalid host: [" + AgentUtils.santizeUrlPwds(host) + "]." +
                        " error: [" + AgentUtils.santizeUrlPwds(e.getMessage()) + "]");
            } catch (MalformedURLException e) {
                throw new RuntimeException("[marvel.agent.exporter] invalid host: [" + AgentUtils.santizeUrlPwds(host) + "]." +
                        " error: [" + AgentUtils.santizeUrlPwds(e.getMessage()) + "]");
            }
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @Inject(optional = true)
    public void setHttpServer(HttpServer httpServer) {
        this.httpServer = httpServer;
    }

    private HttpURLConnection openExportingConnection() {
        logger.trace("setting up an export connection");
        String queryString = "";
        if (bulkTimeout != null) {
            queryString = "?master_timeout=" + bulkTimeout;
        }
        HttpURLConnection conn = openAndValidateConnection("POST", getIndexName() + "/_bulk" + queryString, XContentType.SMILE.restContentType());
        if (conn != null && (keepAliveThread == null || !keepAliveThread.isAlive())) {
            // start keep alive upon successful connection if not there.
            initKeepAliveThread();
        }
        return conn;
    }

    private void render(OutputStream os, MarvelDoc marvelDoc) throws IOException {
        final XContentType xContentType = XContentType.SMILE;

        // Get the appropriate renderer in order to render the MarvelDoc
        Renderer renderer = registry.renderer(marvelDoc.type());
        if (renderer == null) {
            logger.warn("unable to render marvel document of type [{}]: no renderer found in registry", marvelDoc.type());
            return;
        }

        try (XContentBuilder builder = new XContentBuilder(xContentType.xContent(), os)) {

            // Builds the bulk action metadata line
            builder.startObject();
            builder.startObject("index");
            if (marvelDoc.index() != null) {
                builder.field("_index", marvelDoc.index());
            }
            if (marvelDoc.type() != null) {
                builder.field("_type", marvelDoc.type());
            }
            if (marvelDoc.id() != null) {
                builder.field("_id", marvelDoc.id());
            }
            builder.endObject();
            builder.endObject();

            // Adds action metadata line bulk separator
            renderBulkSeparator(builder);

            // Render the MarvelDoc
            renderer.render(marvelDoc,xContentType,  os);

            // Adds final bulk separator
            renderBulkSeparator(builder);
        }
    }

    private void renderBulkSeparator(XContentBuilder builder) throws IOException {
        // Flush is needed here...
        builder.flush();
        //... because the separator is written directly in the builder's stream
        builder.stream().write(builder.contentType().xContent().streamSeparator());
    }

    @SuppressWarnings("unchecked")
    private void sendCloseExportingConnection(HttpURLConnection conn) throws IOException {
        logger.trace("sending content");
        OutputStream os = conn.getOutputStream();
        os.close();
        if (conn.getResponseCode() != 200) {
            logConnectionError("remote target didn't respond with 200 OK", conn);
            return;
        }

        InputStream inputStream = conn.getInputStream();
        try (XContentParser parser = XContentType.SMILE.xContent().createParser(inputStream)) {
            Map<String, Object> response = parser.map();
            if (response.get("items") != null) {
                ArrayList<Object> list = (ArrayList<Object>) response.get("items");
                for (Object itemObject : list) {
                    Map<String, Object> actions = (Map<String, Object>) itemObject;
                    for (String actionKey : actions.keySet()) {
                        Map<String, Object> action = (Map<String, Object>) actions.get(actionKey);
                        if (action.get("error") != null) {
                            logger.error("{} failure (index:[{}] type: [{}]): {}", actionKey, action.get("_index"), action.get("_type"), action.get("error"));
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void doExport(Collection<MarvelDoc> marvelDocs) throws Exception {
        HttpURLConnection connection = openExportingConnection();
        if (connection == null) {
            return;
        }

        if ((marvelDocs != null) && (!marvelDocs.isEmpty())) {
            OutputStream os = connection.getOutputStream();

            // We need to use a buffer to render each Marvel document
            // because the renderer might close the outputstream (ex: XContentBuilder)
            try (BytesStreamOutput buffer = new BytesStreamOutput()) {
                for (MarvelDoc marvelDoc : marvelDocs) {
                    render(buffer, marvelDoc);

                    // write the result to the connection
                    os.write(buffer.bytes().toBytes());
                    buffer.reset();
                }
            } finally {
                try {
                    sendCloseExportingConnection(connection);
                } catch (IOException e) {
                    logger.error("error sending data to [{}]: {}", AgentUtils.santizeUrlPwds(connection.getURL()), AgentUtils.santizeUrlPwds(ExceptionsHelper.detailedMessage(e)));
                }
            }
        }
    }

    @Override
    protected void doStart() {
        // not initializing keep alive worker here but rather upon first exporting.
        // In the case we are sending metrics to the same ES as where the plugin is hosted
        // we want to give it some time to start.
    }


    @Override
    protected void doStop() {
        if (keepAliveThread != null && keepAliveThread.isAlive()) {
            keepAliveWorker.closed = true;
            keepAliveThread.interrupt();
            try {
                keepAliveThread.join(6000);
            } catch (InterruptedException e) {
                // don't care.
            }
        }
    }

    @Override
    protected void doClose() {
    }

    // used for testing
    String[] getHosts() {
        return hosts;
    }

    String getIndexName() {
        return MarvelSettings.MARVEL_INDICES_PREFIX + indexTimeFormatter.print(System.currentTimeMillis());

    }

    /**
     * open a connection to any host, validating it has the template installed if needed
     *
     * @return a url connection to the selected host or null if no current host is available.
     */
    private HttpURLConnection openAndValidateConnection(String method, String path) {
        return openAndValidateConnection(method, path, null);
    }

    /**
     * open a connection to any host, validating it has the template installed if needed
     *
     * @return a url connection to the selected host or null if no current host is available.
     */
    private HttpURLConnection openAndValidateConnection(String method, String path, String contentType) {
        if (hosts.length == 0) {
            // Due to how Guice injection works and because HttpServer can be optional,
            // we can't be 100% sure that the HttpServer is created when the ESExporter
            // instance is created. This is specially true in integration tests.
            // So if HttpServer is enabled in settings we can safely use the NodeService
            // to retrieve the bound address.
            BoundTransportAddress boundAddress = null;
            if (httpEnabled) {
                if ((httpServer != null) && (httpServer.lifecycleState() == Lifecycle.State.STARTED)) {
                    logger.debug("deriving host setting from httpServer");
                    boundAddress = httpServer.info().address();
                } else if (nodeService.info().getHttp() != null) {
                    logger.debug("deriving host setting from node info API");
                    boundAddress = nodeService.info().getHttp().address();
                }
            } else {
                logger.warn("http server is not enabled no hosts are manually configured");
                return null;
            }

            String[] extractedHosts = AgentUtils.extractHostsFromAddress(boundAddress, logger);
            if (extractedHosts == null || extractedHosts.length == 0) {
                return null;
            }
            hosts = extractedHosts;
            logger.trace("auto-resolved hosts to {}", (Object)extractedHosts);
            boundToLocalNode = true;
        }

        // it's important to have boundToLocalNode persistent to prevent calls during shutdown (causing ugly exceptions)
        if (boundToLocalNode && (httpServer != null) && (httpServer.lifecycleState() != Lifecycle.State.STARTED)) {
            logger.debug("local node http server is not started. can't connect");
            return null;
        }

        // out of for to move faulty hosts to the end
        int hostIndex = 0;
        try {
            for (; hostIndex < hosts.length; hostIndex++) {
                String host = hosts[hostIndex];
                if (!checkedAndUploadedIndexTemplate) {
                    // check templates first on the host
                    checkedAndUploadedIndexTemplate = checkAndUploadIndexTemplate(host);
                    if (!checkedAndUploadedIndexTemplate) {
                        continue;
                    }
                }
                HttpURLConnection connection = openConnection(host, method, path, contentType);
                if (connection != null) {
                    return connection;
                }
                // failed hosts - reset template check , someone may have restarted the target cluster and deleted
                // it's data folder. be safe.
                checkedAndUploadedIndexTemplate = false;
            }
        } finally {
            if (hostIndex > 0 && hostIndex < hosts.length) {
                logger.debug("moving [{}] failed hosts to the end of the list", hostIndex);
                String[] newHosts = new String[hosts.length];
                System.arraycopy(hosts, hostIndex, newHosts, 0, hosts.length - hostIndex);
                System.arraycopy(hosts, 0, newHosts, hosts.length - hostIndex, hostIndex);
                hosts = newHosts;
                logger.debug("preferred target host is now [{}]", AgentUtils.santizeUrlPwds(hosts[0]));
            }
        }

        logger.error("could not connect to any configured elasticsearch instances: [{}]", AgentUtils.santizeUrlPwds(Strings.arrayToCommaDelimitedString(hosts)));

        return null;

    }

    /** open a connection to the given hosts, returning null when not successful * */
    private HttpURLConnection openConnection(String host, String method, String path, @Nullable String contentType) {
        try {
            final URL url = AgentUtils.parseHostWithPath(host, path);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            if (conn instanceof HttpsURLConnection && sslSocketFactory != null) {
                HttpsURLConnection httpsConn = (HttpsURLConnection) conn;
                httpsConn.setSSLSocketFactory(sslSocketFactory);
                if (!hostnameVerification) {
                    httpsConn.setHostnameVerifier(TrustAllHostnameVerifier.INSTANCE);
                }
            }

            conn.setRequestMethod(method);
            conn.setConnectTimeout(timeoutInMillis);
            conn.setReadTimeout(readTimeoutInMillis);
            if (contentType != null) {
                conn.setRequestProperty("Content-Type", contentType);
            }
            if (url.getUserInfo() != null) {
                String basicAuth = "Basic " + Base64.encodeBytes(url.getUserInfo().getBytes("ISO-8859-1"));
                conn.setRequestProperty("Authorization", basicAuth);
            }
            conn.setUseCaches(false);
            if (method.equalsIgnoreCase("POST") || method.equalsIgnoreCase("PUT")) {
                conn.setDoOutput(true);
            }
            conn.connect();

            return conn;
        } catch (URISyntaxException e) {
            logErrorBasedOnLevel(e, "error parsing host [{}]", AgentUtils.santizeUrlPwds(host));
        } catch (IOException e) {
            logErrorBasedOnLevel(e, "error connecting to [{}]", AgentUtils.santizeUrlPwds(host));
        }
        return null;
    }

    private void logErrorBasedOnLevel(Throwable t, String msg, Object... params) {
        logger.error(msg + " [" + AgentUtils.santizeUrlPwds(t.getMessage()) + "]", params);
        if (logger.isDebugEnabled()) {
            logger.debug(msg + ". full error details:\n[{}]", params, AgentUtils.santizeUrlPwds(ExceptionsHelper.detailedMessage(t)));
        }
    }


    /**
     * Checks if the index templates already exist and if not uploads it
     * Any critical error that should prevent data exporting is communicated via an exception.
     *
     * @return true if template exists or was uploaded successfully.
     */
    private boolean checkAndUploadIndexTemplate(final String host) {
        byte[] template;
        try (InputStream is = getClass().getResourceAsStream("/marvel_index_template.json")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            template = out.toByteArray();
        } catch (IOException e) {
            // throwing an exception to stop exporting process - we don't want to send data unless
            // we put in the template for it.
            throw new RuntimeException("failed to load marvel_index_template.json", e);
        }

        try {
            int expectedVersion = AgentUtils.parseIndexVersionFromTemplate(template);
            if (expectedVersion < 0) {
                throw new RuntimeException("failed to find an index version in pre-configured index template");
            }

            String queryString = "";
            if (templateCheckTimeout != null) {
                queryString = "?timeout=" + templateCheckTimeout;
            }
            HttpURLConnection conn = openConnection(host, "GET", "_template/marvel" + queryString, null);
            if (conn == null) {
                return false;
            }

            boolean hasTemplate = false;
            if (conn.getResponseCode() == 200) {
                // verify content.
                InputStream is = conn.getInputStream();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Streams.copy(is, out);
                byte[] existingTemplate = out.toByteArray();
                is.close();
                int foundVersion = AgentUtils.parseIndexVersionFromTemplate(existingTemplate);
                if (foundVersion < 0) {
                    logger.warn("found an existing index template but couldn't extract it's version. leaving it as is.");
                    hasTemplate = true;
                } else if (foundVersion >= expectedVersion) {
                    logger.debug("accepting existing index template (version [{}], needed [{}])", foundVersion, expectedVersion);
                    hasTemplate = true;
                } else {
                    logger.debug("replacing existing index template (version [{}], needed [{}])", foundVersion, expectedVersion);
                }
            }
            // nothing there, lets create it
            if (!hasTemplate) {
                logger.debug("uploading index template");
                conn = openConnection(host, "PUT", "_template/marvel" + queryString, XContentType.JSON.restContentType());
                OutputStream os = conn.getOutputStream();
                Streams.copy(template, os);
                if (!(conn.getResponseCode() == 200 || conn.getResponseCode() == 201)) {
                    logConnectionError("error adding the marvel template to [" + host + "]", conn);
                } else {
                    hasTemplate = true;
                }
                conn.getInputStream().close(); // close and release to connection pool.
            }

            return hasTemplate;
        } catch (IOException e) {
            logger.error("failed to verify/upload the marvel template to [{}]:\n{}", AgentUtils.santizeUrlPwds(host), AgentUtils.santizeUrlPwds(e.getMessage()));
            return false;
        }
    }

    private void logConnectionError(String msg, HttpURLConnection conn) {
        InputStream inputStream = conn.getErrorStream();
        String err = "";
        if (inputStream != null) {
            java.util.Scanner s = new java.util.Scanner(inputStream, "UTF-8").useDelimiter("\\A");
            err = s.hasNext() ? s.next() : "";
        }

        try {
            logger.error("{} response code [{} {}]. content: [{}]",
                    AgentUtils.santizeUrlPwds(msg), conn.getResponseCode(),
                    AgentUtils.santizeUrlPwds(conn.getResponseMessage()),
                    AgentUtils.santizeUrlPwds(err));
        } catch (IOException e) {
            logger.error("{}. connection had an error while reporting the error. tough life.", AgentUtils.santizeUrlPwds(msg));
        }
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        TimeValue newTimeout = settings.getAsTime(SETTINGS_TIMEOUT, null);
        if (newTimeout != null) {
            logger.info("connection timeout set to [{}]", newTimeout);
            timeoutInMillis = (int) newTimeout.millis();
        }

        newTimeout = settings.getAsTime(SETTINGS_READ_TIMEOUT, null);
        if (newTimeout != null) {
            logger.info("connection read timeout set to [{}]", newTimeout);
            readTimeoutInMillis = (int) newTimeout.millis();
        }

        String[] newHosts = settings.getAsArray(SETTINGS_HOSTS, null);
        if (newHosts != null) {
            logger.info("hosts set to [{}]", AgentUtils.santizeUrlPwds(Strings.arrayToCommaDelimitedString(newHosts)));
            this.hosts = newHosts;
            this.checkedAndUploadedIndexTemplate = false;
            this.boundToLocalNode = false;
        }

        Boolean newHostnameVerification = settings.getAsBoolean(SETTINGS_SSL_HOSTNAME_VERIFICATION, null);
        if (newHostnameVerification != null) {
            logger.info("hostname verification set to [{}]", newHostnameVerification);
            this.hostnameVerification = newHostnameVerification;
        }

        String newIndexTimeFormat = settings.get(SETTINGS_INDEX_TIME_FORMAT, null);
        if (newIndexTimeFormat != null) {
            try {
                indexTimeFormatter = DateTimeFormat.forPattern(newIndexTimeFormat).withZoneUTC();
            } catch (IllegalArgumentException e) {
                logger.error("Unable to update marvel index time format: format [" + newIndexTimeFormat + "] is invalid", e);
            }
        }
    }

    protected void initKeepAliveThread() {
        keepAliveThread = new Thread(keepAliveWorker, EsExecutors.threadName(settings, "keep_alive"));
        keepAliveThread.setDaemon(true);
        keepAliveThread.start();
    }

    /**
     * Sadly we need to make sure we keep the connection open to the target ES a
     * Java's connection pooling closes connections if idle for 5sec.
     */
    class ConnectionKeepAliveWorker implements Runnable {
        volatile boolean closed = false;

        @Override
        public void run() {
            logger.trace("starting keep alive thread");
            while (!closed) {
                try {
                    Thread.sleep(1000);
                    if (closed) {
                        return;
                    }
                    String[] currentHosts = hosts;
                    if (currentHosts.length == 0) {
                        logger.trace("keep alive thread shutting down. no hosts defined");
                        return; // no hosts configured at the moment.
                    }
                    HttpURLConnection conn = openConnection(currentHosts[0], "GET", "", null);
                    if (conn == null) {
                        logger.trace("keep alive thread shutting down. failed to open connection to current host [{}]", AgentUtils.santizeUrlPwds(currentHosts[0]));
                        return;
                    } else {
                        conn.getInputStream().close(); // close and release to connection pool.
                    }
                } catch (InterruptedException e) {
                    // ignore, if closed, good....
                } catch (Throwable t) {
                    logger.debug("error in keep alive thread, shutting down (will be restarted after a successful connection has been made) {}",
                            AgentUtils.santizeUrlPwds(ExceptionsHelper.detailedMessage(t)));
                    return;
                }
            }
        }
    }

    private static final String SETTINGS_SSL_PREFIX = SETTINGS_PREFIX + "ssl.";

    public static final String SETTINGS_SSL_PROTOCOL = SETTINGS_SSL_PREFIX + "protocol";
    public static final String SETTINGS_SSL_TRUSTSTORE = SETTINGS_SSL_PREFIX + "truststore.path";
    public static final String SETTINGS_SSL_TRUSTSTORE_PASSWORD = SETTINGS_SSL_PREFIX + "truststore.password";
    public static final String SETTINGS_SSL_TRUSTSTORE_ALGORITHM = SETTINGS_SSL_PREFIX + "truststore.algorithm";
    public static final String SETTINGS_SSL_HOSTNAME_VERIFICATION = SETTINGS_SSL_PREFIX + "hostname_verification";

    /** SSL Initialization * */
    public SSLSocketFactory createSSLSocketFactory(Settings settings) {
        SSLContext sslContext;
        // Initialize sslContext
        try {
            String sslContextProtocol = settings.get(SETTINGS_SSL_PROTOCOL, "TLS");
            String trustStore = settings.get(SETTINGS_SSL_TRUSTSTORE, System.getProperty("javax.net.ssl.trustStore"));
            String trustStorePassword = settings.get(SETTINGS_SSL_TRUSTSTORE_PASSWORD, System.getProperty("javax.net.ssl.trustStorePassword"));
            String trustStoreAlgorithm = settings.get(SETTINGS_SSL_TRUSTSTORE_ALGORITHM, System.getProperty("ssl.TrustManagerFactory.algorithm"));

            if (trustStore == null) {
                throw new RuntimeException("truststore is not configured, use " + SETTINGS_SSL_TRUSTSTORE);
            }

            if (trustStoreAlgorithm == null) {
                trustStoreAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
            }

            logger.debug("SSL: using trustStore[{}], trustAlgorithm[{}]", trustStore, trustStoreAlgorithm);

            Path trustStorePath = environment.configFile().resolve(trustStore);
            if (!Files.exists(trustStorePath)) {
                throw new FileNotFoundException("Truststore at path [" + trustStorePath + "] does not exist");
            }

            TrustManager[] trustManagers;
            try (InputStream trustStoreStream = Files.newInputStream(trustStorePath)) {
                // Load TrustStore
                KeyStore ks = KeyStore.getInstance("jks");
                ks.load(trustStoreStream, trustStorePassword == null ? null : trustStorePassword.toCharArray());

                // Initialize a trust manager factory with the trusted store
                TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm);
                trustFactory.init(ks);

                // Retrieve the trust managers from the factory
                trustManagers = trustFactory.getTrustManagers();
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize a TrustManagerFactory", e);
            }

            sslContext = SSLContext.getInstance(sslContextProtocol);
            sslContext.init(null, trustManagers, null);
        } catch (Exception e) {
            throw new RuntimeException("[marvel.agent.exporter] failed to initialize the SSLContext", e);
        }
        return sslContext.getSocketFactory();
    }

    /**
     * Trust all hostname verifier. This simply returns true to completely disable hostname verification
     */
    static class TrustAllHostnameVerifier implements HostnameVerifier {
        static final HostnameVerifier INSTANCE = new TrustAllHostnameVerifier();

        private TrustAllHostnameVerifier() {
        }

        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }
    }
}

