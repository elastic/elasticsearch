/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.packaging.test.PackagingTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.elasticsearch.packaging.util.docker.Docker.copyFromContainer;
import static org.elasticsearch.packaging.util.docker.Docker.dockerShell;
import static org.elasticsearch.packaging.util.docker.Docker.findInContainer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ServerUtils {

    private static final Logger logger = LogManager.getLogger(ServerUtils.class);

    private static String SECURITY_DISABLED = "xpack.security.enabled: false";

    // generous timeout as nested virtualization can be quite slow ...
    private static final long waitTime = TimeUnit.MINUTES.toMillis(3);
    private static final long timeoutLength = TimeUnit.SECONDS.toMillis(30);
    private static final long requestInterval = TimeUnit.SECONDS.toMillis(5);
    private static final long dockerWaitForSecurityIndex = TimeUnit.SECONDS.toMillis(25);

    public static void waitForElasticsearch(Installation installation) throws Exception {
        final boolean securityEnabled;

        if (installation.distribution.isDocker() == false) {
            Path configFilePath = installation.config("elasticsearch.yml");
            // this is fragile, but currently doesn't deviate from a single line enablement and not worth the parsing effort
            String configFile = Files.readString(configFilePath, StandardCharsets.UTF_8);
            securityEnabled = configFile.contains(SECURITY_DISABLED) == false;
        } else {
            final Optional<String> commandLine = dockerShell.run("bash -c 'COLUMNS=2000 ps ax'")
                .stdout()
                .lines()
                .filter(line -> line.contains("org.elasticsearch.bootstrap.Elasticsearch"))
                .findFirst();
            if (commandLine.isPresent() == false) {
                throw new RuntimeException("Installation distribution is docker but a docker container is not running");
            }
            // security is enabled by default, the only way for it to be disabled is to be explicitly disabled
            securityEnabled = commandLine.get().contains("-Expack.security.enabled=false") == false;
        }

        if (securityEnabled) {
            logger.info("Waiting for elasticsearch WITH Security enabled");
            // with security enabled, we may or may not have setup a user/pass, so we use a more generic port being available check.
            // this isn't as good as a health check, but long term all this waiting should go away when node startup does not
            // make the http port available until the system is really ready to serve requests
            waitForXpack(installation);
        } else {
            logger.info("Waiting for elasticsearch WITHOUT Security enabled");
            waitForElasticsearch("green", null, installation, null, null, null);
        }
    }

    /**
     * Executes the supplied request, optionally applying HTTP basic auth if the
     * username and pasword field are supplied.
     * @param request the request to execute
     * @param username the username to supply, or null
     * @param password the password to supply, or null
     * @param caCert path to the ca certificate the server side ssl cert was generated from, or no if not using ssl
     * @return the response from the server
     * @throws IOException if an error occurs
     */
    private static HttpResponse execute(Request request, String username, String password, Path caCert) throws Exception {
        final Executor executor;
        if (caCert != null) {
            try (InputStream inStream = Files.newInputStream(caCert)) {
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                X509Certificate cert = (X509Certificate) cf.generateCertificate(inStream);
                KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());
                truststore.load(null, null);
                truststore.setCertificateEntry("myClusterCA", cert);
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(truststore, null);
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(truststore);
                SSLContext context = SSLContext.getInstance("TLSv1.2");
                context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
                final LayeredConnectionSocketFactory ssl = new SSLConnectionSocketFactory(context);
                final Registry<ConnectionSocketFactory> sfr = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", ssl)
                    .build();
                PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(sfr);
                connectionManager.setDefaultMaxPerRoute(100);
                connectionManager.setMaxTotal(200);
                connectionManager.setValidateAfterInactivity(1000);
                executor = Executor.newInstance(HttpClientBuilder.create().setConnectionManager(connectionManager).build());
            }
        } else {
            executor = Executor.newInstance();
        }

        if (username != null && password != null) {
            executor.auth(username, password);
            executor.authPreemptive(new HttpHost("localhost", 9200));
        }
        return executor.execute(request).returnResponse();
    }

    // polls every two seconds for Elasticsearch to be running on 9200
    private static void waitForXpack(Installation installation) {
        int retries = 60;
        while (retries > 0) {
            retries -= 1;
            try (Socket s = new Socket(InetAddress.getLoopbackAddress(), installation.port)) {
                return;
            } catch (IOException e) {
                // ignore, only want to establish a connection
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (installation != null) {
            FileUtils.logAllLogs(installation.logs, logger);
        }

        throw new RuntimeException("Elasticsearch (with x-pack) did not start");
    }

    public static Path getCaCert(Installation installation) throws IOException {
        if (installation.distribution.isDocker()) {
            final Path tempDir = PackagingTestCase.createTempDir("docker-ssl");
            final Path autoConfigurationDir = findInContainer(installation.config, "d", "\"certs\"");
            if (autoConfigurationDir != null) {
                final Path hostHttpCaCert = tempDir.resolve("http_ca.crt");
                copyFromContainer(autoConfigurationDir.resolve("http_ca.crt"), hostHttpCaCert);
                return hostHttpCaCert;
            } else {
                return null;
            }
        } else {
            return getCaCert(installation.config);
        }
    }

    public static Path getCaCert(Path configPath) throws IOException {
        boolean enrollmentEnabled = false;
        boolean httpSslEnabled = false;
        Path caCert = configPath.resolve("certs").resolve("ca").resolve("ca.crt");
        Path configFilePath = configPath.resolve("elasticsearch.yml");
        if (Files.exists(configFilePath)) {
            // In docker we might not even have a file, and if we do it's not in the host's FS
            Settings settings = Settings.builder().loadFromPath(configFilePath).build();
            enrollmentEnabled = settings.hasValue("xpack.security.enrollment.enabled")
                && settings.get("xpack.security.enrollment.enabled").equals("true");
            httpSslEnabled = settings.hasValue("xpack.security.http.ssl.enabled")
                && settings.get("xpack.security.http.ssl.enabled").equals("true");
        }
        if (enrollmentEnabled && httpSslEnabled) {
            assert Files.exists(caCert) == false;
            List<Path> allAutoconfTLS = FileUtils.lsGlob(configPath, "certs*");
            assertThat(allAutoconfTLS.size(), is(1));
            Path autoconfTLSDir = allAutoconfTLS.get(0);
            caCert = autoconfTLSDir.resolve("http_ca.crt");
            logger.info("Node has TLS auto-configured [" + caCert + "]");
            assert Files.exists(caCert);
        } else if (Files.exists(caCert) == false) {
            logger.info("No TLS certificate configured");
            caCert = null; // no cert, so don't use ssl
        }
        return caCert;
    }

    public static void waitForElasticsearch(
        String status,
        String index,
        Installation installation,
        String username,
        String password,
        Path caCert
    ) throws Exception {
        Objects.requireNonNull(status);
        boolean shouldRetryOnAuthNFailure = false;
        // we loop here rather than letting httpclient handle retries so we can measure the entire waiting time
        final long startTime = System.currentTimeMillis();
        long lastRequest = 0;
        long timeElapsed = 0;
        boolean started = false;
        Throwable thrownException = null;
        if (caCert == null) {
            caCert = getCaCert(installation);
        }

        while (started == false && timeElapsed < waitTime) {
            if (System.currentTimeMillis() - lastRequest > requestInterval) {
                try {

                    final HttpResponse response = execute(
                        Request.Get((caCert != null ? "https" : "http") + "://localhost:9200/_cluster/health")
                            .connectTimeout((int) timeoutLength)
                            .socketTimeout((int) timeoutLength),
                        username,
                        password,
                        caCert
                    );
                    if (response.getStatusLine().getStatusCode() >= 300) {
                        // We create the security index on startup (in order to create an enrollment token and/or set the elastic password)
                        // In Docker, even when the ELASTIC_PASSWORD is set, when the security index exists and we get an authN attempt as
                        // `elastic` , the reserved realm checks the security index first. It can happen that we check the security index
                        // too early after the security index creation in DockerTests causing an UnavailableShardsException. We retry
                        // authentication errors for a couple of seconds just to verify this is not the case.
                        if (installation.distribution.isDocker()
                            && timeElapsed < dockerWaitForSecurityIndex
                            && response.getStatusLine().getStatusCode() == 401) {
                            logger.info(
                                "Authentication against docker failed (possibly due to UnavailableShardsException for the security index)"
                                    + ", retrying..."
                            );
                            shouldRetryOnAuthNFailure = true;
                        } else {
                            final String statusLine = response.getStatusLine().toString();
                            final String body = EntityUtils.toString(response.getEntity());
                            throw new RuntimeException(
                                "Connecting to elasticsearch cluster health API failed:\n" + statusLine + "\n" + body
                            );
                        }
                    }
                    started = true;

                } catch (IOException e) {
                    if (thrownException == null) {
                        thrownException = e;
                    } else {
                        thrownException.addSuppressed(e);
                    }
                }

                lastRequest = System.currentTimeMillis();
            }

            timeElapsed = System.currentTimeMillis() - startTime;
        }

        if (started == false) {
            if (installation != null) {
                FileUtils.logAllLogs(installation.logs, logger);
            }

            throw new RuntimeException("Elasticsearch did not start", thrownException);
        }

        if (shouldRetryOnAuthNFailure == false) {
            final String url;
            if (index == null) {
                url = (caCert != null ? "https" : "http")
                    + "://localhost:9200/_cluster/health?wait_for_status="
                    + status
                    + "&timeout=60s"
                    + "&pretty";
            } else {
                url = (caCert != null ? "https" : "http")
                    + "://localhost:9200/_cluster/health/"
                    + index
                    + "?wait_for_status="
                    + status
                    + "&timeout=60s&pretty";
            }

            final String body = makeRequest(Request.Get(url), username, password, caCert);
            assertThat("cluster health response must contain desired status", body, containsString(status));
        }
    }

    public static void runElasticsearchTests() throws Exception {
        runElasticsearchTests(null, null, null);
    }

    public static void runElasticsearchTests(String username, String password, Path caCert) throws Exception {

        makeRequest(
            Request.Post((caCert != null ? "https" : "http") + "://localhost:9200/library/_doc/1?refresh=true&pretty")
                .bodyString("{ \"title\": \"Book #1\", \"pages\": 123 }", ContentType.APPLICATION_JSON),
            username,
            password,
            caCert
        );

        makeRequest(
            Request.Post((caCert != null ? "https" : "http") + "://localhost:9200/library/_doc/2?refresh=true&pretty")
                .bodyString("{ \"title\": \"Book #2\", \"pages\": 456 }", ContentType.APPLICATION_JSON),
            username,
            password,
            caCert
        );

        String count = makeRequest(
            Request.Get((caCert != null ? "https" : "http") + "://localhost:9200/library/_count?pretty"),
            username,
            password,
            caCert
        );
        assertThat(count, containsString("\"count\" : 2"));

        makeRequest(Request.Delete((caCert != null ? "https" : "http") + "://localhost:9200/library"), username, password, caCert);
    }

    public static String makeRequest(Request request) throws Exception {
        return makeRequest(request, null, null, null);
    }

    public static String makeRequest(Request request, String username, String password, Path caCert) throws Exception {
        final HttpResponse response = execute(request, username, password, caCert);
        final String body = EntityUtils.toString(response.getEntity());

        if (response.getStatusLine().getStatusCode() >= 300) {
            throw new RuntimeException("Request failed:\n" + response.getStatusLine().toString() + "\n" + body);
        }

        return body;
    }

    public static int makeRequestAndGetStatus(Request request, String username, String password, Path caCert) throws Exception {
        final HttpResponse response = execute(request, username, password, caCert);
        return response.getStatusLine().getStatusCode();
    }

    public static void disableGeoIpDownloader(Installation installation) throws IOException {
        // We don't use addSettingToExistingConfiguration because it would overwrite comments in the settings file
        // and we might want to check for them later on to test auto-configuration
        Path yml = installation.config("elasticsearch.yml");
        List<String> lines;
        try (Stream<String> allLines = Files.readAllLines(yml).stream()) {
            lines = allLines.filter(s -> s.startsWith("ingest.geoip.downloader.enabled") == false).collect(Collectors.toList());
        }
        lines.add("ingest.geoip.downloader.enabled: false");
        Files.write(yml, lines, TRUNCATE_EXISTING);
    }

    public static void enableGeoIpDownloader(Installation installation) throws IOException {
        removeSettingFromExistingConfiguration(installation, "ingest.geoip.downloader.enabled");
    }

    /**
     * Explicitly disables security features
     */
    public static void disableSecurityFeatures(Installation installation) throws IOException {
        Path yamlFile = installation.config("elasticsearch.yml");
        final Settings settings = Settings.builder().loadFromPath(yamlFile).build();
        final Settings newSettings = Settings.builder()
            .put(settings.filter(k -> k.startsWith("xpack.security") == false))
            .put("xpack.security.http.ssl.enabled", false)
            .put("xpack.security.transport.ssl.enabled", false)
            .put("xpack.security.enabled", false)
            .build();
        Files.write(
            yamlFile,
            newSettings.keySet().stream().map(k -> k + ": " + newSettings.get(k)).collect(Collectors.toList()),
            TRUNCATE_EXISTING
        );

    }

    public static void enableSecurityFeatures(Installation installation) throws IOException {
        removeSettingFromExistingConfiguration(installation, "xpack.security.enabled");
    }

    public static void disableSecurityAutoConfiguration(Installation installation) throws IOException {
        addSettingToExistingConfiguration(installation, "xpack.security.autoconfiguration.enabled", "false");
    }

    public static void enableSecurityAutoConfiguration(Installation installation) throws IOException {
        removeSettingFromExistingConfiguration(installation, "xpack.security.autoconfiguration.enabled");
    }

    public static void addSettingToExistingConfiguration(Installation installation, String setting, String value) throws IOException {
        addSettingToExistingConfiguration(installation.config, setting, value);
    }

    public static void removeSettingFromExistingConfiguration(Installation installation, String setting) throws IOException {
        removeSettingFromExistingConfiguration(installation.config, setting);
    }

    public static void addSettingToExistingConfiguration(Path confPath, String setting, String value) throws IOException {
        Path yml = confPath.resolve("elasticsearch.yml");
        final Settings settings = Settings.builder().loadFromPath(yml).build();
        final Settings newSettings = Settings.builder().put(settings).put(setting, value).build();
        Files.write(
            yml,
            newSettings.keySet().stream().map(k -> k + ": " + newSettings.get(k)).collect(Collectors.toList()),
            TRUNCATE_EXISTING
        );
    }

    public static void removeSettingFromExistingConfiguration(Path confPath, String setting) throws IOException {
        Path yml = confPath.resolve("elasticsearch.yml");
        final Settings settings = Settings.builder().loadFromPath(yml).build();
        final Settings newSettings = Settings.builder().put(settings.filter(k -> k.equals(setting) == false)).build();
        Files.write(
            yml,
            newSettings.keySet().stream().map(k -> k + ": " + newSettings.get(k)).collect(Collectors.toList()),
            TRUNCATE_EXISTING
        );
    }

}
