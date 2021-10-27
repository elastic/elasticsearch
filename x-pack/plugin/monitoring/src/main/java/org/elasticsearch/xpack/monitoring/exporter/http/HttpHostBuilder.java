/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * {@code HttpHostBuilder} creates an {@link HttpHost} meant to be used with an Elasticsearch cluster. The {@code HttpHostBuilder} uses
 * defaults that are most common for Elasticsearch, including an unspecified port defaulting to <code>9200</code> and the default scheme
 * being <code>http</code> (as opposed to <code>https</code>).
 * <p>
 * The only <em>required</em> detail is the host to connect too, either via hostname or IP address.
 * <p>
 * This enables you to create an {@code HttpHost} directly via a builder mechanism, or indirectly by parsing a URI-like string. For example:
 * <pre><code>
 * HttpHost host1 = HttpHostBuilder.builder("localhost").build();               // http://localhost:9200
 * HttpHost host2 = HttpHostBuilder.builder("localhost:9200").build();          // http://localhost:9200
 * HttpHost host4 = HttpHostBuilder.builder("http://localhost:9200").build();   // http://localhost:9200
 * HttpHost host5 = HttpHostBuilder.builder("https://localhost:9200").build();  // https://localhost:9200
 * HttpHost host6 = HttpHostBuilder.builder("https://localhost:9200").build();  // https://127.0.0.1:9200 (IPv4 localhost)
 * HttpHost host7 = HttpHostBuilder.builder("http://10.1.2.3").build();         // http://10.2.3.4:9200
 * HttpHost host8 = HttpHostBuilder.builder("https://[::1]").build();           // http://[::1]:9200      (IPv6 localhost)
 * HttpHost host9 = HttpHostBuilder.builder("https://[::1]:9200").build();      // http://[::1]:9200      (IPv6 localhost)
 * HttpHost host10= HttpHostBuilder.builder("https://sub.domain").build();      // https://sub.domain:9200
 * </code></pre>
 * Note: {@code HttpHost}s are the mechanism that the {@link RestClient} uses to build the base request. If you need to specify proxy
 * settings, then use the {@link RestClientBuilder.RequestConfigCallback} to configure the {@code Proxy} settings.
 *
 * @see #builder(String)
 * @see #builder()
 */
public class HttpHostBuilder {

    /**
     * The scheme used to connect to Elasticsearch.
     */
    private Scheme scheme = Scheme.HTTP;
    /**
     * The host is the only required portion of the supplied URI when building it. The rest can be defaulted.
     */
    private String host = null;
    /**
     * The port used to connect to Elasticsearch.
     * <p>
     * The default port is 9200 when unset.
     */
    private int port = -1;

    /**
     * Create an empty {@link HttpHostBuilder}.
     * <p>
     * The expectation is that you then explicitly build the {@link HttpHost} piece-by-piece.
     * <p>
     * For example:
     * <pre><code>
     * HttpHost localhost = HttpHostBuilder.builder().host("localhost").build();                            // http://localhost:9200
     * HttpHost explicitLocalhost = HttpHostBuilder.builder.().scheme(Scheme.HTTP).host("localhost").port(9200).build();
     *                                                                                                      // http://localhost:9200
     * HttpHost secureLocalhost = HttpHostBuilder.builder().scheme(Scheme.HTTPS).host("localhost").build(); // https://localhost:9200
     * HttpHost differentPort = HttpHostBuilder.builder().host("my_host").port(19200).build();              // https://my_host:19200
     * HttpHost ipBased = HttpHostBuilder.builder().host("192.168.0.11").port(80).build();                  // https://192.168.0.11:80
     * </code></pre>
     *
     * @return Never {@code null}.
     */
    public static HttpHostBuilder builder() {
        return new HttpHostBuilder();
    }

    /**
     * Create an empty {@link HttpHostBuilder}.
     * <p>
     * The expectation is that you then explicitly build the {@link HttpHost} piece-by-piece.
     * <p>
     * For example:
     * <pre><code>
     * HttpHost localhost = HttpHostBuilder.builder("localhost").build();                     // http://localhost:9200
     * HttpHost explicitLocalhost = HttpHostBuilder.builder("http://localhost:9200").build(); // http://localhost:9200
     * HttpHost secureLocalhost = HttpHostBuilder.builder("https://localhost").build();       // https://localhost:9200
     * HttpHost differentPort = HttpHostBuilder.builder("my_host:19200").build();             // http://my_host:19200
     * HttpHost ipBased = HttpHostBuilder.builder("192.168.0.11:80").build();                 // http://192.168.0.11:80
     * </code></pre>
     *
     * @return Never {@code null}.
     * @throws NullPointerException if {@code uri} is {@code null}.
     * @throws IllegalArgumentException if any issue occurs while parsing the {@code uri}.
     */
    public static HttpHostBuilder builder(final String uri) {
        return new HttpHostBuilder(uri);
    }

    /**
     * Create a new {@link HttpHost} from scratch.
     */
    HttpHostBuilder() {
        // everything is in the default state
    }

    /**
     * Create a new {@link HttpHost} based on the supplied host.
     *
     * @param uri The [partial] URI used to build.
     * @throws NullPointerException if {@code uri} is {@code null}.
     * @throws IllegalArgumentException if any issue occurs while parsing the {@code uri}.
     */
    HttpHostBuilder(final String uri) {
        Objects.requireNonNull(uri, "uri must not be null");

        try {
            String cleanedUri = uri;

            if (uri.contains("://") == false) {
                cleanedUri = "http://" + uri;
            }

            final URI parsedUri = new URI(cleanedUri);

            // "localhost:9200" doesn't have a scheme
            if (parsedUri.getScheme() != null) {
                scheme(Scheme.fromString(parsedUri.getScheme()));
            }

            if (parsedUri.getHost() != null) {
                host(parsedUri.getHost());
            } else {
                // if the host is null, then it means one of two things: we're in a broken state _or_ it had something like underscores
                // we want the raw form so that parts of the URI are not decoded
                final String host = parsedUri.getRawAuthority();

                // they explicitly provided the port, which is unparsed when the host is null
                if (host.contains(":")) {
                    final String[] hostPort = host.split(":", 2);

                    host(hostPort[0]);
                    port(Integer.parseInt(hostPort[1]));
                } else {
                    host(host);
                }
            }

            if (parsedUri.getPort() != -1) {
                port(parsedUri.getPort());
            }

            // fail for proxies
            if (parsedUri.getRawPath() != null && parsedUri.getRawPath().isEmpty() == false) {
                throw new IllegalArgumentException(
                    "HttpHosts do not use paths ["
                        + parsedUri.getRawPath()
                        + "]. see setRequestConfigCallback for proxies. value: ["
                        + uri
                        + "]"
                );
            }
        } catch (URISyntaxException | IndexOutOfBoundsException | NullPointerException e) {
            throw new IllegalArgumentException("error parsing host: [" + uri + "]", e);
        }
    }

    /**
     * Set the scheme (aka protocol) for the {@link HttpHost}.
     *
     * @param scheme The scheme to use.
     * @return Always {@code this}.
     * @throws NullPointerException if {@code scheme} is {@code null}.
     */
    public HttpHostBuilder scheme(final Scheme scheme) {
        this.scheme = Objects.requireNonNull(scheme);

        return this;
    }

    /**
     * Set the host for the {@link HttpHost}.
     * <p>
     * This does not attempt to parse the {@code host} in any way.
     *
     * @param host The host to use.
     * @return Always {@code this}.
     * @throws NullPointerException if {@code host} is {@code null}.
     */
    public HttpHostBuilder host(final String host) {
        this.host = Objects.requireNonNull(host);

        return this;
    }

    /**
     * Set the port for the {@link HttpHost}.
     * <p>
     * Specifying the {@code port} as -1 will cause it to be defaulted to 9200 when the {@code HttpHost} is built.
     *
     * @param port The port to use.
     * @return Always {@code this}.
     * @throws IllegalArgumentException if the {@code port} is not -1 or [1, 65535].
     */
    public HttpHostBuilder port(final int port) {
        // setting a port to 0 makes no sense when you're the client; -1 allows us to use the default when we build
        if (port != -1 && (port < 1 || port > 65535)) {
            throw new IllegalArgumentException("port must be -1 for the default or [1, 65535]. was: " + port);
        }

        this.port = port;

        return this;
    }

    /**
     * Create a new {@link HttpHost} from the current {@code scheme}, {@code host}, and {@code port}.
     *
     * @return Never {@code null}.
     * @throws IllegalStateException if {@code host} is unset.
     */
    public HttpHost build() {
        if (host == null) {
            throw new IllegalStateException("host must be set");
        }

        return new HttpHost(host, port == -1 ? 9200 : port, scheme.toString());
    }

}
