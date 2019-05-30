/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.SslHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.NullDispatcher;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.hamcrest.Matchers;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.common.Strings.collectionToDelimitedString;
import static org.elasticsearch.xpack.core.XPackSettings.DEFAULT_CIPHERS;
import static org.elasticsearch.xpack.core.ssl.explain.SslExceptionExplainer.clientExplainer;
import static org.elasticsearch.xpack.core.ssl.explain.SslExceptionExplainer.serverExplainer;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;

public class SslExceptionExplainerIntegTests extends ESTestCase {

    public void testMessageForIncompatibleCiphers() throws Exception {
        final SSLContext serverContext = SSLContext.getInstance("TLSv1.2");
        try (InputStream crtInput = getClass().getResourceAsStream("http.crt");
             InputStream keyInput = getClass().getResourceAsStream("http.key")) {
            final List<Certificate> certChain = CertParsingUtils.readCertificates(crtInput);
            final char[] password = new char[0];
            final PrivateKey key = PemUtils.readPrivateKey(new InputStreamReader(keyInput, UTF_8), "http.key", () -> password);
            final X509ExtendedKeyManager keyManager = CertParsingUtils.keyManager(certChain.toArray(Certificate[]::new), key, password);
            serverContext.init(new KeyManager[] { keyManager }, null, null);
        }

        // Force a subset cipher suite on the server
        final List<String> serverCiphers = randomSubsetOf(DEFAULT_CIPHERS.size() / 3, DEFAULT_CIPHERS);
        final SSLEngine serverEngine = new SniffingSSLEngine(serverContext.createSSLEngine());
        serverEngine.setUseClientMode(false);
        serverEngine.setEnabledCipherSuites(serverCiphers.toArray(String[]::new));

        // Force a different subset of ciphers on the client
        List<String> clientCiphers = new ArrayList<>(DEFAULT_CIPHERS);
        clientCiphers.removeAll(serverCiphers);
        clientCiphers = randomSubsetOf(clientCiphers.size() / 2, clientCiphers);
        final Settings settings = Settings.builder()
            .putList("xpack.http.ssl.cipher_suites", clientCiphers)
            .put("xpack.http.ssl.verification_mode", "none")
            .build();
        final SSLService sslService = new SSLService(settings, null);

        final NetworkService networkService = new NetworkService(Collections.emptyList());
        final MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration("xpack.http.ssl");
        final SSLSocketFactory clientSocketFactory = sslService.sslSocketFactory(sslConfiguration);
        final HttpClientBuilder clientBuilder = HttpClients.custom()
            .setSSLSocketFactory(new SSLConnectionSocketFactory(clientSocketFactory, NoopHostnameVerifier.INSTANCE));

        TestThreadPool threadPool = new TestThreadPool(getTestName() + "-threadpool");
        try (SslTestNetty4HttpServerTransport transport =
                 new SslTestNetty4HttpServerTransport(serverEngine, networkService, bigArrays, threadPool)) {

            transport.start();
            final InetSocketAddress serverAddress = transport.boundAddress().publishAddress().address();

            try (CloseableHttpClient client = clientBuilder.build();
                 CloseableHttpResponse ignored = SocketAccess.doPrivileged(() -> client.execute(new HttpGet(getUrl(serverAddress))))) {
                fail("Expected exception - incompatible ciphers");
            } catch (SSLException e) {
                final SslExceptionExplainer explainer = clientExplainer("xpack.http.ssl", serverAddress, clientSocketFactory);
                final String explained = explainer.explain(e);

                if (explained == null) {
                    throw e;
                }
                assertThat(explained, oneOf(
                    "The remote server (" + NetworkAddress.format(serverAddress) + ") rejected our SSL handshake",
                    "The remote server (" + NetworkAddress.format(serverAddress) +
                        ") closed the connection unexpectedly during SSL handshake"
                ));
            }

            assertBusy(() -> assertThat(transport.exceptions, Matchers.iterableWithSize(1)));
            assertThat(transport.exceptions.get(0).v1(), Matchers.instanceOf(DecoderException.class));

            final DecoderException decoderException = (DecoderException) transport.exceptions.get(0).v1();
            assertThat(decoderException.getCause(), Matchers.instanceOf(SSLException.class));
            SSLException serverException = (SSLException) decoderException.getCause();
            InetSocketAddress clientAddress = transport.exceptions.get(0).v2();
            final SslExceptionExplainer explainer = serverExplainer("xpack.security.http.ssl", clientAddress, serverEngine);

            final List<String> additionalCiphers = new ArrayList<>(TlsCipherSuites.JVM_SUPPORTED_CIPHER_SUITES);
            additionalCiphers.removeAll(serverCiphers);
            assertThat(explainer.explain(serverException), equalTo("The SSL connection from ["
                + NetworkAddress.format(clientAddress)
                + "] failed because we do not accept any of the client's supported ciphers.\n"
                + "The remote client supports:\n * "
                + collectionToDelimitedString(clientCiphers, " (can be supported on this JVM)\n * ")
                + " (can be supported on this JVM)\n"
                + "We are configured to support:\n * "
                + collectionToDelimitedString(serverCiphers, "\n * ")
                + "\n"
                + "The setting [xpack.security.http.ssl.cipher_suites]"
                + " can be used to configure which ciphers will be used for this type of connection"
            ));
        } finally {
            threadPool.shutdownNow();
        }
    }

    private String getUrl(InetSocketAddress address) {
        return String.format(Locale.ROOT, "https://%s:%d/", address.getHostString(), address.getPort());
    }

    private class SslTestNetty4HttpServerTransport extends Netty4HttpServerTransport {

        private final SSLEngine engine;
        private final List<Tuple<Exception, InetSocketAddress>> exceptions;

        SslTestNetty4HttpServerTransport(SSLEngine engine, NetworkService networkService, BigArrays bigArrays, ThreadPool threadPool) {
            super(httpSettings(), networkService, bigArrays, threadPool, xContentRegistry(), new NullDispatcher());
            this.engine = engine;
            this.exceptions = new ArrayList<>();
        }

        @Override
        protected void serverAcceptedChannel(HttpChannel httpChannel) {
            // Force the channel to resolve the remote address. It's too late if we wait until the exception is thrown
            // (because the connection may be closed, and Socket.getRemoteSocketAddress will return null if it is not connected)
            httpChannel.getRemoteAddress();
            super.serverAcceptedChannel(httpChannel);
        }

        @Override
        public void onException(HttpChannel channel, Exception cause) {
            final InetSocketAddress remoteAddress = channel.getRemoteAddress();
            this.exceptions.add(new Tuple<>(cause, remoteAddress));
        }

        @Override
        public ChannelHandler configureServerChannelHandler() {
            return new HttpChannelHandler(SslTestNetty4HttpServerTransport.this, SslTestNetty4HttpServerTransport.this.handlingSettings) {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    super.initChannel(ch);
                    ch.pipeline().addFirst(new SslHandler(engine));
                }
            };
        }
    }

    private static Settings httpSettings() {
        return Settings.builder()
            .put("http.bind_host", "localhost")
            .build();
    }
}
