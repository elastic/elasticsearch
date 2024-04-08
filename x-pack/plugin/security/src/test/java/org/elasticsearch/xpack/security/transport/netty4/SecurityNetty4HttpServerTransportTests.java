/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpConstants;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AsciiString;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.http.AbstractHttpServerTransportTestCase;
import org.elasticsearch.http.HttpHeadersValidationException;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.http.NullDispatcher;
import org.elasticsearch.http.netty4.Netty4FullHttpResponse;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.http.netty4.internal.HttpHeadersAuthenticatorUtils;
import org.elasticsearch.http.netty4.internal.HttpHeadersWithAuthenticationContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLEngine;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.elasticsearch.transport.Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX;
import static org.elasticsearch.xpack.security.transport.netty4.SimpleSecurityNetty4ServerTransportTests.randomCapitalization;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class SecurityNetty4HttpServerTransportTests extends AbstractHttpServerTransportTestCase {

    private SSLService sslService;
    private Environment env;
    private Path testnodeCert;
    private Path testnodeKey;

    @Before
    public void createSSLService() {
        testnodeCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        testnodeKey = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.key", testnodeKey)
            .put("xpack.security.http.ssl.certificate", testnodeCert)
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings)
            .build();
        env = TestEnvironment.newEnvironment(settings);
        sslService = new SSLService(env);
    }

    public void testDefaultClientAuth() throws Exception {
        Settings settings = Settings.builder().put(env.settings()).put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        sslService = new SSLService(TestEnvironment.newEnvironment(settings));
        Netty4HttpServerTransport transport = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            Tracer.NOOP,
            new TLSConfig(sslService.getHttpTransportSSLConfiguration(), sslService::createSSLEngine),
            null,
            randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
        );
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testOptionalClientAuth() throws Exception {
        String value = randomCapitalization(SslClientAuthenticationMode.OPTIONAL);
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.client_authentication", value)
            .build();
        sslService = new SSLService(TestEnvironment.newEnvironment(settings));
        Netty4HttpServerTransport transport = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            Tracer.NOOP,
            new TLSConfig(sslService.getHttpTransportSSLConfiguration(), sslService::createSSLEngine),
            null,
            randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
        );
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(true));
    }

    public void testRequiredClientAuth() throws Exception {
        String value = randomCapitalization(SslClientAuthenticationMode.REQUIRED);
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.client_authentication", value)
            .build();
        sslService = new SSLService(TestEnvironment.newEnvironment(settings));
        Netty4HttpServerTransport transport = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            Tracer.NOOP,
            new TLSConfig(sslService.getHttpTransportSSLConfiguration(), sslService::createSSLEngine),
            null,
            randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
        );
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(true));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testNoClientAuth() throws Exception {
        String value = randomCapitalization(SslClientAuthenticationMode.NONE);
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.client_authentication", value)
            .build();
        sslService = new SSLService(TestEnvironment.newEnvironment(settings));
        Netty4HttpServerTransport transport = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            Tracer.NOOP,
            new TLSConfig(sslService.getHttpTransportSSLConfiguration(), sslService::createSSLEngine),
            null,
            randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
        );
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testCustomSSLConfiguration() throws Exception {
        Settings settings = Settings.builder().put(env.settings()).put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        sslService = new SSLService(TestEnvironment.newEnvironment(settings));
        Netty4HttpServerTransport transport = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            Tracer.NOOP,
            new TLSConfig(sslService.getHttpTransportSSLConfiguration(), sslService::createSSLEngine),
            null,
            randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
        );
        ChannelHandler handler = transport.configureServerChannelHandler();
        EmbeddedChannel ch = new EmbeddedChannel(handler);
        SSLEngine defaultEngine = ch.pipeline().get(SslHandler.class).engine();

        settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.supported_protocols", "TLSv1.2")
            .build();
        sslService = new SSLService(TestEnvironment.newEnvironment(settings));
        transport = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            Tracer.NOOP,
            new TLSConfig(sslService.getHttpTransportSSLConfiguration(), sslService::createSSLEngine),
            null,
            randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
        );
        handler = transport.configureServerChannelHandler();
        ch = new EmbeddedChannel(handler);
        SSLEngine customEngine = ch.pipeline().get(SslHandler.class).engine();
        assertThat(customEngine.getEnabledProtocols(), arrayContaining("TLSv1.2"));
        assertThat(customEngine.getEnabledProtocols(), not(equalTo(defaultEngine.getEnabledProtocols())));
    }

    public void testNoExceptionWhenConfiguredWithoutSslKeySSLDisabled() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", false)
            .put("xpack.security.http.ssl.key", testnodeKey)
            .put("xpack.security.http.ssl.certificate", testnodeCert)
            .setSecureSettings(secureSettings)
            .put("path.home", createTempDir())
            .build();
        env = TestEnvironment.newEnvironment(settings);
        sslService = new SSLService(env);
        Netty4HttpServerTransport transport = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            Tracer.NOOP,
            new TLSConfig(sslService.getHttpTransportSSLConfiguration(), sslService::createSSLEngine),
            null,
            randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
        );
        assertNotNull(transport.configureServerChannelHandler());
    }

    public void testAuthnContextWrapping() throws Exception {
        final Settings settings = Settings.builder().put(env.settings()).build();
        final AtomicReference<HttpRequest> dispatchedHttpRequestReference = new AtomicReference<>();
        final String header = "TEST-" + randomAlphaOfLength(8);
        final String headerValue = "TEST-" + randomAlphaOfLength(8);
        final String transientHeader = "TEST-" + randomAlphaOfLength(8);
        final String transientHeaderValue = "TEST-" + randomAlphaOfLength(8);
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                request.getHttpRequest().release();
                // STEP 2: store the dispatched request, which should be wrapping the context
                dispatchedHttpRequestReference.set(request.getHttpRequest());
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error(() -> "--> Unexpected bad request [" + FakeRestRequest.requestToString(channel.request()) + "]", cause);
                throw new AssertionError("Unexpected bad request");
            }

        };
        final ThreadPool testThreadPool = new TestThreadPool(TEST_MOCK_TRANSPORT_THREAD_PREFIX);
        try (
            Netty4HttpServerTransport transport = Security.getHttpServerTransportWithHeadersValidator(
                settings,
                new NetworkService(List.of()),
                testThreadPool,
                xContentRegistry(),
                dispatcher,
                randomClusterSettings(),
                new SharedGroupFactory(settings),
                Tracer.NOOP,
                TLSConfig.noTLS(),
                null,
                (httpPreRequest, channel, listener) -> {
                    // STEP 1: amend the thread context during authentication
                    testThreadPool.getThreadContext().putHeader(header, headerValue);
                    testThreadPool.getThreadContext().putTransient(transientHeader, transientHeaderValue);
                    listener.onResponse(null);
                }
            )
        ) {
            final ChannelHandler handler = transport.configureServerChannelHandler();
            final EmbeddedChannel ch = new EmbeddedChannel(handler);
            // remove these pipeline handlers as they interfere in the test scenario
            for (String pipelineHandlerName : ch.pipeline().names()) {
                if (pipelineHandlerName.equals("decoder")
                    || pipelineHandlerName.equals("encoder")
                    || pipelineHandlerName.equals("encoder_compress")
                    || pipelineHandlerName.equals("chunked_writer")) {
                    ch.pipeline().remove(pipelineHandlerName);
                }
            }
            // STEP 0: send a "wrapped" request
            var writeFuture = testThreadPool.generic().submit(() -> {
                ch.writeInbound(
                    HttpHeadersAuthenticatorUtils.wrapAsMessageWithAuthenticationContext(
                        new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/wrapped_request")
                    )
                );
                ch.writeInbound(new DefaultLastHttpContent());
                ch.flushInbound();
            });
            writeFuture.get();
            // STEP 3: assert the wrapped context
            var storedAuthnContext = HttpHeadersAuthenticatorUtils.extractAuthenticationContext(dispatchedHttpRequestReference.get());
            assertThat(storedAuthnContext, notNullValue());
            try (var ignored = testThreadPool.getThreadContext().stashContext()) {
                assertThat(testThreadPool.getThreadContext().getHeader(header), nullValue());
                assertThat(testThreadPool.getThreadContext().getTransientHeaders().get(transientHeader), nullValue());
                storedAuthnContext.restore();
                assertThat(testThreadPool.getThreadContext().getHeader(header), is(headerValue));
                assertThat(testThreadPool.getThreadContext().getTransientHeaders().get(transientHeader), is(transientHeaderValue));
            }
        } finally {
            testThreadPool.shutdownNow();
        }
    }

    public void testHttpHeaderAuthnBypassHeaderValidator() throws Exception {
        final Settings settings = Settings.builder().put(env.settings()).build();
        final ThreadPool testThreadPool = new TestThreadPool(TEST_MOCK_TRANSPORT_THREAD_PREFIX);
        try (
            Netty4HttpServerTransport transport = Security.getHttpServerTransportWithHeadersValidator(
                settings,
                new NetworkService(List.of()),
                testThreadPool,
                xContentRegistry(),
                new NullDispatcher(),
                randomClusterSettings(),
                new SharedGroupFactory(settings),
                Tracer.NOOP,
                TLSConfig.noTLS(),
                null,
                (httpPreRequest, channel, listener) -> listener.onResponse(null)
            )
        ) {
            final ChannelHandler handler = transport.configureServerChannelHandler();
            final EmbeddedChannel ch = new EmbeddedChannel(handler);
            for (String pipelineHandlerName : ch.pipeline().names()) {
                // remove the decoder AND the header_validator
                if (pipelineHandlerName.equals("decoder") || pipelineHandlerName.equals("header_validator")
                // remove these pipeline handlers as they interfere in the test scenario
                    || pipelineHandlerName.equals("encoder")
                    || pipelineHandlerName.equals("encoder_compress")
                    || pipelineHandlerName.equals("chunked_writer")) {
                    ch.pipeline().remove(pipelineHandlerName);
                }
            }
            // this tests a request that cannot be authenticated, but somehow passed authentication
            // this is the case of an erroneous internal state
            var writeFuture = testThreadPool.generic().submit(() -> {
                ch.writeInbound(new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, "/unauthenticable_request"));
                ch.flushInbound();
            });
            writeFuture.get();
            ch.flushOutbound();
            Netty4FullHttpResponse response = ch.readOutbound();
            assertThat(response.status(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR));
            String responseContentString = new String(ByteBufUtil.getBytes(response.content()), StandardCharsets.UTF_8);
            assertThat(
                responseContentString,
                containsString("\"type\":\"security_exception\",\"reason\":\"Request is not authenticated\"")
            );
            // this tests a request that CAN be authenticated, but that, somehow, has not been
            writeFuture = testThreadPool.generic().submit(() -> {
                ch.writeInbound(
                    HttpHeadersAuthenticatorUtils.wrapAsMessageWithAuthenticationContext(
                        new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/_request")
                    )
                );
                ch.writeInbound(new DefaultLastHttpContent());
                ch.flushInbound();
            });
            writeFuture.get();
            ch.flushOutbound();
            response = ch.readOutbound();
            assertThat(response.status(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR));
            responseContentString = new String(ByteBufUtil.getBytes(response.content()), StandardCharsets.UTF_8);
            assertThat(
                responseContentString,
                containsString("\"type\":\"security_exception\",\"reason\":\"Request is not authenticated\"")
            );
            // this tests the case where authentication passed and the request is to be dispatched, BUT that the authentication context
            // cannot be instated before dispatching the request
            writeFuture = testThreadPool.generic().submit(() -> {
                HttpMessage authenticableMessage = HttpHeadersAuthenticatorUtils.wrapAsMessageWithAuthenticationContext(
                    new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/unauthenticated_request")
                );
                ((HttpHeadersWithAuthenticationContext) authenticableMessage.headers()).setAuthenticationContext(() -> {
                    throw new ElasticsearchException("Boom");
                });
                ch.writeInbound(authenticableMessage);
                ch.writeInbound(new DefaultLastHttpContent());
                ch.flushInbound();
            });
            writeFuture.get();
            ch.flushOutbound();
            response = ch.readOutbound();
            assertThat(response.status(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR));
            responseContentString = new String(ByteBufUtil.getBytes(response.content()), StandardCharsets.UTF_8);
            assertThat(responseContentString, containsString("\"type\":\"exception\",\"reason\":\"Boom\""));
        } finally {
            testThreadPool.shutdownNow();
        }
    }

    public void testHttpHeaderAuthnBypassDecoder() throws Exception {
        final Settings settings = Settings.builder().put(env.settings()).build();
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected good request dispatch [" + FakeRestRequest.requestToString(channel.request()) + "]");
                throw new AssertionError("Unexpected good request dispatch");
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                assertThat(cause, instanceOf(HttpHeadersValidationException.class));
                try {
                    channel.sendResponse(new RestResponse(channel, (Exception) ((ElasticsearchWrapperException) cause).getCause()));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        };
        final ThreadPool testThreadPool = new TestThreadPool(TEST_MOCK_TRANSPORT_THREAD_PREFIX);
        try (
            Netty4HttpServerTransport transport = Security.getHttpServerTransportWithHeadersValidator(
                settings,
                new NetworkService(List.of()),
                testThreadPool,
                xContentRegistry(),
                dispatcher,
                randomClusterSettings(),
                new SharedGroupFactory(settings),
                Tracer.NOOP,
                TLSConfig.noTLS(),
                null,
                (httpPreRequest, channel, listener) -> listener.onResponse(null)
            )
        ) {
            final ChannelHandler handler = transport.configureServerChannelHandler();
            final EmbeddedChannel ch = new EmbeddedChannel(handler);
            // replace the decoder with the vanilla one that does no wrapping and will trip the header validator
            ch.pipeline().replace("decoder", "decoder", new HttpRequestDecoder());
            // remove these pipeline handlers as they interfere in the test scenario
            for (String pipelineHandlerName : ch.pipeline().names()) {
                if (pipelineHandlerName.equals("encoder")
                    || pipelineHandlerName.equals("encoder_compress")
                    || pipelineHandlerName.equals("chunked_writer")) {
                    ch.pipeline().remove(pipelineHandlerName);
                }
            }
            // tests requests that are not wrapped by the "decoder" and so cannot be authenticated
            testThreadPool.generic().submit(() -> {
                ch.writeInbound(new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, "/unwrapped_full_request"));
                ch.flushInbound();
            }).get();
            ch.flushOutbound();
            Netty4FullHttpResponse response = ch.readOutbound();
            assertThat(response.status(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR));
            var responseContentString = new String(ByteBufUtil.getBytes(response.content()), StandardCharsets.UTF_8);
            assertThat(
                responseContentString,
                containsString("\"type\":\"illegal_state_exception\",\"reason\":\"Cannot authenticate unwrapped requests\"")
            );
            testThreadPool.generic().submit(() -> {
                ch.writeInbound(new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/unwrapped_request"));
                ch.flushInbound();
            }).get();
            ch.flushOutbound();
            response = ch.readOutbound();
            assertThat(response.status(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR));
            responseContentString = new String(ByteBufUtil.getBytes(response.content()), StandardCharsets.UTF_8);
            assertThat(
                responseContentString,
                containsString("\"type\":\"illegal_state_exception\",\"reason\":\"Cannot authenticate unwrapped requests\"")
            );
        } finally {
            testThreadPool.shutdownNow();
        }
    }

    public void testMalformedRequestDispatchedNoAuthn() throws Exception {
        assumeTrue(
            "This test doesn't work correctly under turkish-like locale, because it uses String#toUpper() for asserted error messages",
            isTurkishLocale() == false
        );
        final AtomicReference<Throwable> dispatchThrowableReference = new AtomicReference<>();
        final AtomicInteger authnInvocationCount = new AtomicInteger();
        final AtomicInteger badDispatchInvocationCount = new AtomicInteger();
        final Settings settings = Settings.builder()
            .put(env.settings())
            .put(HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE.getKey(), "32b")
            .put(HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.getKey(), "32b")
            .build();
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected dispatched request [" + FakeRestRequest.requestToString(channel.request()) + "]");
                throw new AssertionError("Unexpected dispatched request");
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                assertThat(cause, notNullValue());
                dispatchThrowableReference.set(cause);
                badDispatchInvocationCount.incrementAndGet();
            }
        };
        final ThreadPool testThreadPool = new TestThreadPool(TEST_MOCK_TRANSPORT_THREAD_PREFIX);
        try (
            Netty4HttpServerTransport transport = Security.getHttpServerTransportWithHeadersValidator(
                settings,
                new NetworkService(List.of()),
                testThreadPool,
                xContentRegistry(),
                dispatcher,
                randomClusterSettings(),
                new SharedGroupFactory(settings),
                Tracer.NOOP,
                TLSConfig.noTLS(),
                null,
                (httpPreRequest, channel, listener) -> {
                    authnInvocationCount.incrementAndGet();
                    throw new AssertionError("Malformed requests shouldn't be authenticated");
                }
            )
        ) {
            final ChannelHandler handler = transport.configureServerChannelHandler();
            assertThat(authnInvocationCount.get(), is(0));
            assertThat(badDispatchInvocationCount.get(), is(0));
            // case 1: invalid initial line
            {
                EmbeddedChannel ch = new EmbeddedChannel(handler);
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of("This is not a valid HTTP line"), buf);
                buf.writeByte(HttpConstants.LF);
                buf.writeByte(HttpConstants.LF);
                var writeFuture = testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                });
                writeFuture.get();
                assertThat(dispatchThrowableReference.get().toString(), containsString("NOT A VALID HTTP LINE"));
                assertThat(badDispatchInvocationCount.get(), is(1));
                assertThat(authnInvocationCount.get(), is(0));
            }
            // case 2: too long initial line
            {
                EmbeddedChannel ch = new EmbeddedChannel(handler);
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of("GET /this/is/a/valid/but/too/long/initial/line HTTP/1.1"), buf);
                buf.writeByte(HttpConstants.LF);
                buf.writeByte(HttpConstants.LF);
                var writeFuture = testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                });
                writeFuture.get();
                assertThat(dispatchThrowableReference.get().toString(), containsString("HTTP line is larger than"));
                assertThat(badDispatchInvocationCount.get(), is(2));
                assertThat(authnInvocationCount.get(), is(0));
            }
            // case 3: invalid header with no colon
            {
                EmbeddedChannel ch = new EmbeddedChannel(handler);
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of("GET /url HTTP/1.1"), buf);
                buf.writeByte(HttpConstants.LF);
                ByteBufUtil.copy(AsciiString.of("Host"), buf);
                buf.writeByte(HttpConstants.LF);
                buf.writeByte(HttpConstants.LF);
                var writeFuture = testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                });
                writeFuture.get();
                assertThat(dispatchThrowableReference.get().toString(), containsString("No colon found"));
                assertThat(badDispatchInvocationCount.get(), is(3));
                assertThat(authnInvocationCount.get(), is(0));
            }
            // case 4: invalid header longer than max allowed
            {
                EmbeddedChannel ch = new EmbeddedChannel(handler);
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of("GET /url HTTP/1.1"), buf);
                buf.writeByte(HttpConstants.LF);
                ByteBufUtil.copy(AsciiString.of("Host: this.looks.like.a.good.url.but.is.longer.than.permitted"), buf);
                buf.writeByte(HttpConstants.LF);
                buf.writeByte(HttpConstants.LF);
                var writeFuture = testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                });
                writeFuture.get();
                assertThat(dispatchThrowableReference.get().toString(), containsString("HTTP header is larger than"));
                assertThat(badDispatchInvocationCount.get(), is(4));
                assertThat(authnInvocationCount.get(), is(0));
            }
            // case 5: invalid header format
            {
                EmbeddedChannel ch = new EmbeddedChannel(handler);
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of("GET /url HTTP/1.1"), buf);
                buf.writeByte(HttpConstants.LF);
                ByteBufUtil.copy(AsciiString.of("Host: invalid header value"), buf);
                buf.writeByte(0x01);
                buf.writeByte(HttpConstants.LF);
                buf.writeByte(HttpConstants.LF);
                var writeFuture = testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                });
                writeFuture.get();
                assertThat(dispatchThrowableReference.get().toString(), containsString("Validation failed for header 'Host'"));
                assertThat(badDispatchInvocationCount.get(), is(5));
                assertThat(authnInvocationCount.get(), is(0));
            }
            // case 6: connection closed before all headers are sent
            {
                EmbeddedChannel ch = new EmbeddedChannel(handler);
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of("GET /url HTTP/1.1"), buf);
                buf.writeByte(HttpConstants.LF);
                ByteBufUtil.copy(AsciiString.of("Host: localhost"), buf);
                buf.writeByte(HttpConstants.LF);
                testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                }).get();
                testThreadPool.generic().submit(() -> ch.close().get()).get();
                assertThat(authnInvocationCount.get(), is(0));
            }
        } finally {
            testThreadPool.shutdownNow();
        }
    }

    public void testOptionsRequestsFailWith400AndNoAuthn() throws Exception {
        final Settings settings = Settings.builder().put(env.settings()).build();
        AtomicReference<Throwable> badRequestCauseReference = new AtomicReference<>();
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected dispatched request [" + FakeRestRequest.requestToString(channel.request()) + "]");
                throw new AssertionError("Unexpected dispatched request");
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                badRequestCauseReference.set(cause);
            }
        };
        final ThreadPool testThreadPool = new TestThreadPool(TEST_MOCK_TRANSPORT_THREAD_PREFIX);
        try (
            Netty4HttpServerTransport transport = Security.getHttpServerTransportWithHeadersValidator(
                settings,
                new NetworkService(List.of()),
                testThreadPool,
                xContentRegistry(),
                dispatcher,
                randomClusterSettings(),
                new SharedGroupFactory(settings),
                Tracer.NOOP,
                TLSConfig.noTLS(),
                null,
                (httpPreRequest, channel, listener) -> {
                    throw new AssertionError("should not be invoked for OPTIONS requests");
                },
                (httpPreRequest, channel, listener) -> {
                    throw new AssertionError("should not be invoked for OPTIONS requests with a body");
                }
            )
        ) {
            final ChannelHandler handler = transport.configureServerChannelHandler();
            final EmbeddedChannel ch = new EmbeddedChannel(handler);
            // OPTIONS request with fixed length content written in one chunk
            {
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of("OPTIONS /url/whatever/fixed-length-single-chunk HTTP/1.1"), buf);
                buf.writeByte(HttpConstants.LF);
                if (randomBoolean()) {
                    ByteBufUtil.copy(AsciiString.of("Host: localhost"), buf);
                    buf.writeByte(HttpConstants.LF);
                }
                if (randomBoolean()) {
                    ByteBufUtil.copy(AsciiString.of("Accept: */*"), buf);
                    buf.writeByte(HttpConstants.LF);
                }
                if (randomBoolean()) {
                    ByteBufUtil.copy(AsciiString.of("Content-Encoding: gzip"), buf);
                    buf.writeByte(HttpConstants.LF);
                }
                if (randomBoolean()) {
                    ByteBufUtil.copy(
                        AsciiString.of("Content-Type: " + randomFrom("text/plain; charset=utf-8", "application/json; charset=utf-8")),
                        buf
                    );
                    buf.writeByte(HttpConstants.LF);
                }
                String content = randomAlphaOfLengthBetween(4, 1024);
                // having a "Content-Length" request header is what makes it "fixed length"
                ByteBufUtil.copy(AsciiString.of("Content-Length: " + content.length()), buf);
                buf.writeByte(HttpConstants.LF);
                // end of headers
                buf.writeByte(HttpConstants.LF);
                ByteBufUtil.copy(AsciiString.of(content), buf);
                // write everything in one single chunk
                testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                }).get();
                ch.runPendingTasks();
                Throwable badRequestCause = badRequestCauseReference.get();
                assertThat(badRequestCause, instanceOf(HttpHeadersValidationException.class));
                assertThat(badRequestCause.getCause(), instanceOf(ElasticsearchException.class));
                assertThat(((ElasticsearchException) badRequestCause.getCause()).status(), is(RestStatus.BAD_REQUEST));
                assertThat(
                    ((ElasticsearchException) badRequestCause.getCause()).getDetailedMessage(),
                    containsString("OPTIONS requests with a payload body are not supported")
                );
            }
            {
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of("OPTIONS /url/whatever/chunked-transfer?encoding HTTP/1.1"), buf);
                buf.writeByte(HttpConstants.LF);
                if (randomBoolean()) {
                    ByteBufUtil.copy(AsciiString.of("Host: localhost"), buf);
                    buf.writeByte(HttpConstants.LF);
                }
                if (randomBoolean()) {
                    ByteBufUtil.copy(AsciiString.of("Accept: */*"), buf);
                    buf.writeByte(HttpConstants.LF);
                }
                if (randomBoolean()) {
                    ByteBufUtil.copy(AsciiString.of("Content-Encoding: gzip"), buf);
                    buf.writeByte(HttpConstants.LF);
                }
                if (randomBoolean()) {
                    ByteBufUtil.copy(
                        AsciiString.of("Content-Type: " + randomFrom("text/plain; charset=utf-8", "application/json; charset=utf-8")),
                        buf
                    );
                    buf.writeByte(HttpConstants.LF);
                }
                // do not write a "Content-Length" header to make the request "variable length"
                if (randomBoolean()) {
                    ByteBufUtil.copy(AsciiString.of("Transfer-Encoding: " + randomFrom("chunked", "gzip, chunked")), buf);
                } else {
                    ByteBufUtil.copy(AsciiString.of("Transfer-Encoding: chunked"), buf);
                }
                buf.writeByte(HttpConstants.LF);
                buf.writeByte(HttpConstants.LF);
                // maybe append some chunks as well
                String[] contentParts = randomArray(0, 4, String[]::new, () -> randomAlphaOfLengthBetween(1, 64));
                for (String content : contentParts) {
                    ByteBufUtil.copy(AsciiString.of(Integer.toHexString(content.length())), buf);
                    buf.writeByte(HttpConstants.CR);
                    buf.writeByte(HttpConstants.LF);
                    ByteBufUtil.copy(AsciiString.of(content), buf);
                    buf.writeByte(HttpConstants.CR);
                    buf.writeByte(HttpConstants.LF);
                }
                testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                }).get();
                // append some more chunks as well
                ByteBuf buf2 = ch.alloc().buffer();
                contentParts = randomArray(1, 4, String[]::new, () -> randomAlphaOfLengthBetween(1, 64));
                for (String content : contentParts) {
                    ByteBufUtil.copy(AsciiString.of(Integer.toHexString(content.length())), buf2);
                    buf2.writeByte(HttpConstants.CR);
                    buf2.writeByte(HttpConstants.LF);
                    ByteBufUtil.copy(AsciiString.of(content), buf2);
                    buf2.writeByte(HttpConstants.CR);
                    buf2.writeByte(HttpConstants.LF);
                }
                // finish chunked request
                ByteBufUtil.copy(AsciiString.of("0"), buf2);
                buf2.writeByte(HttpConstants.CR);
                buf2.writeByte(HttpConstants.LF);
                buf2.writeByte(HttpConstants.CR);
                buf2.writeByte(HttpConstants.LF);
                testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf2);
                    ch.flushInbound();
                }).get();
                ch.runPendingTasks();
                Throwable badRequestCause = badRequestCauseReference.get();
                assertThat(badRequestCause, instanceOf(HttpHeadersValidationException.class));
                assertThat(badRequestCause.getCause(), instanceOf(ElasticsearchException.class));
                assertThat(((ElasticsearchException) badRequestCause.getCause()).status(), is(RestStatus.BAD_REQUEST));
                assertThat(
                    ((ElasticsearchException) badRequestCause.getCause()).getDetailedMessage(),
                    containsString("OPTIONS requests with a payload body are not supported")
                );
            }
        } finally {
            testThreadPool.shutdownNow();
        }
    }

}
