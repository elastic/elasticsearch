/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import jdk.internal.net.http.HttpClientFacade;
import sun.net.www.protocol.ftp.FtpURLConnection;
import sun.net.www.protocol.https.AbstractDelegateHttpsURLConnection;
import sun.net.www.protocol.https.HttpsURLConnectionImpl;
import sun.net.www.protocol.mailto.MailToURLConnection;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.rules.TypeToken;
import org.elasticsearch.entitlement.rules.function.CheckMethod;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.net.Authenticator;
import java.net.ContentHandlerFactory;
import java.net.CookieHandler;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.DatagramSocketImplFactory;
import java.net.FileNameMap;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.ProtocolFamily;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.ResponseCache;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketImplFactory;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.spi.InetAddressResolverProvider;
import java.net.spi.URLStreamHandlerProvider;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.AsynchronousChannelProvider;
import java.nio.channels.spi.SelectorProvider;

public class NetworkInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        builder.on(ProxySelector.class, rule -> {
            rule.callingVoidStatic(ProxySelector::setDefault, ProxySelector.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
        });

        builder.on(ResponseCache.class, rule -> {
            rule.callingVoidStatic(ResponseCache::setDefault, ResponseCache.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
        });

        builder.on(Authenticator.class, rule -> {
            rule.callingVoidStatic(Authenticator::setDefault, Authenticator.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
        });

        builder.on(CookieHandler.class, rule -> {
            rule.callingVoidStatic(CookieHandler::setDefault, CookieHandler.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
        });

        builder.on(URL.class, rule -> {
            rule.callingStatic(URL::new, String.class, String.class, Integer.class, String.class, URLStreamHandler.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
            rule.callingStatic(URL::new, URL.class, String.class, URLStreamHandler.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
            rule.callingVoidStatic(URL::setURLStreamHandlerFactory, URLStreamHandlerFactory.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
            rule.calling(URL::openConnection).enforce(Policies::entitlementForUrl).elseThrowNotEntitled();
            rule.calling(URL::openConnection, Proxy.class).enforce((url, proxy) -> {
                if (proxy.type() != Proxy.Type.DIRECT) {
                    return Policies.outboundNetworkAccess().and(Policies.entitlementForUrl(url));
                }
                return Policies.entitlementForUrl(url);
            }).elseThrowNotEntitled();
            rule.calling(URL::openStream).enforce(Policies::entitlementForUrl).elseThrowNotEntitled();
            rule.calling(URL::getContent).enforce(Policies::entitlementForUrl).elseThrowNotEntitled();
            rule.calling(URL::getContent, Class[].class).enforce(Policies::entitlementForUrl).elseThrowNotEntitled();
        });

        builder.on(URLConnection.class, rule -> {
            rule.callingVoid(URLConnection::connect).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoidStatic(URLConnection::setContentHandlerFactory, ContentHandlerFactory.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
            rule.callingVoidStatic(URLConnection::setFileNameMap, FileNameMap.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
            rule.calling(URLConnection::getContentLength).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
            rule.calling(URLConnection::getContentLengthLong).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
            rule.calling(URLConnection::getContentType).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
            rule.calling(URLConnection::getContentEncoding).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
            rule.calling(URLConnection::getExpiration).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
            rule.calling(URLConnection::getDate).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
            rule.calling(URLConnection::getLastModified).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
            rule.calling(URLConnection::getHeaderFieldInt, String.class, Integer.class)
                .enforce(Policies::entitlementForUrlConnection)
                .elseThrowNotEntitled();
            rule.calling(URLConnection::getHeaderFieldLong, String.class, Long.class)
                .enforce(Policies::entitlementForUrlConnection)
                .elseThrowNotEntitled();
            rule.calling(URLConnection::getHeaderFieldDate, String.class, Long.class)
                .enforce(Policies::entitlementForUrlConnection)
                .elseThrowNotEntitled();
            rule.calling(URLConnection::getContent).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
            rule.calling(URLConnection::getContent, Class[].class).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
        });

        builder.on(HttpURLConnection.class, rule -> {
            rule.callingVoidStatic(HttpURLConnection::setFollowRedirects, Boolean.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
            rule.calling(HttpURLConnection::getResponseCode).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpURLConnection::getResponseMessage).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpURLConnection::getHeaderFieldDate, String.class, Long.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
        });

        builder.on(Socket.class, rule -> {
            rule.callingStatic(Socket::new).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(Socket::new, Proxy.class).enforce((proxy) -> {
                if (proxy.type() == Proxy.Type.HTTP || proxy.type() == Proxy.Type.SOCKS) {
                    return Policies.outboundNetworkAccess();
                } else {
                    return Policies.empty();
                }
            }).elseThrowNotEntitled();
            rule.callingStatic(Socket::new, String.class, Integer.class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(Socket::new, String.class, Integer.class, InetAddress.class, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(Socket::new, InetAddress.class, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(Socket::new, InetAddress.class, Integer.class, InetAddress.class, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(Socket::new, String.class, Integer.class, Boolean.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(Socket::new, InetAddress.class, Integer.class, Boolean.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingVoidStatic(Socket::setSocketImplFactory, SocketImplFactory.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
            rule.callingVoid(Socket::bind, SocketAddress.class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(Socket::connect, SocketAddress.class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(Socket::connect, SocketAddress.class, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
        });

        builder.on(ServerSocket.class, rule -> {
            rule.callingStatic(ServerSocket::new).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(ServerSocket::new, Integer.class).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(ServerSocket::new, Integer.class, Integer.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(ServerSocket::new, Integer.class, Integer.class, InetAddress.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingVoidStatic(ServerSocket::setSocketFactory, SocketImplFactory.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
            rule.callingVoid(ServerSocket::bind, SocketAddress.class).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(ServerSocket::bind, SocketAddress.class, Integer.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(ServerSocket::accept).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on(URLClassLoader.class, rule -> {
            rule.callingStatic(URLClassLoader::newInstance, URL[].class).enforce(Policies::createClassLoader).elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::newInstance, URL[].class, ClassLoader.class)
                .enforce(Policies::createClassLoader)
                .elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::new, URL[].class).enforce(Policies::createClassLoader).elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::new, URL[].class, ClassLoader.class)
                .enforce(Policies::createClassLoader)
                .elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::new, String.class, URL[].class, ClassLoader.class)
                .enforce(Policies::createClassLoader)
                .elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::new, String.class, URL[].class, ClassLoader.class, URLStreamHandlerFactory.class)
                .enforce(Policies::createClassLoader)
                .elseThrowNotEntitled();
        });

        builder.on(ServerSocket.class, rule -> {
            rule.callingStatic(ServerSocket::new).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(ServerSocket::new, Integer.class).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(ServerSocket::new, Integer.class, Integer.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(ServerSocket::new, Integer.class, Integer.class, InetAddress.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
        });

        builder.on(Socket.class, rule -> {
            rule.callingStatic(Socket::new).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(Socket::new, Proxy.class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(Socket::new, String.class, Integer.class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(Socket::new, InetAddress.class, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(Socket::new, String.class, Integer.class, InetAddress.class, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(Socket::new, InetAddress.class, Integer.class, InetAddress.class, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(Socket::new, String.class, Integer.class, Boolean.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(Socket::new, InetAddress.class, Integer.class, Boolean.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
        });

        builder.on(DatagramSocket.class, rule -> {
            rule.callingStatic(DatagramSocket::new).enforce(Policies::allNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(DatagramSocket::new, Integer.class).enforce(Policies::allNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(DatagramSocket::new, Integer.class, InetAddress.class)
                .enforce(Policies::allNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingStatic(DatagramSocket::new, SocketAddress.class).enforce(Policies::allNetworkAccess).elseThrowNotEntitled();
            rule.callingVoidStatic(DatagramSocket::setDatagramSocketImplFactory, DatagramSocketImplFactory.class)
                .enforce(Policies::changeNetworkHandling)
                .elseThrowNotEntitled();
            rule.callingVoid(DatagramSocket::bind, SocketAddress.class).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(DatagramSocket::connect, InetAddress.class, Integer.class)
                .enforce(Policies::allNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingVoid(DatagramSocket::connect, SocketAddress.class).enforce(Policies::allNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(DatagramSocket::receive, DatagramPacket.class).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(DatagramSocket::send, DatagramPacket.class)
                .enforce(
                    (socket, packet) -> packet.getAddress().isMulticastAddress()
                        ? Policies.allNetworkAccess()
                        : Policies.outboundNetworkAccess()
                )
                .elseThrowNotEntitled();
            rule.callingVoid(DatagramSocket::joinGroup, SocketAddress.class, NetworkInterface.class)
                .enforce(Policies::allNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingVoid(DatagramSocket::leaveGroup, SocketAddress.class, NetworkInterface.class)
                .enforce(Policies::allNetworkAccess)
                .elseThrowNotEntitled();
        });

        builder.on(MulticastSocket.class, rule -> {
            rule.callingVoid(MulticastSocket::joinGroup, InetAddress.class).enforce(Policies::allNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(MulticastSocket::joinGroup, SocketAddress.class, NetworkInterface.class)
                .enforce(Policies::allNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingVoid(MulticastSocket::leaveGroup, InetAddress.class).enforce(Policies::allNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(MulticastSocket::leaveGroup, SocketAddress.class, NetworkInterface.class)
                .enforce(Policies::allNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingVoid(MulticastSocket::send, DatagramPacket.class, Byte.class)
                .enforce(Policies::allNetworkAccess)
                .elseThrowNotEntitled();
        });

        builder.on(SocketChannel.class, rule -> {
            rule.callingStatic(SocketChannel::open, SocketAddress.class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(SocketChannel::open, ProtocolFamily.class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingStatic(SocketChannel::open).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(SocketChannel::bind, SocketAddress.class).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on("sun.nio.ch.SocketChannelImpl", SocketChannel.class, rule -> {
            rule.callingVoid(SocketChannel::bind, SocketAddress.class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(SocketChannel::connect, SocketAddress.class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on(ServerSocketChannel.class, rule -> {
            rule.callingVoid(ServerSocketChannel::bind, SocketAddress.class).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on("sun.nio.ch.ServerSocketChannelImpl", ServerSocketChannel.class, rule -> {
            rule.callingVoid(ServerSocketChannel::bind, SocketAddress.class, Integer.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(ServerSocketChannel::accept).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on(DatagramChannel.class, rule -> {
            rule.callingVoid(DatagramChannel::bind, SocketAddress.class).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on("sun.nio.ch.DatagramChannelImpl", DatagramChannel.class, rule -> {
            rule.callingVoid(DatagramChannel::bind, SocketAddress.class).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(DatagramChannel::connect, SocketAddress.class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(DatagramChannel::send, ByteBuffer.class, SocketAddress.class).enforce((_, _, target) -> {
                if (target instanceof InetSocketAddress isa && isa.getAddress().isMulticastAddress()) {
                    return Policies.allNetworkAccess();
                } else {
                    return Policies.outboundNetworkAccess();
                }
            }).elseThrowNotEntitled();
            rule.callingVoid(DatagramChannel::receive, ByteBuffer.class).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on(AsynchronousServerSocketChannel.class, rule -> {
            rule.callingVoid(AsynchronousServerSocketChannel::bind, SocketAddress.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
        });

        builder.on("sun.nio.ch.AsynchronousSocketChannelImpl", AsynchronousSocketChannel.class, rule -> {
            rule.callingVoid(AsynchronousSocketChannel::connect, SocketAddress.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.callingVoid(
                AsynchronousSocketChannel::connect,
                TypeToken.of(SocketAddress.class),
                TypeToken.of(Object.class),
                new TypeToken<CompletionHandler<Void, Object>>() {}
            ).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(AsynchronousSocketChannel::bind, SocketAddress.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
        });

        builder.on("sun.nio.ch.AsynchronousServerSocketChannelImpl", AsynchronousServerSocketChannel.class, rule -> {
            rule.callingVoid(AsynchronousServerSocketChannel::bind, SocketAddress.class, Integer.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(AsynchronousServerSocketChannel::accept).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
            rule.callingVoid(
                AsynchronousServerSocketChannel::accept,
                TypeToken.of(Object.class),
                new TypeToken<CompletionHandler<AsynchronousSocketChannel, Object>>() {}
            ).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on(AsynchronousSocketChannel.class, rule -> {
            rule.callingVoid(AsynchronousSocketChannel::bind, SocketAddress.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
        });

        builder.on(
            SelectorProvider.class,
            rule -> { rule.protectedCtor().enforce(Policies::changeNetworkHandling).elseThrowNotEntitled(); }
        );

        builder.on(InetAddressResolverProvider.class, rule -> {
            rule.protectedCtor().enforce(Policies::changeNetworkHandling).elseThrowNotEntitled();
        });

        builder.on(
            URLStreamHandlerProvider.class,
            rule -> { rule.protectedCtor().enforce(Policies::changeNetworkHandling).elseThrowNotEntitled(); }
        );

        builder.on(SelectableChannel.class, rule -> {
            rule.calling(SelectableChannel::register, Selector.class, Integer.class)
                .enforce(() -> Policies.outboundNetworkAccess().and(Policies.inboundNetworkAccess()))
                .elseThrowNotEntitled();
        });

        builder.on(AsynchronousChannelProvider.class, rule -> {
            rule.protectedCtor().enforce(Policies::changeNetworkHandling).elseThrowNotEntitled();
        });

        builder.on(sun.net.www.URLConnection.class, rule -> {
            rule.calling(sun.net.www.URLConnection::getHeaderField, String.class)
                .enforce(Policies::entitlementForUrlConnection)
                .elseThrowNotEntitled();
            rule.calling(sun.net.www.URLConnection::getHeaderField, Integer.class)
                .enforce(Policies::entitlementForUrlConnection)
                .elseThrowNotEntitled();
            rule.calling(sun.net.www.URLConnection::getHeaderFields).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
            rule.calling(sun.net.www.URLConnection::getHeaderFieldKey, Integer.class)
                .enforce(Policies::entitlementForUrlConnection)
                .elseThrowNotEntitled();
            rule.calling(sun.net.www.URLConnection::getContentType).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
            rule.calling(sun.net.www.URLConnection::getContentLength).enforce(Policies::entitlementForUrlConnection).elseThrowNotEntitled();
        });

        builder.on(FtpURLConnection.class, rule -> {
            rule.callingVoid(FtpURLConnection::connect).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(FtpURLConnection::getInputStream).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(FtpURLConnection::getOutputStream).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on(sun.net.www.protocol.http.HttpURLConnection.class, rule -> {
            // This method seems to have been removed in later JDKs
            // .callingStatic(sun.net.www.protocol.http.HttpURLConnection::openConnectionCheckRedirects, URLConnection.class)
            // .enforce(Policies::outboundNetworkAccess)
            // .elseThrowNotEntitled()
            rule.callingVoid(sun.net.www.protocol.http.HttpURLConnection::connect)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(sun.net.www.protocol.http.HttpURLConnection::getInputStream)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(sun.net.www.protocol.http.HttpURLConnection::getOutputStream)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(sun.net.www.protocol.http.HttpURLConnection::getErrorStream)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(sun.net.www.protocol.http.HttpURLConnection::getHeaderField, String.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(sun.net.www.protocol.http.HttpURLConnection::getHeaderFields)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(sun.net.www.protocol.http.HttpURLConnection::getHeaderField, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(sun.net.www.protocol.http.HttpURLConnection::getHeaderFieldKey, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
        });

        builder.on(HttpsURLConnectionImpl.class, rule -> {
            rule.callingVoid(HttpsURLConnectionImpl::connect).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getInputStream).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getErrorStream).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getOutputStream).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getHeaderField, String.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getHeaderField, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getHeaderFieldKey, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getHeaderFields).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getResponseCode).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getResponseMessage).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getContentLength).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getContentLengthLong).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getContentType).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getContentEncoding).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getDate).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getExpiration).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getLastModified).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getHeaderFieldDate, String.class, Long.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getHeaderFieldInt, String.class, Integer.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getHeaderFieldLong, String.class, Long.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getContent).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(HttpsURLConnectionImpl::getContent, Class[].class).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on(AbstractDelegateHttpsURLConnection.class, rule -> {
            rule.callingVoid(AbstractDelegateHttpsURLConnection::connect).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on(MailToURLConnection.class, rule -> {
            rule.callingVoid(MailToURLConnection::connect).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(MailToURLConnection::getOutputStream).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on("jdk.internal.net.http.HttpClientImpl", HttpClient.class, rule -> {
            rule.calling(HttpClient::send, TypeToken.of(HttpRequest.class), new TypeToken<HttpResponse.BodyHandler<?>>() {})
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(HttpClient::sendAsync, TypeToken.of(HttpRequest.class), new TypeToken<HttpResponse.BodyHandler<?>>() {})
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(
                HttpClient::sendAsync,
                TypeToken.of(HttpRequest.class),
                new TypeToken<HttpResponse.BodyHandler<Void>>() {},
                new TypeToken<HttpResponse.PushPromiseHandler<Void>>() {}
            ).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on(HttpClientFacade.class, rule -> {
            rule.calling(HttpClientFacade::send, TypeToken.of(HttpRequest.class), new TypeToken<HttpResponse.BodyHandler<?>>() {})
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(HttpClientFacade::sendAsync, TypeToken.of(HttpRequest.class), new TypeToken<HttpResponse.BodyHandler<?>>() {})
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(
                HttpClientFacade::sendAsync,
                TypeToken.of(HttpRequest.class),
                new TypeToken<HttpResponse.BodyHandler<Void>>() {},
                new TypeToken<HttpResponse.PushPromiseHandler<Void>>() {}
            ).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
        });

        builder.on(AbstractSelectableChannel.class, rule -> {
            rule.calling(AbstractSelectableChannel::register, Selector.class, Integer.class, Object.class).enforce((_, _, ops) -> {
                CheckMethod check = Policies.empty();
                if ((ops & SelectionKey.OP_CONNECT) != 0) {
                    check = check.and(Policies.outboundNetworkAccess());
                }
                if ((ops & SelectionKey.OP_ACCEPT) != 0) {
                    check = check.and(Policies.inboundNetworkAccess());
                }

                return check;
            }).elseThrowNotEntitled();
            rule.calling(AbstractSelectableChannel::register, Selector.class, Integer.class).enforce((_, _, ops) -> {
                CheckMethod check = Policies.empty();
                if ((ops & SelectionKey.OP_CONNECT) != 0) {
                    check = check.and(Policies.outboundNetworkAccess());
                }
                if ((ops & SelectionKey.OP_ACCEPT) != 0) {
                    check = check.and(Policies.inboundNetworkAccess());
                }

                return check;
            }).elseThrowNotEntitled();
        });

    }
}
