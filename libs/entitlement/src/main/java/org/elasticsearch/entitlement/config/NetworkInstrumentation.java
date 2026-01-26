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

import org.elasticsearch.entitlement.rules.EntitlementRules;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.rules.TypeToken;
import org.elasticsearch.entitlement.rules.function.CheckMethod;

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
    public void init() {
        EntitlementRules.on(ProxySelector.class)
            .callingStatic(ProxySelector::setDefault, ProxySelector.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled();

        EntitlementRules.on(ResponseCache.class)
            .callingStatic(ResponseCache::setDefault, ResponseCache.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled();

        EntitlementRules.on(Authenticator.class)
            .callingStatic(Authenticator::setDefault, Authenticator.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled();

        EntitlementRules.on(CookieHandler.class)
            .callingStatic(CookieHandler::setDefault, CookieHandler.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled();

        EntitlementRules.on(URL.class)
            .callingStatic(URL::new, String.class, String.class, Integer.class, String.class, URLStreamHandler.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled()
            .callingStatic(URL::new, URL.class, String.class, URLStreamHandler.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled()
            .callingStatic(URL::setURLStreamHandlerFactory, URLStreamHandlerFactory.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled()
            .calling(URL::openConnection)
            .enforce(Policies::entitlementForUrl)
            .elseThrowNotEntitled()
            .calling(URL::openConnection, Proxy.class)
            .enforce((url, proxy) -> {
                if (proxy.type() != Proxy.Type.DIRECT) {
                    return Policies.outboundNetworkAccess().and(Policies.entitlementForUrl(url));
                }
                return Policies.entitlementForUrl(url);
            })
            .elseThrowNotEntitled()
            .calling(URL::openStream)
            .enforce(Policies::entitlementForUrl)
            .elseThrowNotEntitled()
            .calling(URL::getContent)
            .enforce(Policies::entitlementForUrl)
            .elseThrowNotEntitled()
            .calling(URL::getContent, Class[].class)
            .enforce(Policies::entitlementForUrl)
            .elseThrowNotEntitled();

        EntitlementRules.on(URLConnection.class)
            .calling(URLConnection::connect)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(URLConnection::setContentHandlerFactory, ContentHandlerFactory.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled()
            .callingStatic(URLConnection::setFileNameMap, FileNameMap.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled()
            .calling(URLConnection::getContentLength)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getContentLengthLong)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getContentType)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getContentEncoding)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getExpiration)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getDate)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getLastModified)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getHeaderFieldInt, String.class, Integer.class)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getHeaderFieldLong, String.class, Long.class)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getHeaderFieldDate, String.class, Long.class)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getContent)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(URLConnection::getContent, Class[].class)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled();

        EntitlementRules.on(HttpURLConnection.class)
            .callingStatic(HttpURLConnection::setFollowRedirects, Boolean.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled()
            .calling(HttpURLConnection::getResponseCode)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpURLConnection::getResponseMessage)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpURLConnection::getHeaderFieldDate, String.class, Long.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(Socket.class)
            .callingStatic(Socket::new)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, Proxy.class)
            .enforce((proxy) -> {
                if (proxy.type() == Proxy.Type.HTTP || proxy.type() == Proxy.Type.SOCKS) {
                    return Policies.outboundNetworkAccess();
                } else {
                    return Policies.noop();
                }
            })
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, String.class, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, String.class, Integer.class, InetAddress.class, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, InetAddress.class, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, InetAddress.class, Integer.class, InetAddress.class, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, String.class, Integer.class, Boolean.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, InetAddress.class, Integer.class, Boolean.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::setSocketImplFactory, SocketImplFactory.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled()
            .calling(Socket::bind, SocketAddress.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(Socket::connect, SocketAddress.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(Socket::connect, SocketAddress.class, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(ServerSocket.class)
            .callingStatic(ServerSocket::new)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(ServerSocket::new, Integer.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(ServerSocket::new, Integer.class, Integer.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(ServerSocket::new, Integer.class, Integer.class, InetAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(ServerSocket::setSocketFactory, SocketImplFactory.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled()
            .calling(ServerSocket::bind, SocketAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(ServerSocket::bind, SocketAddress.class, Integer.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(ServerSocket::accept)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(URLClassLoader.class)
            .callingStatic(URLClassLoader::newInstance, URL[].class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::newInstance, URL[].class, ClassLoader.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::new, URL[].class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::new, URL[].class, ClassLoader.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::new, String.class, URL[].class, ClassLoader.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::new, String.class, URL[].class, ClassLoader.class, URLStreamHandlerFactory.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled();

        EntitlementRules.on(ServerSocket.class)
            .callingStatic(ServerSocket::new)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(ServerSocket::new, Integer.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(ServerSocket::new, Integer.class, Integer.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(ServerSocket::new, Integer.class, Integer.class, InetAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(Socket.class)
            .callingStatic(Socket::new)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, Proxy.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, String.class, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, InetAddress.class, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, String.class, Integer.class, InetAddress.class, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, InetAddress.class, Integer.class, InetAddress.class, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, String.class, Integer.class, Boolean.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(Socket::new, InetAddress.class, Integer.class, Boolean.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(DatagramSocket.class)
            .callingStatic(DatagramSocket::new)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(DatagramSocket::new, Integer.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(DatagramSocket::new, Integer.class, InetAddress.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(DatagramSocket::new, SocketAddress.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(DatagramSocket::setDatagramSocketImplFactory, DatagramSocketImplFactory.class)
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled()
            .calling(DatagramSocket::bind, SocketAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(DatagramSocket::connect, InetAddress.class, Integer.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .calling(DatagramSocket::connect, SocketAddress.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .calling(DatagramSocket::receive, DatagramPacket.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(DatagramSocket::send, DatagramPacket.class)
            .enforce(
                (socket, packet) -> packet.getAddress().isMulticastAddress()
                    ? Policies.allNetworkAccess()
                    : Policies.outboundNetworkAccess()
            )
            .elseThrowNotEntitled()
            .calling(DatagramSocket::joinGroup, SocketAddress.class, NetworkInterface.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .calling(DatagramSocket::leaveGroup, SocketAddress.class, NetworkInterface.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(MulticastSocket.class)
            .calling(MulticastSocket::joinGroup, InetAddress.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .calling(MulticastSocket::joinGroup, SocketAddress.class, NetworkInterface.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .calling(MulticastSocket::leaveGroup, InetAddress.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .calling(MulticastSocket::leaveGroup, SocketAddress.class, NetworkInterface.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled()
            .calling(MulticastSocket::send, DatagramPacket.class, Byte.class)
            .enforce(Policies::allNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(SocketChannel.class)
            .callingStatic(SocketChannel::open, SocketAddress.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(SocketChannel::open, ProtocolFamily.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .callingStatic(SocketChannel::open)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(SocketChannel::bind, SocketAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on("sun.nio.ch.SocketChannelImpl", SocketChannel.class)
            .calling(SocketChannel::bind, SocketAddress.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(SocketChannel::connect, SocketAddress.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(ServerSocketChannel.class)
            .calling(ServerSocketChannel::bind, SocketAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on("sun.nio.ch.ServerSocketChannelImpl", ServerSocketChannel.class)
            .calling(ServerSocketChannel::bind, SocketAddress.class, Integer.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(ServerSocketChannel::accept)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(DatagramChannel.class)
            .calling(DatagramChannel::bind, SocketAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on("sun.nio.ch.DatagramChannelImpl", DatagramChannel.class)
            .calling(DatagramChannel::bind, SocketAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(DatagramChannel::connect, SocketAddress.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(DatagramChannel::send, ByteBuffer.class, SocketAddress.class)
            .enforce((_, _, target) -> {
                if (target instanceof InetSocketAddress isa && isa.getAddress().isMulticastAddress()) {
                    return Policies.allNetworkAccess();
                } else {
                    return Policies.outboundNetworkAccess();
                }
            })
            .elseThrowNotEntitled()
            .calling(DatagramChannel::receive, ByteBuffer.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(AsynchronousServerSocketChannel.class)
            .calling(AsynchronousServerSocketChannel::bind, SocketAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on("sun.nio.ch.AsynchronousSocketChannelImpl", AsynchronousSocketChannel.class)
            .calling(AsynchronousSocketChannel::connect, SocketAddress.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(
                AsynchronousSocketChannel::connect,
                TypeToken.of(SocketAddress.class),
                TypeToken.of(Object.class),
                new TypeToken<CompletionHandler<Void, Object>>() {}
            )
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(AsynchronousSocketChannel::bind, SocketAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on("sun.nio.ch.AsynchronousServerSocketChannelImpl", AsynchronousServerSocketChannel.class)
            .calling(AsynchronousServerSocketChannel::bind, SocketAddress.class, Integer.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(AsynchronousServerSocketChannel::accept)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(
                AsynchronousServerSocketChannel::accept,
                TypeToken.of(Object.class),
                new TypeToken<CompletionHandler<AsynchronousSocketChannel, Object>>() {}
            )
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(AsynchronousSocketChannel.class)
            .calling(AsynchronousSocketChannel::bind, SocketAddress.class)
            .enforce(Policies::inboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(SelectorProvider.class)
            .protectedCtor()
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled();

        EntitlementRules.on(InetAddressResolverProvider.class)
            .protectedCtor()
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled();

        EntitlementRules.on(URLStreamHandlerProvider.class).protectedCtor().enforce(Policies::changeNetworkHandling).elseThrowNotEntitled();

        EntitlementRules.on(SelectableChannel.class)
            .calling(SelectableChannel::register, Selector.class, Integer.class)
            .enforce(() -> Policies.outboundNetworkAccess().and(Policies.inboundNetworkAccess()))
            .elseThrowNotEntitled();

        EntitlementRules.on(AsynchronousChannelProvider.class)
            .protectedCtor()
            .enforce(Policies::changeNetworkHandling)
            .elseThrowNotEntitled();

        EntitlementRules.on(sun.net.www.URLConnection.class)
            .calling(sun.net.www.URLConnection::getHeaderField, String.class)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(sun.net.www.URLConnection::getHeaderField, Integer.class)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(sun.net.www.URLConnection::getHeaderFields)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(sun.net.www.URLConnection::getHeaderFieldKey, Integer.class)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(sun.net.www.URLConnection::getContentType)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled()
            .calling(sun.net.www.URLConnection::getContentLength)
            .enforce(Policies::entitlementForUrlConnection)
            .elseThrowNotEntitled();

        EntitlementRules.on(FtpURLConnection.class)
            .calling(FtpURLConnection::connect)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(FtpURLConnection::getInputStream)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(FtpURLConnection::getOutputStream)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(sun.net.www.protocol.http.HttpURLConnection.class)
//            .callingStatic(sun.net.www.protocol.http.HttpURLConnection::openConnectionCheckRedirects, URLConnection.class)
//            .enforce(Policies::outboundNetworkAccess)
//            .elseThrowNotEntitled()
            .calling(sun.net.www.protocol.http.HttpURLConnection::connect)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(sun.net.www.protocol.http.HttpURLConnection::getInputStream)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(sun.net.www.protocol.http.HttpURLConnection::getOutputStream)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(sun.net.www.protocol.http.HttpURLConnection::getErrorStream)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(sun.net.www.protocol.http.HttpURLConnection::getHeaderField, String.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(sun.net.www.protocol.http.HttpURLConnection::getHeaderFields)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(sun.net.www.protocol.http.HttpURLConnection::getHeaderField, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(sun.net.www.protocol.http.HttpURLConnection::getHeaderFieldKey, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(HttpsURLConnectionImpl.class)
            .calling(HttpsURLConnectionImpl::connect)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getInputStream)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getErrorStream)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getOutputStream)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getHeaderField, String.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getHeaderField, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getHeaderFieldKey, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getHeaderFields)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getResponseCode)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getResponseMessage)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getContentLength)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getContentLengthLong)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getContentType)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getContentEncoding)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getDate)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getExpiration)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getLastModified)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getHeaderFieldDate, String.class, Long.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getHeaderFieldInt, String.class, Integer.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getHeaderFieldLong, String.class, Long.class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getContent)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpsURLConnectionImpl::getContent, Class[].class)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(AbstractDelegateHttpsURLConnection.class)
            .calling(AbstractDelegateHttpsURLConnection::connect)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(MailToURLConnection.class)
            .calling(MailToURLConnection::connect)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(MailToURLConnection::getOutputStream)
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on("jdk.internal.net.http.HttpClientImpl", HttpClient.class)
            .calling(HttpClient::send, TypeToken.of(HttpRequest.class), new TypeToken<HttpResponse.BodyHandler<?>>() {})
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpClient::sendAsync, TypeToken.of(HttpRequest.class), new TypeToken<HttpResponse.BodyHandler<?>>() {})
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(
                HttpClient::sendAsync,
                TypeToken.of(HttpRequest.class),
                new TypeToken<HttpResponse.BodyHandler<Void>>() {},
                new TypeToken<HttpResponse.PushPromiseHandler<Void>>() {}
            )
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(HttpClientFacade.class)
            .calling(HttpClientFacade::send, TypeToken.of(HttpRequest.class), new TypeToken<HttpResponse.BodyHandler<?>>() {})
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(HttpClientFacade::sendAsync, TypeToken.of(HttpRequest.class), new TypeToken<HttpResponse.BodyHandler<?>>() {})
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled()
            .calling(
                HttpClientFacade::sendAsync,
                TypeToken.of(HttpRequest.class),
                new TypeToken<HttpResponse.BodyHandler<Void>>() {},
                new TypeToken<HttpResponse.PushPromiseHandler<Void>>() {}
            )
            .enforce(Policies::outboundNetworkAccess)
            .elseThrowNotEntitled();

        EntitlementRules.on(AbstractSelectableChannel.class)
            .calling(AbstractSelectableChannel::register, Selector.class, Integer.class, Object.class)
            .enforce((_, _, ops) -> {
                CheckMethod check = Policies.noop();
                if ((ops & SelectionKey.OP_CONNECT) != 0) {
                    check = check.and(Policies.outboundNetworkAccess());
                }
                if ((ops & SelectionKey.OP_ACCEPT) != 0) {
                    check = check.and(Policies.inboundNetworkAccess());
                }

                return check;
            })
            .elseThrowNotEntitled()
            .calling(AbstractSelectableChannel::register, Selector.class, Integer.class)
            .enforce((_, _, ops) -> {
                CheckMethod check = Policies.noop();
                if ((ops & SelectionKey.OP_CONNECT) != 0) {
                    check = check.and(Policies.outboundNetworkAccess());
                }
                if ((ops & SelectionKey.OP_ACCEPT) != 0) {
                    check = check.and(Policies.inboundNetworkAccess());
                }

                return check;
            })
            .elseThrowNotEntitled();

    }
}
