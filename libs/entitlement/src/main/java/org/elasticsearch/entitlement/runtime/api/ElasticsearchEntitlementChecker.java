/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;

import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.ContentHandlerFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.DatagramSocketImplFactory;
import java.net.FileNameMap;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.ResponseCache;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketImplFactory;
import java.net.URL;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.cert.CertStoreParameters;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

/**
 * Implementation of the {@link EntitlementChecker} interface, providing additional
 * API methods for managing the checks.
 * The trampoline module loads this object via SPI.
 */
public class ElasticsearchEntitlementChecker implements EntitlementChecker {

    private final PolicyManager policyManager;

    public ElasticsearchEntitlementChecker(PolicyManager policyManager) {
        this.policyManager = policyManager;
    }

    @Override
    public void check$java_lang_Runtime$exit(Class<?> callerClass, Runtime runtime, int status) {
        policyManager.checkExitVM(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$halt(Class<?> callerClass, Runtime runtime, int status) {
        policyManager.checkExitVM(callerClass);
    }

    @Override
    public void check$java_lang_System$$exit(Class<?> callerClass, int status) {
        policyManager.checkExitVM(callerClass);
    }

    @Override
    public void check$java_lang_ClassLoader$(Class<?> callerClass) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_lang_ClassLoader$(Class<?> callerClass, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_lang_ClassLoader$(Class<?> callerClass, String name, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_security_SecureClassLoader$(Class<?> callerClass) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_security_SecureClassLoader$(Class<?> callerClass, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_security_SecureClassLoader$(Class<?> callerClass, String name, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(
        Class<?> callerClass,
        String name,
        URL[] urls,
        ClassLoader parent,
        URLStreamHandlerFactory factory
    ) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_lang_ProcessBuilder$start(Class<?> callerClass, ProcessBuilder processBuilder) {
        policyManager.checkStartProcess(callerClass);
    }

    @Override
    public void check$java_lang_ProcessBuilder$$startPipeline(Class<?> callerClass, List<ProcessBuilder> builders) {
        policyManager.checkStartProcess(callerClass);
    }

    @Override
    public void check$java_lang_System$$setIn(Class<?> callerClass, InputStream in) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_System$$setOut(Class<?> callerClass, PrintStream out) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_System$$setErr(Class<?> callerClass, PrintStream err) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$addShutdownHook(Class<?> callerClass, Runtime runtime, Thread hook) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$removeShutdownHook(Class<?> callerClass, Runtime runtime, Thread hook) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_tools_jlink_internal_Jlink$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_tools_jlink_internal_Main$$run(Class<?> callerClass, PrintWriter out, PrintWriter err, String... args) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_vm_ci_services_JVMCIServiceLocator$$getProviders(Class<?> callerClass, Class<?> service) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_vm_ci_services_Services$$load(Class<?> callerClass, Class<?> service) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_vm_ci_services_Services$$loadSingle(Class<?> callerClass, Class<?> service, boolean required) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$com_sun_tools_jdi_VirtualMachineManagerImpl$$virtualMachineManager(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_Thread$$setDefaultUncaughtExceptionHandler(Class<?> callerClass, Thread.UncaughtExceptionHandler ueh) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_System$$clearProperty(Class<?> callerClass, String key) {
        policyManager.checkWriteProperty(callerClass, key);
    }

    @Override
    public void check$java_lang_System$$setProperty(Class<?> callerClass, String key, String value) {
        policyManager.checkWriteProperty(callerClass, key);
    }

    @Override
    public void check$java_lang_System$$setProperties(Class<?> callerClass, Properties props) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_LocaleServiceProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_BreakIteratorProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_CollatorProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_DateFormatProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_DateFormatSymbolsProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_DecimalFormatSymbolsProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_NumberFormatProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_CalendarDataProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_CalendarNameProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_CurrencyNameProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_LocaleNameProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_TimeZoneNameProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_logging_LogManager$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$$setDatagramSocketImplFactory(Class<?> callerClass, DatagramSocketImplFactory fac) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_HttpURLConnection$$setFollowRedirects(Class<?> callerClass, boolean set) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$$setSocketFactory(Class<?> callerClass, SocketImplFactory fac) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_Socket$$setSocketImplFactory(Class<?> callerClass, SocketImplFactory fac) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_URL$$setURLStreamHandlerFactory(Class<?> callerClass, URLStreamHandlerFactory fac) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_URLConnection$$setFileNameMap(Class<?> callerClass, FileNameMap map) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_URLConnection$$setContentHandlerFactory(Class<?> callerClass, ContentHandlerFactory fac) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$javax_net_ssl_HttpsURLConnection$setSSLSocketFactory(
        Class<?> callerClass,
        HttpsURLConnection connection,
        SSLSocketFactory sf
    ) {
        policyManager.checkSetHttpsConnectionProperties(callerClass);
    }

    @Override
    public void check$javax_net_ssl_HttpsURLConnection$$setDefaultSSLSocketFactory(Class<?> callerClass, SSLSocketFactory sf) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$javax_net_ssl_HttpsURLConnection$$setDefaultHostnameVerifier(Class<?> callerClass, HostnameVerifier hv) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$javax_net_ssl_SSLContext$$setDefault(Class<?> callerClass, SSLContext context) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_ProxySelector$$setDefault(Class<?> callerClass, ProxySelector ps) {
        policyManager.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_ResponseCache$$setDefault(Class<?> callerClass, ResponseCache rc) {
        policyManager.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_spi_InetAddressResolverProvider$(Class<?> callerClass) {
        policyManager.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_spi_URLStreamHandlerProvider$(Class<?> callerClass) {
        policyManager.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_URL$(Class<?> callerClass, String protocol, String host, int port, String file, URLStreamHandler handler) {
        policyManager.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_URL$(Class<?> callerClass, URL context, String spec, URLStreamHandler handler) {
        policyManager.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$bind(Class<?> callerClass, DatagramSocket that, SocketAddress addr) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$connect(Class<?> callerClass, DatagramSocket that, InetAddress addr) {
        policyManager.checkAllNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$connect(Class<?> callerClass, DatagramSocket that, SocketAddress addr) {
        policyManager.checkAllNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$send(Class<?> callerClass, DatagramSocket that, DatagramPacket p) {
        if (p.getAddress().isMulticastAddress()) {
            policyManager.checkAllNetworkAccess(callerClass);
        } else {
            policyManager.checkOutboundNetworkAccess(callerClass);
        }
    }

    @Override
    public void check$java_net_DatagramSocket$receive(Class<?> callerClass, DatagramSocket that, DatagramPacket p) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$joinGroup(Class<?> caller, DatagramSocket that, SocketAddress addr, NetworkInterface ni) {
        policyManager.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_DatagramSocket$leaveGroup(Class<?> caller, DatagramSocket that, SocketAddress addr, NetworkInterface ni) {
        policyManager.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_MulticastSocket$joinGroup(Class<?> caller, MulticastSocket that, InetAddress addr) {
        policyManager.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_MulticastSocket$joinGroup(Class<?> caller, MulticastSocket that, SocketAddress addr, NetworkInterface ni) {
        policyManager.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_MulticastSocket$leaveGroup(Class<?> caller, MulticastSocket that, InetAddress addr) {
        policyManager.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_MulticastSocket$leaveGroup(Class<?> caller, MulticastSocket that, SocketAddress addr, NetworkInterface ni) {
        policyManager.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_MulticastSocket$send(Class<?> callerClass, MulticastSocket that, DatagramPacket p, byte ttl) {
        policyManager.checkAllNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$(Class<?> callerClass, int port) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$(Class<?> callerClass, int port, int backlog) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$(Class<?> callerClass, int port, int backlog, InetAddress bindAddr) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$accept(Class<?> callerClass, ServerSocket that) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$implAccept(Class<?> callerClass, ServerSocket that, Socket s) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$bind(Class<?> callerClass, ServerSocket that, SocketAddress endpoint) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$bind(Class<?> callerClass, ServerSocket that, SocketAddress endpoint, int backlog) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, Proxy proxy) {
        if (proxy.type() == Proxy.Type.SOCKS || proxy.type() == Proxy.Type.HTTP) {
            policyManager.checkOutboundNetworkAccess(callerClass);
        }
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, String host, int port) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, InetAddress address, int port) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, String host, int port, InetAddress localAddr, int localPort) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, InetAddress address, int port, InetAddress localAddr, int localPort) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, String host, int port, boolean stream) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, InetAddress host, int port, boolean stream) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$bind(Class<?> callerClass, Socket that, SocketAddress endpoint) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$connect(Class<?> callerClass, Socket that, SocketAddress endpoint) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$connect(Class<?> callerClass, Socket that, SocketAddress endpoint, int backlog) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_URL$openConnection(Class<?> callerClass, URL that, Proxy proxy) {
        if (proxy.type() != Proxy.Type.DIRECT) {
            policyManager.checkOutboundNetworkAccess(callerClass);
        }
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientImpl$send(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest request,
        HttpResponse.BodyHandler<?> responseBodyHandler
    ) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientImpl$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler
    ) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientImpl$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler,
        HttpResponse.PushPromiseHandler<?> pushPromiseHandler
    ) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientFacade$send(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest request,
        HttpResponse.BodyHandler<?> responseBodyHandler
    ) {
        check$jdk_internal_net_http_HttpClientImpl$send(callerClass, that, request, responseBodyHandler);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientFacade$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler
    ) {
        check$jdk_internal_net_http_HttpClientImpl$sendAsync(callerClass, that, userRequest, responseHandler);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientFacade$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler,
        HttpResponse.PushPromiseHandler<?> pushPromiseHandler
    ) {
        check$jdk_internal_net_http_HttpClientImpl$sendAsync(callerClass, that, userRequest, responseHandler, pushPromiseHandler);
    }

    @Override
    public void check$java_security_cert_CertStore$$getInstance(Class<?> callerClass, String type, CertStoreParameters params) {
        // We need to check "just" the LDAPCertStore instantiation: this is the CertStore that will try to perform a network operation
        // (connect to an LDAP server). But LDAPCertStore is internal (created via SPI), so we instrument the general factory instead and
        // then do the check only for the path that leads to sensitive code (by looking at the `type` parameter).
        if ("LDAP".equals(type)) {
            policyManager.checkOutboundNetworkAccess(callerClass);
        }
    }

    @Override
    public void check$java_nio_channels_AsynchronousServerSocketChannel$bind(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        SocketAddress local
    ) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$bind(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        SocketAddress local,
        int backlog
    ) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousSocketChannelImpl$bind(
        Class<?> callerClass,
        AsynchronousSocketChannel that,
        SocketAddress local
    ) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_DatagramChannelImpl$bind(Class<?> callerClass, DatagramChannel that, SocketAddress local) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_nio_channels_ServerSocketChannel$bind(Class<?> callerClass, ServerSocketChannel that, SocketAddress local) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_ServerSocketChannelImpl$bind(
        Class<?> callerClass,
        ServerSocketChannel that,
        SocketAddress local,
        int backlog
    ) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_SocketChannelImpl$bind(Class<?> callerClass, SocketChannel that, SocketAddress local) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_SocketChannelImpl$connect(Class<?> callerClass, SocketChannel that, SocketAddress remote) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousSocketChannelImpl$connect(
        Class<?> callerClass,
        AsynchronousSocketChannel that,
        SocketAddress remote
    ) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousSocketChannelImpl$connect(
        Class<?> callerClass,
        AsynchronousSocketChannel that,
        SocketAddress remote,
        Object attachment,
        CompletionHandler<Void, Object> handler
    ) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_DatagramChannelImpl$connect(Class<?> callerClass, DatagramChannel that, SocketAddress remote) {
        policyManager.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_ServerSocketChannelImpl$accept(Class<?> callerClass, ServerSocketChannel that) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$accept(Class<?> callerClass, AsynchronousServerSocketChannel that) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$accept(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        Object attachment,
        CompletionHandler<AsynchronousSocketChannel, Object> handler
    ) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_DatagramChannelImpl$send(
        Class<?> callerClass,
        DatagramChannel that,
        ByteBuffer src,
        SocketAddress target
    ) {
        if (target instanceof InetSocketAddress isa && isa.getAddress().isMulticastAddress()) {
            policyManager.checkAllNetworkAccess(callerClass);
        } else {
            policyManager.checkOutboundNetworkAccess(callerClass);
        }
    }

    @Override
    public void check$sun_nio_ch_DatagramChannelImpl$receive(Class<?> callerClass, DatagramChannel that, ByteBuffer dst) {
        policyManager.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$load(Class<?> callerClass, Runtime that, String filename) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$loadLibrary(Class<?> callerClass, Runtime that, String libname) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_System$$load(Class<?> callerClass, String filename) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_System$$loadLibrary(Class<?> callerClass, String libname) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }
}
