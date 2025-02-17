/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
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
import java.net.URI;
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
import java.nio.channels.spi.SelectorProvider;
import java.nio.charset.Charset;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.spi.FileSystemProvider;
import java.security.cert.CertStoreParameters;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

/**
 * Implementation of the {@link EntitlementChecker} interface, providing additional
 * API methods for managing the checks.
 * The trampoline module loads this object via SPI.
 */
@SuppressForbidden(reason = "Explicitly checking APIs that are forbidden")
public class ElasticsearchEntitlementChecker implements EntitlementChecker {

    private final PolicyManager policyManager;

    public ElasticsearchEntitlementChecker(PolicyManager policyManager) {
        this.policyManager = policyManager;
    }

    /// /////////////////
    //
    // Exit the JVM process
    //

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

    /// /////////////////
    //
    // create class loaders
    //

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

    /// /////////////////
    //
    // "setFactory" methods
    //

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

    /// /////////////////
    //
    // Process creation
    //

    @Override
    public void check$java_lang_ProcessBuilder$start(Class<?> callerClass, ProcessBuilder processBuilder) {
        policyManager.checkStartProcess(callerClass);
    }

    @Override
    public void check$java_lang_ProcessBuilder$$startPipeline(Class<?> callerClass, List<ProcessBuilder> builders) {
        policyManager.checkStartProcess(callerClass);
    }

    /// /////////////////
    //
    // System Properties and similar
    //

    @Override
    public void check$java_lang_System$$clearProperty(Class<?> callerClass, String key) {
        policyManager.checkWriteProperty(callerClass, key);
    }

    @Override
    public void check$java_lang_System$$setProperties(Class<?> callerClass, Properties props) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_System$$setProperty(Class<?> callerClass, String key, String value) {
        policyManager.checkWriteProperty(callerClass, key);
    }

    /// /////////////////
    //
    // JVM-wide state changes
    //

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
    public void check$java_nio_charset_spi_CharsetProvider$(Class<?> callerClass) {
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
    public void check$java_util_Locale$$setDefault(Class<?> callerClass, Locale.Category category, Locale locale) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_Locale$$setDefault(Class<?> callerClass, Locale locale) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_TimeZone$$setDefault(Class<?> callerClass, TimeZone zone) {
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

    /// /////////////////
    //
    // Network access
    //

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
    public void check$java_nio_channels_spi_SelectorProvider$(Class<?> callerClass) {
        policyManager.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_nio_channels_spi_AsynchronousChannelProvider$(Class<?> callerClass) {
        policyManager.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void checkSelectorProviderInheritedChannel(Class<?> callerClass, SelectorProvider that) {
        policyManager.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$load(Class<?> callerClass, Runtime that, String filename) {
        policyManager.checkFileRead(callerClass, Path.of(filename));
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$loadLibrary(Class<?> callerClass, Runtime that, String libname) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_System$$load(Class<?> callerClass, String filename) {
        policyManager.checkFileRead(callerClass, Path.of(filename));
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_System$$loadLibrary(Class<?> callerClass, String libname) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_layout_ValueLayouts$OfAddressImpl$withTargetLayout(
        Class<?> callerClass,
        AddressLayout that,
        MemoryLayout memoryLayout
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        FunctionDescriptor function,
        Linker.Option... options
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        MemorySegment address,
        FunctionDescriptor function,
        Linker.Option... options
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_abi_AbstractLinker$upcallStub(
        Class<?> callerClass,
        Linker that,
        MethodHandle target,
        FunctionDescriptor function,
        Arena arena,
        Linker.Option... options
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(Class<?> callerClass, MemorySegment that, long newSize) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(
        Class<?> callerClass,
        MemorySegment that,
        long newSize,
        Arena arena,
        Consumer<MemorySegment> cleanup
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(
        Class<?> callerClass,
        MemorySegment that,
        Arena arena,
        Consumer<MemorySegment> cleanup
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, String name, Arena arena) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, Path path, Arena arena) {
        policyManager.checkFileRead(callerClass, path);
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_ModuleLayer$Controller$enableNativeAccess(
        Class<?> callerClass,
        ModuleLayer.Controller that,
        Module target
    ) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    /// /////////////////
    //
    // File access
    //

    // old io (ie File)

    @Override
    public void check$java_io_File$createNewFile(Class<?> callerClass, File file) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$$createTempFile(Class<?> callerClass, String prefix, String suffix, File directory) {
        policyManager.checkFileWrite(callerClass, directory);
    }

    @Override
    public void check$java_io_File$delete(Class<?> callerClass, File file) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$deleteOnExit(Class<?> callerClass, File file) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$mkdir(Class<?> callerClass, File file) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$mkdirs(Class<?> callerClass, File file) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$renameTo(Class<?> callerClass, File file, File dest) {
        policyManager.checkFileRead(callerClass, file);
        policyManager.checkFileWrite(callerClass, dest);
    }

    @Override
    public void check$java_io_File$setExecutable(Class<?> callerClass, File file, boolean executable) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setExecutable(Class<?> callerClass, File file, boolean executable, boolean ownerOnly) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setLastModified(Class<?> callerClass, File file, long time) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setReadable(Class<?> callerClass, File file, boolean readable) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setReadable(Class<?> callerClass, File file, boolean readable, boolean ownerOnly) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setReadOnly(Class<?> callerClass, File file) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setWritable(Class<?> callerClass, File file, boolean writable) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setWritable(Class<?> callerClass, File file, boolean writable, boolean ownerOnly) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_FileOutputStream$(Class<?> callerClass, String name) {
        policyManager.checkFileWrite(callerClass, new File(name));
    }

    @Override
    public void check$java_io_FileOutputStream$(Class<?> callerClass, String name, boolean append) {
        policyManager.checkFileWrite(callerClass, new File(name));
    }

    @Override
    public void check$java_io_FileOutputStream$(Class<?> callerClass, File file) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_FileOutputStream$(Class<?> callerClass, File file, boolean append) {
        policyManager.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_util_Scanner$(Class<?> callerClass, File source) {
        policyManager.checkFileRead(callerClass, source);
    }

    @Override
    public void check$java_util_Scanner$(Class<?> callerClass, File source, String charsetName) {
        policyManager.checkFileRead(callerClass, source);
    }

    @Override
    public void check$java_util_Scanner$(Class<?> callerClass, File source, Charset charset) {
        policyManager.checkFileRead(callerClass, source);
    }

    // nio

    @Override
    public void check$java_nio_file_Files$$getOwner(Class<?> callerClass, Path path, LinkOption... options) {
        policyManager.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$probeContentType(Class<?> callerClass, Path path) {
        policyManager.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$setOwner(Class<?> callerClass, Path path, UserPrincipal principal) {
        policyManager.checkFileWrite(callerClass, path);
    }

    // file system providers

    @Override
    public void check$java_nio_file_spi_FileSystemProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void checkNewFileSystem(Class<?> callerClass, FileSystemProvider that, URI uri, Map<String, ?> env) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void checkNewFileSystem(Class<?> callerClass, FileSystemProvider that, Path path, Map<String, ?> env) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void checkNewInputStream(Class<?> callerClass, FileSystemProvider that, Path path, OpenOption... options) {
        policyManager.checkFileRead(callerClass, path);
    }

    @Override
    public void checkNewOutputStream(Class<?> callerClass, FileSystemProvider that, Path path, OpenOption... options) {
        policyManager.checkFileWrite(callerClass, path);
    }

    private static boolean isOpenForWrite(Set<? extends OpenOption> options) {
        return options.contains(StandardOpenOption.WRITE)
            || options.contains(StandardOpenOption.APPEND)
            || options.contains(StandardOpenOption.CREATE)
            || options.contains(StandardOpenOption.CREATE_NEW)
            || options.contains(StandardOpenOption.DELETE_ON_CLOSE);
    }

    @Override
    public void checkNewFileChannel(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    ) {
        if (isOpenForWrite(options)) {
            policyManager.checkFileWrite(callerClass, path);
        } else {
            policyManager.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void checkNewAsynchronousFileChannel(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Set<? extends OpenOption> options,
        ExecutorService executor,
        FileAttribute<?>... attrs
    ) {
        if (isOpenForWrite(options)) {
            policyManager.checkFileWrite(callerClass, path);
        } else {
            policyManager.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void checkNewByteChannel(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    ) {
        if (isOpenForWrite(options)) {
            policyManager.checkFileWrite(callerClass, path);
        } else {
            policyManager.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void checkNewDirectoryStream(
        Class<?> callerClass,
        FileSystemProvider that,
        Path dir,
        DirectoryStream.Filter<? super Path> filter
    ) {
        policyManager.checkFileRead(callerClass, dir);
    }

    @Override
    public void checkCreateDirectory(Class<?> callerClass, FileSystemProvider that, Path dir, FileAttribute<?>... attrs) {
        policyManager.checkFileWrite(callerClass, dir);
    }

    @Override
    public void checkCreateSymbolicLink(Class<?> callerClass, FileSystemProvider that, Path link, Path target, FileAttribute<?>... attrs) {
        policyManager.checkFileWrite(callerClass, link);
        policyManager.checkFileRead(callerClass, target);
    }

    @Override
    public void checkCreateLink(Class<?> callerClass, FileSystemProvider that, Path link, Path existing) {
        policyManager.checkFileWrite(callerClass, link);
        policyManager.checkFileRead(callerClass, existing);
    }

    @Override
    public void checkDelete(Class<?> callerClass, FileSystemProvider that, Path path) {
        policyManager.checkFileWrite(callerClass, path);
    }

    @Override
    public void checkDeleteIfExists(Class<?> callerClass, FileSystemProvider that, Path path) {
        policyManager.checkFileWrite(callerClass, path);
    }

    @Override
    public void checkReadSymbolicLink(Class<?> callerClass, FileSystemProvider that, Path link) {
        policyManager.checkFileRead(callerClass, link);
    }

    @Override
    public void checkCopy(Class<?> callerClass, FileSystemProvider that, Path source, Path target, CopyOption... options) {
        policyManager.checkFileWrite(callerClass, target);
        policyManager.checkFileRead(callerClass, source);
    }

    @Override
    public void checkMove(Class<?> callerClass, FileSystemProvider that, Path source, Path target, CopyOption... options) {
        policyManager.checkFileWrite(callerClass, target);
        policyManager.checkFileWrite(callerClass, source);
    }

    @Override
    public void checkIsSameFile(Class<?> callerClass, FileSystemProvider that, Path path, Path path2) {
        policyManager.checkFileRead(callerClass, path);
        policyManager.checkFileRead(callerClass, path2);
    }

    @Override
    public void checkIsHidden(Class<?> callerClass, FileSystemProvider that, Path path) {
        policyManager.checkFileRead(callerClass, path);
    }

    @Override
    public void checkGetFileStore(Class<?> callerClass, FileSystemProvider that, Path path) {
        policyManager.checkFileRead(callerClass, path);
    }

    @Override
    public void checkCheckAccess(Class<?> callerClass, FileSystemProvider that, Path path, AccessMode... modes) {
        policyManager.checkFileRead(callerClass, path);
    }

    @Override
    public void checkGetFileAttributeView(Class<?> callerClass, FileSystemProvider that, Path path, Class<?> type, LinkOption... options) {
        policyManager.checkGetFileAttributeView(callerClass);
    }

    @Override
    public void checkReadAttributes(Class<?> callerClass, FileSystemProvider that, Path path, Class<?> type, LinkOption... options) {
        policyManager.checkFileRead(callerClass, path);
    }

    @Override
    public void checkReadAttributes(Class<?> callerClass, FileSystemProvider that, Path path, String attributes, LinkOption... options) {
        policyManager.checkFileRead(callerClass, path);
    }

    @Override
    public void checkReadAttributesIfExists(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Class<?> type,
        LinkOption... options
    ) {
        policyManager.checkFileRead(callerClass, path);
    }

    @Override
    public void checkSetAttribute(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        String attribute,
        Object value,
        LinkOption... options
    ) {
        policyManager.checkFileWrite(callerClass, path);

    }

    @Override
    public void checkExists(Class<?> callerClass, FileSystemProvider that, Path path, LinkOption... options) {
        policyManager.checkFileRead(callerClass, path);
    }

    // Thread management

    @Override
    public void check$java_lang_Thread$start(Class<?> callerClass, Thread thread) {
        policyManager.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_Thread$setDaemon(Class<?> callerClass, Thread thread, boolean on) {
        policyManager.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_ThreadGroup$setDaemon(Class<?> callerClass, ThreadGroup threadGroup, boolean daemon) {
        policyManager.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_util_concurrent_ForkJoinPool$setParallelism(Class<?> callerClass, ForkJoinPool forkJoinPool, int size) {
        policyManager.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_Thread$setName(Class<?> callerClass, Thread thread, String name) {
        policyManager.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_Thread$setPriority(Class<?> callerClass, Thread thread, int newPriority) {
        policyManager.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_Thread$setUncaughtExceptionHandler(
        Class<?> callerClass,
        Thread thread,
        Thread.UncaughtExceptionHandler ueh
    ) {
        policyManager.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_ThreadGroup$setMaxPriority(Class<?> callerClass, ThreadGroup threadGroup, int pri) {
        policyManager.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void checkGetFileStoreAttributeView(Class<?> callerClass, FileStore that, Class<?> type) {
        policyManager.checkWriteStoreAttributes(callerClass);
    }

    @Override
    public void checkGetAttribute(Class<?> callerClass, FileStore that, String attribute) {
        policyManager.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkGetBlockSize(Class<?> callerClass, FileStore that) {
        policyManager.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkGetTotalSpace(Class<?> callerClass, FileStore that) {
        policyManager.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkGetUnallocatedSpace(Class<?> callerClass, FileStore that) {
        policyManager.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkGetUsableSpace(Class<?> callerClass, FileStore that) {
        policyManager.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkIsReadOnly(Class<?> callerClass, FileStore that) {
        policyManager.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkName(Class<?> callerClass, FileStore that) {
        policyManager.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkType(Class<?> callerClass, FileStore that) {
        policyManager.checkReadStoreAttributes(callerClass);
    }
}
