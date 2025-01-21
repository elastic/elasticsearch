/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.ContentHandlerFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.DatagramSocketImplFactory;
import java.net.FileNameMap;
import java.net.InetAddress;
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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

@SuppressWarnings("unused") // Called from instrumentation code inserted by the Entitlements agent
public interface EntitlementChecker {

    ////////////////////
    //
    // Exit the JVM process
    //

    void check$java_lang_Runtime$exit(Class<?> callerClass, Runtime runtime, int status);

    void check$java_lang_Runtime$halt(Class<?> callerClass, Runtime runtime, int status);

    void check$java_lang_System$$exit(Class<?> callerClass, int status);

    ////////////////////
    //
    // ClassLoader ctor
    //

    void check$java_lang_ClassLoader$(Class<?> callerClass);

    void check$java_lang_ClassLoader$(Class<?> callerClass, ClassLoader parent);

    void check$java_lang_ClassLoader$(Class<?> callerClass, String name, ClassLoader parent);

    ////////////////////
    //
    // SecureClassLoader ctor
    //

    void check$java_security_SecureClassLoader$(Class<?> callerClass);

    void check$java_security_SecureClassLoader$(Class<?> callerClass, ClassLoader parent);

    void check$java_security_SecureClassLoader$(Class<?> callerClass, String name, ClassLoader parent);

    ////////////////////
    //
    // URLClassLoader constructors
    //

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls);

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent);

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory);

    void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent);

    void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory);

    ////////////////////
    //
    // "setFactory" methods
    //

    void check$javax_net_ssl_HttpsURLConnection$setSSLSocketFactory(Class<?> callerClass, HttpsURLConnection conn, SSLSocketFactory sf);

    void check$javax_net_ssl_HttpsURLConnection$$setDefaultSSLSocketFactory(Class<?> callerClass, SSLSocketFactory sf);

    void check$javax_net_ssl_HttpsURLConnection$$setDefaultHostnameVerifier(Class<?> callerClass, HostnameVerifier hv);

    void check$javax_net_ssl_SSLContext$$setDefault(Class<?> callerClass, SSLContext context);

    ////////////////////
    //
    // Process creation
    //

    void check$java_lang_ProcessBuilder$start(Class<?> callerClass, ProcessBuilder that);

    void check$java_lang_ProcessBuilder$$startPipeline(Class<?> callerClass, List<ProcessBuilder> builders);

    ////////////////////
    //
    // JVM-wide state changes
    //

    void check$java_lang_System$$setIn(Class<?> callerClass, InputStream in);

    void check$java_lang_System$$setOut(Class<?> callerClass, PrintStream out);

    void check$java_lang_System$$setErr(Class<?> callerClass, PrintStream err);

    void check$java_lang_Runtime$addShutdownHook(Class<?> callerClass, Runtime runtime, Thread hook);

    void check$java_lang_Runtime$removeShutdownHook(Class<?> callerClass, Runtime runtime, Thread hook);

    void check$jdk_tools_jlink_internal_Jlink$(Class<?> callerClass);

    void check$jdk_tools_jlink_internal_Main$$run(Class<?> callerClass, PrintWriter out, PrintWriter err, String... args);

    void check$jdk_vm_ci_services_JVMCIServiceLocator$$getProviders(Class<?> callerClass, Class<?> service);

    void check$jdk_vm_ci_services_Services$$load(Class<?> callerClass, Class<?> service);

    void check$jdk_vm_ci_services_Services$$loadSingle(Class<?> callerClass, Class<?> service, boolean required);

    void check$com_sun_tools_jdi_VirtualMachineManagerImpl$$virtualMachineManager(Class<?> callerClass);

    void check$java_lang_Thread$$setDefaultUncaughtExceptionHandler(Class<?> callerClass, Thread.UncaughtExceptionHandler ueh);

    void check$java_util_spi_LocaleServiceProvider$(Class<?> callerClass);

    void check$java_text_spi_BreakIteratorProvider$(Class<?> callerClass);

    void check$java_text_spi_CollatorProvider$(Class<?> callerClass);

    void check$java_text_spi_DateFormatProvider$(Class<?> callerClass);

    void check$java_text_spi_DateFormatSymbolsProvider$(Class<?> callerClass);

    void check$java_text_spi_DecimalFormatSymbolsProvider$(Class<?> callerClass);

    void check$java_text_spi_NumberFormatProvider$(Class<?> callerClass);

    void check$java_util_spi_CalendarDataProvider$(Class<?> callerClass);

    void check$java_util_spi_CalendarNameProvider$(Class<?> callerClass);

    void check$java_util_spi_CurrencyNameProvider$(Class<?> callerClass);

    void check$java_util_spi_LocaleNameProvider$(Class<?> callerClass);

    void check$java_util_spi_TimeZoneNameProvider$(Class<?> callerClass);

    void check$java_util_logging_LogManager$(Class<?> callerClass);

    void check$java_net_DatagramSocket$$setDatagramSocketImplFactory(Class<?> callerClass, DatagramSocketImplFactory fac);

    void check$java_net_HttpURLConnection$$setFollowRedirects(Class<?> callerClass, boolean set);

    void check$java_net_ServerSocket$$setSocketFactory(Class<?> callerClass, SocketImplFactory fac);

    void check$java_net_Socket$$setSocketImplFactory(Class<?> callerClass, SocketImplFactory fac);

    void check$java_net_URL$$setURLStreamHandlerFactory(Class<?> callerClass, URLStreamHandlerFactory fac);

    void check$java_net_URLConnection$$setFileNameMap(Class<?> callerClass, FileNameMap map);

    void check$java_net_URLConnection$$setContentHandlerFactory(Class<?> callerClass, ContentHandlerFactory fac);

    ////////////////////
    //
    // Network access
    //
    void check$java_net_ProxySelector$$setDefault(Class<?> callerClass, ProxySelector ps);

    void check$java_net_ResponseCache$$setDefault(Class<?> callerClass, ResponseCache rc);

    void check$java_net_spi_InetAddressResolverProvider$(Class<?> callerClass);

    void check$java_net_spi_URLStreamHandlerProvider$(Class<?> callerClass);

    void check$java_net_URL$(Class<?> callerClass, String protocol, String host, int port, String file, URLStreamHandler handler);

    void check$java_net_URL$(Class<?> callerClass, URL context, String spec, URLStreamHandler handler);

    void check$java_net_DatagramSocket$bind(Class<?> callerClass, DatagramSocket that, SocketAddress addr);

    void check$java_net_DatagramSocket$connect(Class<?> callerClass, DatagramSocket that, InetAddress addr);

    void check$java_net_DatagramSocket$connect(Class<?> callerClass, DatagramSocket that, SocketAddress addr);

    void check$java_net_DatagramSocket$send(Class<?> callerClass, DatagramSocket that, DatagramPacket p);

    void check$java_net_DatagramSocket$receive(Class<?> callerClass, DatagramSocket that, DatagramPacket p);

    void check$java_net_DatagramSocket$joinGroup(Class<?> callerClass, DatagramSocket that, SocketAddress addr, NetworkInterface ni);

    void check$java_net_DatagramSocket$leaveGroup(Class<?> callerClass, DatagramSocket that, SocketAddress addr, NetworkInterface ni);

    void check$java_net_MulticastSocket$joinGroup(Class<?> callerClass, MulticastSocket that, InetAddress addr);

    void check$java_net_MulticastSocket$joinGroup(Class<?> callerClass, MulticastSocket that, SocketAddress addr, NetworkInterface ni);

    void check$java_net_MulticastSocket$leaveGroup(Class<?> callerClass, MulticastSocket that, InetAddress addr);

    void check$java_net_MulticastSocket$leaveGroup(Class<?> callerClass, MulticastSocket that, SocketAddress addr, NetworkInterface ni);

    void check$java_net_MulticastSocket$send(Class<?> callerClass, MulticastSocket that, DatagramPacket p, byte ttl);

    // Binding/connecting ctor
    void check$java_net_ServerSocket$(Class<?> callerClass, int port);

    void check$java_net_ServerSocket$(Class<?> callerClass, int port, int backlog);

    void check$java_net_ServerSocket$(Class<?> callerClass, int port, int backlog, InetAddress bindAddr);

    void check$java_net_ServerSocket$accept(Class<?> callerClass, ServerSocket that);

    void check$java_net_ServerSocket$implAccept(Class<?> callerClass, ServerSocket that, Socket s);

    void check$java_net_ServerSocket$bind(Class<?> callerClass, ServerSocket that, SocketAddress endpoint);

    void check$java_net_ServerSocket$bind(Class<?> callerClass, ServerSocket that, SocketAddress endpoint, int backlog);

    // Binding/connecting ctors
    void check$java_net_Socket$(Class<?> callerClass, Proxy proxy);

    void check$java_net_Socket$(Class<?> callerClass, String host, int port);

    void check$java_net_Socket$(Class<?> callerClass, InetAddress address, int port);

    void check$java_net_Socket$(Class<?> callerClass, String host, int port, InetAddress localAddr, int localPort);

    void check$java_net_Socket$(Class<?> callerClass, InetAddress address, int port, InetAddress localAddr, int localPort);

    void check$java_net_Socket$(Class<?> callerClass, String host, int port, boolean stream);

    void check$java_net_Socket$(Class<?> callerClass, InetAddress host, int port, boolean stream);

    void check$java_net_Socket$bind(Class<?> callerClass, Socket that, SocketAddress endpoint);

    void check$java_net_Socket$connect(Class<?> callerClass, Socket that, SocketAddress endpoint);

    void check$java_net_Socket$connect(Class<?> callerClass, Socket that, SocketAddress endpoint, int backlog);

    // Network miscellanea
    void check$java_net_URL$openConnection(Class<?> callerClass, java.net.URL that, Proxy proxy);

    // HttpClient#send and sendAsync are abstract, so we instrument their internal implementations
    void check$jdk_internal_net_http_HttpClientImpl$send(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest request,
        HttpResponse.BodyHandler<?> responseBodyHandler
    );

    void check$jdk_internal_net_http_HttpClientImpl$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler
    );

    void check$jdk_internal_net_http_HttpClientImpl$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler,
        HttpResponse.PushPromiseHandler<?> pushPromiseHandler
    );

    void check$jdk_internal_net_http_HttpClientFacade$send(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest request,
        HttpResponse.BodyHandler<?> responseBodyHandler
    );

    void check$jdk_internal_net_http_HttpClientFacade$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler
    );

    void check$jdk_internal_net_http_HttpClientFacade$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler,
        HttpResponse.PushPromiseHandler<?> pushPromiseHandler
    );

    // We need to check the LDAPCertStore, as this will connect, but this is internal/created via SPI,
    // so we instrument the general factory instead and then filter in the check method implementation
    void check$java_security_cert_CertStore$$getInstance(Class<?> callerClass, String type, CertStoreParameters params);

    /* NIO
     * For NIO, we are sometime able to check a method on the public surface/interface (e.g. AsynchronousServerSocketChannel#bind)
     * but most of the time these methods are abstract in the public classes/interfaces (e.g. ServerSocketChannel#accept,
     * NetworkChannel#bind), so we are forced to implement the "impl" classes.
     * You can distinguish the 2 cases form the namespaces: java_nio_channels for the public ones, sun_nio_ch for the implementation
     * classes. When you see a check on a sun_nio_ch class/method, this means the matching method on the public class is abstract
     * (not instrumentable).
     */

    // bind

    void check$java_nio_channels_AsynchronousServerSocketChannel$bind(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        SocketAddress local
    );

    void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$bind(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        SocketAddress local,
        int backlog
    );

    void check$sun_nio_ch_AsynchronousSocketChannelImpl$bind(Class<?> callerClass, AsynchronousSocketChannel that, SocketAddress local);

    void check$sun_nio_ch_DatagramChannelImpl$bind(Class<?> callerClass, DatagramChannel that, SocketAddress local);

    void check$java_nio_channels_ServerSocketChannel$bind(Class<?> callerClass, ServerSocketChannel that, SocketAddress local);

    void check$sun_nio_ch_ServerSocketChannelImpl$bind(Class<?> callerClass, ServerSocketChannel that, SocketAddress local, int backlog);

    void check$sun_nio_ch_SocketChannelImpl$bind(Class<?> callerClass, SocketChannel that, SocketAddress local);

    // connect

    void check$sun_nio_ch_SocketChannelImpl$connect(Class<?> callerClass, SocketChannel that, SocketAddress remote);

    void check$sun_nio_ch_AsynchronousSocketChannelImpl$connect(Class<?> callerClass, AsynchronousSocketChannel that, SocketAddress remote);

    void check$sun_nio_ch_AsynchronousSocketChannelImpl$connect(
        Class<?> callerClass,
        AsynchronousSocketChannel that,
        SocketAddress remote,
        Object attachment,
        CompletionHandler<Void, Object> handler
    );

    void check$sun_nio_ch_DatagramChannelImpl$connect(Class<?> callerClass, DatagramChannel that, SocketAddress remote);

    // accept

    void check$sun_nio_ch_ServerSocketChannelImpl$accept(Class<?> callerClass, ServerSocketChannel that);

    void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$accept(Class<?> callerClass, AsynchronousServerSocketChannel that);

    void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$accept(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        Object attachment,
        CompletionHandler<AsynchronousSocketChannel, Object> handler
    );

    // send/receive

    void check$sun_nio_ch_DatagramChannelImpl$send(Class<?> callerClass, DatagramChannel that, ByteBuffer src, SocketAddress target);

    void check$sun_nio_ch_DatagramChannelImpl$receive(Class<?> callerClass, DatagramChannel that, ByteBuffer dst);
}
